"""Concept transformer for harmonising ICD-9-CM diagnosis codes to ICD-10-CM.

Heterogeneous ICU datasets record diagnoses in a mix of ICD-9-CM and
ICD-10-CM (e.g. MIMIC-IV switched vocabularies with the 2015 US mandate).
For pooled downstream use — in particular tokenised event streams for
foundation / autoregressive generative models (cf. ETHOS) — diagnoses must
live in a single vocabulary. This module maps ICD-9-CM codes forward to
ICD-10-CM using the CMS General Equivalence Mappings (GEM, 2018 final
release), the same crosswalk used by ETHOS and distributed by OHDSI-adjacent
tooling. The crosswalk is bundled with the package so the pipeline stays
fully offline.

Mapping policy (documented, deterministic):

- Native ICD-10 rows are passed through unchanged.
- ICD-9 rows are mapped via the GEM. When a source code has several GEM
  rows (approximate, combination, or choice-list mappings), the first row in
  GEM file order is used — scenario 1 / choice 1, the same simplification
  made by ETHOS via the ``icd-mappings`` package.
- ICD-9 rows without a GEM target (``no_map``) are kept in the output under
  an ``ICD9CM`` code namespace instead of being dropped, so no diagnosis
  events are silently lost.
"""

from functools import lru_cache
from importlib import resources
from typing import TYPE_CHECKING

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger

if TYPE_CHECKING:
    from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.steps.concept.step import ConceptStep

logger = get_logger(__name__)

GEM_RESOURCE = "icd9cm_to_icd10cm_gem.parquet"


@lru_cache(maxsize=1)
def load_gem_lookup() -> pl.DataFrame:
    """Load the bundled CMS GEM crosswalk as a one-to-one ICD-9-CM -> ICD-10-CM lookup.

    ``no_map`` rows are dropped and multi-target source codes are reduced to
    their first GEM row (scenario 1 / choice 1), so the result has exactly one
    ICD-10-CM target per ICD-9-CM source code. Codes are in undotted,
    uppercase GEM format (e.g. ``4019``, not ``401.9``).

    Returns:
        DataFrame with columns ``icd9cm`` and ``icd10cm``
    """
    resource = resources.files("open_icu.steps.concept.transformers") / "data" / GEM_RESOURCE
    with resources.as_file(resource) as path:
        gem = pl.read_parquet(path)

    return (
        gem.filter(pl.col("no_map") == 0)
        .unique(subset="icd9cm", keep="first", maintain_order=True)
        .select("icd9cm", "icd10cm")
    )


class ICD9ToICD10Transformer:
    """Complex-concept transformer that emits diagnosis events in a single ICD-10-CM vocabulary.

    Reads extracted diagnosis events, parses the ICD code and version out of
    the MEDS ``code`` column with a regex, and rewrites the code to
    ``<concept name>//ICD10CM//<code>`` (or ``<concept name>//ICD9CM//<code>``
    for unmappable ICD-9 codes). The source code and version are preserved as
    extension columns.

    Configured per dataset in a ``type: complex`` mapping YAML via ``kwargs``:

    Args:
        concept: The owning concept configuration (provides output code prefix and path)
        complex_config: The per-dataset complex mapping configuration
        table: Extraction table name the diagnosis events were written from
        code_pattern: Regex with named groups ``icd_code`` and (unless
            ``default_version`` is given) ``icd_version`` applied to the
            extracted ``code`` column
        event: Extraction event name; all events of the table when omitted
        default_version: Fixed ICD version ("9" or "10") for datasets whose
            codes do not carry a version label
    """

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: "ComplexDatasetConceptConfig",
        *,
        table: str,
        code_pattern: str,
        event: str | None = None,
        default_version: str | None = None,
        **kwargs,
    ) -> None:
        self._concept = concept
        self._config = complex_config
        self._table = table
        self._event = event
        self._code_pattern = code_pattern
        self._default_version = default_version

    def __call__(self, step: "ConceptStep") -> None:
        extraction_dataset = step.extraction_dataset
        if extraction_dataset is None:
            logger.warning(
                "skipping concept %s: extraction dataset not found",
                self._concept.identifier,
            )
            return

        table_path = extraction_dataset.data_path / self._config.dataset / self._config.version / self._table
        if self._event is None:
            data_paths = sorted(table_path.glob("*.parquet"))
        else:
            data_paths = [table_path / f"{self._event}.parquet"]
        data_paths = [path for path in data_paths if path.exists()]

        if not data_paths:
            logger.warning(
                "skipping concept %s for dataset %s: no event files found in %s",
                self._concept.identifier,
                self._config.dataset,
                table_path,
            )
            return

        frames = [self._harmonise(pl.scan_parquet(path), path.stem) for path in data_paths]
        lf = pl.concat(frames)

        output_dir = step.concept_output_dir(self._concept)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self._config.dataset}.parquet"

        logger.info(
            "Writing harmonised diagnosis concept %s to %s",
            self._concept.identifier,
            output_file,
        )
        lf.sink_parquet(output_file)

    def _harmonise(self, lf: pl.LazyFrame, event_name: str) -> pl.LazyFrame:
        parts = pl.col("code").str.extract_groups(self._code_pattern)
        source_code = parts.struct.field("icd_code")
        if self._default_version is None:
            source_version = parts.struct.field("icd_version")
        else:
            source_version = pl.lit(self._default_version)

        lf = lf.with_columns(
            source_code.alias("icd_code"),
            source_version.cast(pl.String).str.strip_chars().alias("icd_version"),
            # GEM format: undotted, uppercase.
            source_code.str.replace_all(".", "", literal=True).str.to_uppercase().alias("_gem_code"),
        ).filter(pl.col("icd_code").is_not_null() & pl.col("icd_version").is_in(["9", "10"]))

        lf = lf.join(load_gem_lookup().lazy(), left_on="_gem_code", right_on="icd9cm", how="left")

        icd10_code = pl.when(pl.col("icd_version") == "10").then(pl.col("_gem_code")).otherwise(pl.col("icd10cm"))
        lf = lf.with_columns(
            pl.when(icd10_code.is_not_null())
            .then(pl.format("{}//ICD10CM//{}", pl.lit(self._concept.name), icd10_code))
            .otherwise(pl.format("{}//ICD9CM//{}", pl.lit(self._concept.name), pl.col("_gem_code")))
            .alias("code")
        )

        # Provenance columns available to the concept's extension expressions,
        # mirroring the simple-concept extraction path.
        lf = lf.with_columns(
            pl.lit(self._config.dataset).alias("dataset"),
            pl.lit(self._config.version).alias("version"),
            pl.lit(self._table).alias("table"),
            pl.lit(event_name).alias("event"),
        )
        extension_names = list(dict.fromkeys(["icd_code", "icd_version", *self._concept.extension_columns]))
        for col_name, col_expr in self._concept.extension_columns.items():
            lf = lf.with_columns(parse_expr(lf, col_expr).alias(col_name))

        return lf.select(
            [
                pl.col("subject_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime(time_unit="us")),
                pl.col("code").cast(pl.String),
                pl.col("numeric_value").cast(pl.Float32),
                pl.col("text_value").cast(pl.String),
            ]
            + [pl.col(col).cast(pl.String) for col in extension_names]
        )
