"""Base class for complex concept transformers.

A *complex* concept is one that cannot be expressed as a mapping over a single
source table: it is computed from the output of other concepts, which the
pipeline has already harmonised and written as per-dataset parquet. This module
factors out everything such a concept shares regardless of what it computes —
resolving and scanning its dependencies, evaluating the subclass's
``transform``, attaching the MEDS columns and any configured extension columns,
and sinking the result to the concept's output directory.

Subclasses implement :meth:`BaseConceptTransformer.transform` and nothing else.
It receives dependency concepts keyed by (unversioned) name and returns a
LazyFrame of ``subject_id`` and ``time`` plus ``numeric_value`` and/or
``text_value``; both are filled in as null where the subclass does not produce
them. Keeping ``transform`` pure and I/O-free is deliberate — it makes the
computation testable on in-memory frames without a configured pipeline, and
keeps the plan lazy all the way to a single streaming ``sink_parquet``.
"""

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig, ConceptTransformerProtocol

if TYPE_CHECKING:
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.steps.concept.step import ConceptStep

logger = get_logger(__name__)


class BaseConceptTransformer(ConceptTransformerProtocol, metaclass=ABCMeta):
    """Computes one complex concept for one dataset from other concepts' output.

    Instances are constructed by the concept step from the mapping's
    configuration and invoked once per dataset; calling the instance runs the
    whole pipeline and writes the concept's parquet as a side effect.

    Attributes:
        strict_dependencies: Whether a declared dependency with no output is a
            hard error. True (the default) fails loudly, which is what a
            concept wants when every dependency is genuinely required.
            Transformers that tolerate absent inputs — a dataset without
            dobutamine, a cohort without urine output — set this to False and
            receive a partial mapping in :meth:`transform`, letting the concept
            degrade to the inputs a given dataset actually carries rather than
            dropping out of that dataset entirely.
    """

    strict_dependencies: bool = True

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: ComplexDatasetConceptConfig,
        step: "ConceptStep",
        **kwargs,
    ) -> None:
        """Bind the transformer to one concept, one dataset mapping and the step.

        Args:
            concept: The concept being produced; supplies the output code,
                identifier and extension columns.
            complex_config: The per-dataset mapping, naming the dataset and the
                dependency concepts to resolve.
            step: The owning concept step, used to resolve dependencies through
                its registry and to locate concept output directories.
            **kwargs: The mapping's ``kwargs`` block, passed through verbatim
                for subclasses to interpret. Available as ``self._kwargs``.
        """
        self._concept = concept
        self._complex_config = complex_config
        self._step: "ConceptStep" = step
        self._kwargs = kwargs

    def __call__(self) -> None:
        """Resolve dependencies, evaluate ``transform`` and write the concept.

        Dependencies are keyed by concept name rather than identifier, so that
        ``transform`` can refer to its inputs by the name used in the mapping
        without pinning a version; a name collision between two resolved
        dependencies is logged and the later one dropped. The transformed frame
        is completed with the MEDS columns (``code``, ``numeric_value``,
        ``text_value``) and any configured extension columns, then sunk to
        ``<concept output dir>/<dataset>.parquet``.

        Returns without writing when no dependency could be resolved, which
        under :attr:`strict_dependencies` = False means the concept is simply
        not computable for this dataset.
        """
        dependencies: dict[str, pl.LazyFrame] = {}
        for concept_id in self._complex_config.dependencies:
            resolved = self._read_concept(concept_id)
            if resolved is None:
                continue
            name, lf = resolved
            if name in dependencies:
                logger.error(
                    "Concept %s: dependencies resolve to duplicate name %s; keeping the first.",
                    self._concept.identifier,
                    name,
                )
                continue
            dependencies[name] = lf

        if not dependencies:
            logger.warning(
                "Skipping concept %s for dataset %s: none of its dependencies are available.",
                self._concept.identifier,
                self._complex_config.dataset,
            )
            return

        lf = self.transform(dependencies)

        lf = lf.with_columns(
            code=pl.lit(self._concept.code),
            dataset=pl.lit(self._complex_config.dataset),
        )
        lf = lf.with_columns(
            text_value=pl.coalesce(pl.col("^text_value$"), pl.lit(None, dtype=pl.String)),
            numeric_value=pl.coalesce(pl.col("^numeric_value$"), pl.lit(None, dtype=pl.Float32)),
        )

        for col_name, col_expr in self._concept.extension_columns.items():
            lf = lf.with_columns(parse_expr(lf, col_expr).alias(col_name))

        lf = lf.select(
            [
                pl.col("subject_id").cast(pl.Int64),
                pl.col("time").cast(pl.Datetime(time_unit="us")),
                pl.col("code").cast(pl.String),
                pl.col("numeric_value").cast(pl.Float32),
                pl.col("text_value").cast(pl.String),
            ]
            + [pl.col(col).cast(pl.String) for col in self._concept.extension_columns]
        ).sort("subject_id", "time")

        output_dir = self._step.concept_output_dir(self._concept)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self._complex_config.dataset}.parquet"
        logger.info("Writing complex concept %s to %s", self._concept.identifier, output_file)
        lf.sink_parquet(output_file)

    def _read_concept(self, concept_id: str) -> tuple[str, pl.LazyFrame] | None:
        """Resolve one dependency to its concept name and a scan of its output.

        Args:
            concept_id: Versioned identifier of the dependency concept, as
                declared in the mapping.

        Returns:
            The dependency's unversioned name paired with a lazy scan of its
            parquet for this dataset, or ``None`` when it cannot be resolved
            and :attr:`strict_dependencies` is False.

        Raises:
            ValueError: The identifier is not in the step's concept registry
                and :attr:`strict_dependencies` is True.
            FileNotFoundError: The dependency has no output for this dataset
                and :attr:`strict_dependencies` is True.
        """
        concept = self._step._registry.get(concept_id)
        if concept is None:
            if self.strict_dependencies:
                logger.error("Concept %s not found in registry.", concept_id)
                raise ValueError(f"Concept {concept_id} not found in registry.")
            logger.warning("Concept %s not found in registry; treating as unavailable.", concept_id)
            return None

        concept_path = self._step.concept_output_dir(concept) / f"{self._complex_config.dataset}.parquet"
        if not concept_path.exists():
            if self.strict_dependencies:
                logger.error("Concept file %s for concept %s does not exist.", concept_path, concept.identifier)
                raise FileNotFoundError(f"Concept file {concept_path} does not exist.")
            logger.warning(
                "Concept %s: dependency %s has no output for dataset %s; treating as never measured.",
                self._concept.identifier,
                concept.identifier,
                self._complex_config.dataset,
            )
            return None

        return concept.name, pl.scan_parquet(concept_path)

    @abstractmethod
    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        """Compute the concept from its dependencies.

        Implementations should stay pure and lazy: no reads, no writes, no
        ``collect``. Everything the concept needs is in ``dependencies``, and
        everything that happens to the result afterwards is handled by
        :meth:`__call__`.

        Args:
            dependencies: Resolved dependency concepts keyed by name, each a
                MEDS-shaped lazy frame (``subject_id``, ``time``, ``code``,
                ``numeric_value``, ``text_value``). Under
                :attr:`strict_dependencies` = False a declared dependency may
                be absent, which the implementation should treat as never
                measured rather than as an error.

        Returns:
            A lazy frame of ``subject_id`` and ``time`` carrying
            ``numeric_value`` and/or ``text_value``; whichever is not produced
            is filled in as null.
        """
        raise NotImplementedError
