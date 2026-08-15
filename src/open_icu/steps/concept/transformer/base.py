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
    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: ComplexDatasetConceptConfig,
        step: "ConceptStep",
        **kwargs
    ):
        self._concept = concept
        self._complex_config = complex_config
        self._step: "ConceptStep" = step
        self._kwargs = kwargs

    def __call__(self) -> None:
        dependencies = dict(
            self._read_concept(concept_id)
            for concept_id in self._complex_config.dependencies
        )

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

    def _read_concept(self, concept_id: str) -> tuple[str, pl.LazyFrame]:
        concept = self._step._registry.get(concept_id)
        if concept is None:
            logger.error("Concept %s not found in registry.", concept_id)
            raise ValueError(f"Concept {concept_id} not found in registry.")
        concept_path = self._step.concept_output_dir(concept) / f"{self._complex_config.dataset}.parquet"
        if not concept_path.exists():
            logger.error("Concept file %s for concept %s does not exist.", concept_path, concept.identifier)
            raise FileNotFoundError(f"Concept file {concept_path} does not exist.")

        return concept.name, pl.scan_parquet(concept_path)

    @abstractmethod
    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        raise NotImplementedError
