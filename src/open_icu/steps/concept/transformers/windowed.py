"""Reusable transformer for windowed features over continuous event streams.

Many complex concepts share a shape: they depend on a handful of other
concepts, align those event streams onto a common per-subject timeline, carry
each one onto that timeline with some *windowed aggregation* (last value,
sum/max/mean over a trailing window, presence within a window), and then
compute an output from the aligned columns. Severity scores (the SOFA
components, SAPS, APACHE), Sepsis-3 suspected-infection windows, and any
"mean/last/worst X over the past N hours" derived concept all fit it — and
none are expressible with the exact-key joins of a ``derived`` concept.

``WindowedConceptTransformer`` provides that machinery once:

- resolve the dependency concepts named under ``concepts`` in the mapping and
  read their per-dataset parquets;
- union them onto one continuous-time grid: every distinct ``(subject, time)``
  at which *any* input was measured becomes a candidate evaluation point (so a
  window sees every event), with coincident records collapsed per input;
- carry each input onto the grid via its :class:`Aggregation`;
- evaluate the subclass ``compute`` expression over the aligned columns;
- keep only rows at which a *trigger* input was measured (default: any input)
  and write MEDS output.

Subclasses declare ``inputs`` (concept name -> aggregation), optionally
``triggers``, and a ``compute`` returning the ``numeric_value`` expression in
terms of the aligned input columns (referenced by concept name).
"""

from typing import TYPE_CHECKING

import polars as pl

from open_icu.callbacks.interpreter import parse_expr
from open_icu.logging import get_logger

if TYPE_CHECKING:
    from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.steps.concept.step import ConceptStep

logger = get_logger(__name__)

_SUBJECT = "subject_id"
_TIME = "time"


class Aggregation:
    """Carries one input concept's events onto the evaluation grid.

    ``collapse`` combines several records of the input at the *same* timestamp
    into a single grid value; ``align`` turns that per-timestamp column into
    the windowed feature (already partitioned by subject). Both receive the
    working column name, which holds the input's ``numeric_value``.
    """

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).drop_nulls().last()

    def align(self, col: str) -> pl.Expr:
        raise NotImplementedError


class Locf(Aggregation):
    """Last observation carried forward: the most recent value at or before t."""

    def align(self, col: str) -> pl.Expr:
        return pl.col(col).forward_fill().over(_SUBJECT)


class _Rolling(Aggregation):
    def __init__(self, window: str) -> None:
        self._window = window


class RollingSum(_Rolling):
    """Sum over the trailing window ``(t - window, t]``.

    With ``missing_is_zero=False`` (default) a window containing no records of
    the input yields null rather than 0, so downstream logic can distinguish
    "no data" from a genuine recorded total near zero.
    """

    def __init__(self, window: str, *, missing_is_zero: bool = False) -> None:
        super().__init__(window)
        self._missing_is_zero = missing_is_zero

    def collapse(self, col: str) -> pl.Expr:
        # sum coincident records, but keep null when none are present
        return pl.when(pl.col(col).is_not_null().any()).then(pl.col(col).sum()).otherwise(None)

    def align(self, col: str) -> pl.Expr:
        total = pl.col(col).rolling_sum_by(_TIME, self._window, closed="right").over(_SUBJECT)
        if self._missing_is_zero:
            return total.fill_null(0)
        count = (
            pl.col(col).is_not_null().cast(pl.Int32).rolling_sum_by(_TIME, self._window, closed="right").over(_SUBJECT)
        )
        return pl.when(count > 0).then(total).otherwise(None)


class RollingMax(_Rolling):
    """Maximum (worst-high) over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).max()

    def align(self, col: str) -> pl.Expr:
        return pl.col(col).rolling_max_by(_TIME, self._window, closed="right").over(_SUBJECT)


class RollingMin(_Rolling):
    """Minimum (worst-low) over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).min()

    def align(self, col: str) -> pl.Expr:
        return pl.col(col).rolling_min_by(_TIME, self._window, closed="right").over(_SUBJECT)


class RollingMean(_Rolling):
    """Mean over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).mean()

    def align(self, col: str) -> pl.Expr:
        return pl.col(col).rolling_mean_by(_TIME, self._window, closed="right").over(_SUBJECT)


class Exists(_Rolling):
    """Whether the input was recorded at all within the trailing window.

    Keys off event presence (a row of this input at that time), not
    ``numeric_value``, so marker/boolean concepts (antibiotics, cultures)
    without a value are detected.
    """

    def align(self, col: str) -> pl.Expr:
        # inclusive both ends: "within <window>" counts an event exactly <window> ago
        count = (
            pl.col(f"__present_{col}").cast(pl.Int32).rolling_sum_by(_TIME, self._window, closed="both").over(_SUBJECT)
        )
        return count > 0


class LastEventTime(_Rolling):
    """Timestamp of the most recent event of this input within the trailing window.

    Null when the input had no event in ``(t - window, t]`` (inclusive). Keys
    off event presence, not ``numeric_value``, so it works for marker concepts.
    Useful for dating a derived event at a partner's time (see ``event_time``).
    """

    def align(self, col: str) -> pl.Expr:
        event_time = pl.when(pl.col(f"__present_{col}")).then(pl.col(_TIME)).otherwise(None)
        return event_time.rolling_max_by(_TIME, self._window, closed="both").over(_SUBJECT)


class WindowedConceptTransformer:
    """Base transformer aligning dependency concepts onto a windowed grid.

    Args:
        concept: The owning concept configuration (provides output code and path)
        complex_config: The per-dataset complex mapping configuration
        inputs: Optional override of the class-level ``inputs`` mapping
        triggers: Optional override of the class-level ``triggers`` set
    """

    #: concept name -> aggregation carrying it onto the grid
    inputs: dict[str, Aggregation] = {}
    #: input names whose measurements define evaluation points; None = all inputs
    triggers: set[str] | None = None

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: "ComplexDatasetConceptConfig",
        *,
        inputs: dict[str, Aggregation] | None = None,
        triggers: set[str] | None = None,
        **kwargs,
    ) -> None:
        self._concept = concept
        self._config = complex_config
        if inputs is not None:
            self.inputs = inputs
        if triggers is not None:
            self.triggers = triggers

    def compute(self) -> pl.Expr:
        """Return the ``numeric_value`` expression over the aligned input columns.

        Aligned inputs are referenced by concept name, e.g. ``pl.col("creatinine")``.
        """
        raise NotImplementedError

    def measured(self, name: str) -> pl.Expr:
        """True where ``name`` had an event at exactly this timestamp.

        Available to ``compute`` for logic that depends on which input fired at
        the row being evaluated — e.g. applying a lookback window only at the
        events of the *other* input.
        """
        return pl.col(f"__present_{name}")

    def event_time(self) -> pl.Expr:
        """Output timestamp for each emitted event; default = the evaluation time.

        Override to date an event at a time derived from the aligned inputs
        rather than the row it was detected on — e.g. an onset carried by a
        :class:`LastEventTime` aggregation.
        """
        return pl.col(_TIME)

    def transform(self, inputs: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        """Align the given input concept frames and evaluate ``compute``.

        Pure and I/O-free: ``inputs`` maps concept name to a MEDS-shaped frame
        (``subject_id``, ``time``, ``numeric_value``); a declared input absent
        from the mapping is treated as never-measured. Returns a frame of
        ``subject_id``, ``time``, ``numeric_value`` at the trigger timestamps.
        """
        names = list(self.inputs)

        frames = []
        for name in names:
            marker = f"__event_{name}"
            lf = inputs.get(name)
            if lf is None:
                frames.append(
                    pl.LazyFrame(
                        schema={
                            _SUBJECT: pl.Int64,
                            _TIME: pl.Datetime(time_unit="us"),
                            name: pl.Float64,
                            marker: pl.Int32,
                        }
                    )
                )
                continue
            frames.append(
                lf.select(
                    pl.col(_SUBJECT).cast(pl.Int64),
                    pl.col(_TIME).cast(pl.Datetime(time_unit="us")),
                    pl.col("numeric_value").cast(pl.Float64).alias(name),
                    pl.lit(1, dtype=pl.Int32).alias(marker),
                )
            )

        events = pl.concat(frames, how="diagonal")
        grid = (
            events.group_by(_SUBJECT, _TIME)
            .agg(
                *[self.inputs[name].collapse(name).alias(name) for name in names],
                # presence = an event row of this input exists at this timestamp,
                # independent of numeric_value (so marker/boolean concepts count)
                *[pl.col(f"__event_{name}").is_not_null().any().alias(f"__present_{name}") for name in names],
            )
            .sort(_SUBJECT, _TIME)
        )

        aligned = grid.with_columns(*[self.inputs[name].align(name).alias(name) for name in names])

        triggers = self.triggers if self.triggers is not None else set(names)
        return (
            aligned.with_columns(numeric_value=self.compute(), __out_time=self.event_time())
            .filter(pl.any_horizontal(*[pl.col(f"__present_{name}") for name in triggers]))
            # a row with no value emits no event (lets compute suppress non-events)
            .filter(pl.col("numeric_value").is_not_null())
            .select(pl.col(_SUBJECT), pl.col("__out_time").alias(_TIME), pl.col("numeric_value"))
            # collapse rows that resolve to the same output timestamp (e.g. several
            # later events pointing back to one onset)
            .unique(subset=[_SUBJECT, _TIME], keep="first")
            .sort(_SUBJECT, _TIME)
        )

    def __call__(self, step: "ConceptStep") -> None:
        available = {name: lf for name in self.inputs if (lf := self._read_dependency(step, name)) is not None}
        if not available:
            logger.warning(
                "skipping concept %s for dataset %s: none of its input concepts are available",
                self._concept.identifier,
                self._config.dataset,
            )
            return

        lf = self.transform(available)

        lf = lf.with_columns(
            code=pl.lit(self._concept.code),
            text_value=pl.lit(None, dtype=pl.String),
            dataset=pl.lit(self._config.dataset),
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

        output_dir = step.concept_output_dir(self._concept)
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self._config.dataset}.parquet"
        logger.info("Writing concept %s to %s", self._concept.identifier, output_file)
        lf.sink_parquet(output_file)

    def _read_dependency(self, step: "ConceptStep", name: str) -> pl.LazyFrame | None:
        """Resolve a dependency concept (by name, among the versioned ``concepts``) and scan it."""
        concept = next(
            (
                dependency
                for dependency_id in self._config.dependencies
                if (dependency := step._registry.get(dependency_id)) is not None and dependency.name == name
            ),
            None,
        )
        if concept is None:
            logger.warning(
                "concept %s: input %s not found among declared concepts %s",
                self._concept.identifier,
                name,
                sorted(self._config.dependencies),
            )
            return None
        path = step.concept_output_dir(concept) / f"{self._config.dataset}.parquet"
        if not path.exists():
            logger.warning("concept %s: input file not found (%s)", self._concept.identifier, path)
            return None
        return pl.scan_parquet(path)


class WindowedSumTransformer(WindowedConceptTransformer):
    """Sum of LOCF-carried inputs, re-evaluated whenever any input changes.

    Fully declarative: the summed concepts are listed under ``concepts`` in the
    mapping and named (unversioned) in ``kwargs.terms``. Missing inputs
    contribute 0. Used for aggregate concepts such as a GCS total (eye + motor
    + verbal) or the total SOFA (sum of its component sub-scores).
    """

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: "ComplexDatasetConceptConfig",
        *,
        terms: list[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(concept, complex_config, **kwargs)
        if terms is not None:
            self.inputs = {name: Locf() for name in terms}

    def compute(self) -> pl.Expr:
        return pl.sum_horizontal(*[pl.col(name).fill_null(0) for name in self.inputs]).cast(pl.Float32)
