"""Reusable transformer for windowed features over continuous event streams.

Many complex concepts share a shape: they depend on a handful of other
concepts, align those event streams onto a common per-subject timeline, carry
each one onto that timeline with some *windowed aggregation* (last value within
a lookback, sum/max/mean over a trailing window, presence within a window), and
then compute an output from the aligned columns. The SOFA components, SAPS,
APACHE, Sepsis-3 suspected-infection windows and any "worst X over the past N
hours" concept all fit it, and none is expressible with the exact-key joins of
a ``derived`` concept.

:class:`WindowedConceptTransformer` provides that machinery once:

- union the dependency frames onto one continuous-time grid: every distinct
  ``(subject_id, time)`` at which *any* input was measured becomes a candidate
  evaluation point, with coincident records of one input collapsed;
- carry each input onto the grid via its :class:`Aggregation`;
- evaluate the subclass ``compute`` expression over the aligned columns;
- keep only rows at which a *trigger* input was measured (default: any input)
  and where ``compute`` produced a value.

Subclasses declare ``inputs`` (concept name -> aggregation), optionally
``triggers``, and a ``compute`` returning the ``numeric_value`` expression in
terms of the aligned input columns (referenced by concept name).

An :class:`Aggregation` receives the whole grid frame rather than returning a
single expression, so multi-stage features (segmentation, completeness flags)
are expressible; helper columns are namespaced ``__`` and dropped again.
"""

from copy import copy
from logging import ERROR, WARNING
from typing import TYPE_CHECKING

import polars as pl

from open_icu.logging import get_logger
from open_icu.steps.concept.config.complex import ComplexDatasetConceptConfig
from open_icu.steps.concept.transformer.base import BaseConceptTransformer

if TYPE_CHECKING:
    from open_icu.steps.concept.config.concept import ConceptConfig
    from open_icu.steps.concept.step import ConceptStep

logger = get_logger(__name__)

_SUBJECT = "subject_id"
_TIME = "time"


def _event(name: str) -> str:
    return f"__event_{name}"


def _present(name: str) -> str:
    return f"__present_{name}"


def _ago(window: str) -> pl.Expr:
    """The instant ``window`` before each evaluation time."""
    return pl.col(_TIME).dt.offset_by(f"-{window}")


class Aggregation:
    """Carries one input concept's events onto the evaluation grid.

    ``collapse`` combines several records of the input at the *same* timestamp
    into a single grid value. ``align`` turns that per-timestamp column into
    the windowed feature; it receives the grid (sorted by subject and time,
    with ``__present_<col>`` already computed) and must return it with ``col``
    replaced and any helper column removed again.

    Attributes:
        source: The dependency concept to read, when it differs from the name
            the aligned column is given. Set through :meth:`of`.
    """

    source: str | None = None

    def of(self, concept: str) -> "Aggregation":
        """Return a copy of this aggregation reading from ``concept``.

        Lets one dependency be carried onto the grid several times under
        different windows, each as its own column::

            "creatinine": WindowedLocf("24h"),
            "creatinine_baseline": RollingMin("7d").of("creatinine"),
            "creatinine_48h_min": RollingMin("48h").of("creatinine"),
        """
        aliased = copy(self)
        aliased.source = concept
        return aliased

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).drop_nulls().last()

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        raise NotImplementedError


class Locf(Aggregation):
    """Last observation carried forward, without expiry."""

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(pl.col(col).forward_fill().over(_SUBJECT).alias(col))


class WindowedLocf(Aggregation):
    """Last observation carried forward, expiring after ``window``.

    The value is the most recent measurement at or before *t*, but only while
    that measurement is no older than ``window`` (inclusive at the boundary,
    matching ``join_asof(tolerance=...)``); otherwise the input reads as
    missing. This is the aggregation to use when a concept should reflect
    "what we currently know" but stale data must not be treated as current.
    """

    def __init__(self, window: str) -> None:
        self._window = window

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            pl.when(
                pl.when(pl.col(col).is_not_null())
                .then(pl.col(_TIME))
                .otherwise(None)
                .forward_fill()
                .over(_SUBJECT)
                >= _ago(self._window)
            )
            .then(pl.col(col).forward_fill().over(_SUBJECT))
            .otherwise(None)
            .alias(col)
        )


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
        # sum coincident records, but keep null when none carry a value
        return pl.when(pl.col(col).is_not_null().any()).then(pl.col(col).sum()).otherwise(None)

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        total = pl.col(col).rolling_sum_by(_TIME, self._window, closed="right").over(_SUBJECT)
        if self._missing_is_zero:
            return lf.with_columns(total.fill_null(0).alias(col))
        count = (
            pl.col(col)
            .is_not_null()
            .cast(pl.Int32)
            .rolling_sum_by(_TIME, self._window, closed="right")
            .over(_SUBJECT)
        )
        return lf.with_columns(pl.when(count > 0).then(total).otherwise(None).alias(col))


class SegmentedRollingSum(_Rolling):
    """Trailing-window sum that only reports once the window is actually covered.

    Balance-type inputs (urine output above all) are recorded intermittently
    and their stream is interrupted by gaps — discharge and readmission, a
    period without a catheter, a stretch simply not charted. A naive 24h sum
    over such a stream reads a partially observed window as oliguria.

    Each subject's stream is therefore split into *segments* at every gap
    longer than ``gap`` between consecutive records of this input. The sum is
    reported only where both hold:

    - the current segment started at least ``window`` ago, so the trailing
      window lies wholly inside a period of observation, and
    - the most recent record is no older than ``gap``, so the segment is still
      running rather than already ended.

    Where they hold, a window with no records sums to 0 — real anuria, not
    missingness. Where they do not, the input reads as missing and the concept
    can fall back to its other inputs.

    ``gap`` defaults to ``window``.
    """

    def __init__(self, window: str, *, gap: str | None = None) -> None:
        super().__init__(window)
        self._gap = gap or window

    def collapse(self, col: str) -> pl.Expr:
        return pl.when(pl.col(col).is_not_null().any()).then(pl.col(col).sum()).otherwise(None)

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        last, start = f"__last_{col}", f"__segment_start_{col}"

        # time of the most recent record of this input at or before t; the grid
        # is unique per (subject, time), so its value one row back is the
        # *previous* record whenever the current row is itself a record
        lf = lf.with_columns(
            pl.when(pl.col(_present(col)))
            .then(pl.col(_TIME))
            .otherwise(None)
            .forward_fill()
            .over(_SUBJECT)
            .alias(last)
        )

        # a record opens a new segment if nothing precedes it or the preceding
        # record is more than `gap` old
        lf = lf.with_columns(
            pl.when(
                pl.col(_present(col))
                & (
                    pl.col(last).shift(1).over(_SUBJECT).is_null()
                    | (pl.col(last).shift(1).over(_SUBJECT) < _ago(self._gap))
                )
            )
            .then(pl.col(_TIME))
            .otherwise(None)
            .forward_fill()
            .over(_SUBJECT)
            .alias(start)
        )

        return lf.with_columns(
            pl.when((pl.col(start) <= _ago(self._window)) & (pl.col(last) >= _ago(self._gap)))
            .then(
                pl.col(col)
                .rolling_sum_by(_TIME, self._window, closed="right")
                .over(_SUBJECT)
                .fill_null(0)
            )
            .otherwise(None)
            .alias(col)
        ).drop(last, start)


class RollingMax(_Rolling):
    """Maximum (worst-high) over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).max()

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(col).rolling_max_by(_TIME, self._window, closed="right").over(_SUBJECT).alias(col)
        )


class RollingMin(_Rolling):
    """Minimum (worst-low) over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).min()

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(col).rolling_min_by(_TIME, self._window, closed="right").over(_SUBJECT).alias(col)
        )


class RollingMean(_Rolling):
    """Mean over the trailing window ``(t - window, t]``."""

    def collapse(self, col: str) -> pl.Expr:
        return pl.col(col).mean()

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            pl.col(col).rolling_mean_by(_TIME, self._window, closed="right").over(_SUBJECT).alias(col)
        )


class Exists(_Rolling):
    """Whether the input was recorded at all within the trailing window.

    Keys off event presence rather than ``numeric_value``, so marker concepts
    (antibiotics, cultures) that carry no value are detected.
    """

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            (
                pl.col(_present(col))
                .cast(pl.Int32)
                .rolling_sum_by(_TIME, self._window, closed="both")
                .over(_SUBJECT)
                > 0
            ).alias(col)
        )


class LastEventTime(_Rolling):
    """Timestamp of the most recent event of this input within the window.

    Null when the input had no event in ``[t - window, t]``. Keys off event
    presence, not ``numeric_value``. Useful for dating a derived event at a
    partner's time (see :meth:`WindowedConceptTransformer.event_time`).
    """

    def align(self, lf: pl.LazyFrame, col: str) -> pl.LazyFrame:
        return lf.with_columns(
            pl.when(pl.col(_present(col)))
            .then(pl.col(_TIME))
            .otherwise(None)
            .rolling_max_by(_TIME, self._window, closed="both")
            .over(_SUBJECT)
            .alias(col)
        )


class WindowedConceptTransformer(BaseConceptTransformer):
    """Aligns dependency concepts onto a windowed grid and evaluates ``compute``.

    A declared input that is absent from the dependency mapping is treated as
    never measured, so a concept degrades gracefully on datasets that do not
    carry all of its inputs.
    """

    #: concept name -> aggregation carrying it onto the grid
    inputs: dict[str, Aggregation] = {}
    #: input names whose measurements define evaluation points; None = all inputs
    triggers: set[str] | None = None

    strict_dependencies = False

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: ComplexDatasetConceptConfig,
        step: "ConceptStep",
        *,
        inputs: dict[str, Aggregation] | None = None,
        triggers: set[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(concept, complex_config, step, **kwargs)
        if inputs is not None:
            self.inputs = inputs
        if triggers is not None:
            self.triggers = set(triggers)

    def compute(self) -> pl.Expr:
        """Return the ``numeric_value`` expression over the aligned input columns.

        Aligned inputs are referenced by concept name, e.g.
        ``pl.col("creatinine")``. A null result emits no event, which lets
        ``compute`` suppress rows it cannot evaluate.
        """
        raise NotImplementedError

    def measured(self, name: str) -> pl.Expr:
        """True where ``name`` had an event at exactly this timestamp."""
        return pl.col(_present(name))

    def event_time(self) -> pl.Expr:
        """Output timestamp for each emitted event; default = the evaluation time.

        Override to date an event at a time derived from the aligned inputs
        rather than the row it was detected on — e.g. an onset carried by a
        :class:`LastEventTime` aggregation.
        """
        return pl.col(_TIME)

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        """Align the declared inputs and evaluate ``compute``.

        Pure and I/O-free: ``dependencies`` maps concept name to a MEDS-shaped
        frame (``subject_id``, ``time``, ``numeric_value``). Returns
        ``subject_id``, ``time``, ``numeric_value`` at the trigger timestamps.
        """
        names = list(self.inputs)
        if not names:
            raise ValueError(f"{type(self).__name__} declares no inputs")

        sources = {name: self.inputs[name].source or name for name in names}
        missing = sorted({source for source in sources.values() if source not in dependencies})
        if missing:
            logger.log(
                ERROR if len(missing) == len(set(sources.values())) else WARNING,
                "Concept %s (%s): declared input(s) %s absent from the resolved dependencies %s; "
                "treating them as never measured. Each input name must match the `name` of a "
                "concept declared under this mapping's dependencies.",
                self._concept.identifier,
                type(self).__name__,
                missing,
                sorted(dependencies),
            )

        frames = []
        for name in names:
            lf = dependencies.get(sources[name])
            if lf is None:
                frames.append(
                    pl.LazyFrame(
                        schema={
                            _SUBJECT: pl.Int64,
                            _TIME: pl.Datetime(time_unit="us"),
                            name: pl.Float64,
                            _event(name): pl.Int32,
                        }
                    )
                )
                continue
            frames.append(
                lf.select(
                    pl.col(_SUBJECT).cast(pl.Int64),
                    pl.col(_TIME).cast(pl.Datetime(time_unit="us")),
                    pl.col("numeric_value").cast(pl.Float64).alias(name),
                    pl.lit(1, dtype=pl.Int32).alias(_event(name)),
                )
            )

        grid = (
            pl.concat(frames, how="diagonal")
            .group_by(_SUBJECT, _TIME)
            .agg(
                *[self.inputs[name].collapse(name).alias(name) for name in names],
                # presence = an event row of this input exists at this timestamp,
                # independent of numeric_value (so marker concepts count)
                *[pl.col(_event(name)).is_not_null().any().alias(_present(name)) for name in names],
            )
            .sort(_SUBJECT, _TIME)
        )

        aligned = grid
        for name in names:
            aligned = self.inputs[name].align(aligned, name)

        triggers = self.triggers if self.triggers is not None else set(names)
        return (
            aligned.with_columns(numeric_value=self.compute(), __out_time=self.event_time())
            .filter(pl.any_horizontal(*[pl.col(_present(name)) for name in triggers]))
            .filter(pl.col("numeric_value").is_not_null())
            .select(pl.col(_SUBJECT), pl.col("__out_time").alias(_TIME), pl.col("numeric_value"))
            # collapse rows that resolve to the same output timestamp (only
            # reachable via an event_time override)
            .unique(subset=[_SUBJECT, _TIME], keep="first")
            .sort(_SUBJECT, _TIME)
        )


class GradedConceptTransformer(WindowedConceptTransformer):
    """A piecewise-constant grade over aligned inputs, e.g. a severity sub-score.

    Subclasses implement :meth:`build_inputs` — rather than a class-level
    ``inputs`` dict — so that the lookback stays configurable per mapping, and
    :meth:`grade`. The grade is emitted only where :meth:`observed` holds, so a
    concept with nothing current produces no event at all rather than a zero
    that a downstream aggregate would mistake for a measurement.
    """

    default_window = "24h"

    def __init__(
        self,
        concept: "ConceptConfig",
        complex_config: ComplexDatasetConceptConfig,
        step: "ConceptStep",
        **kwargs,
    ) -> None:
        super().__init__(concept, complex_config, step, **kwargs)
        if not self.inputs:
            self.inputs = self.build_inputs()

    @property
    def window(self) -> str:
        """How long an input's last value stays current; the mapping's ``window``."""
        return self._kwargs.get("window", self.default_window)

    def build_inputs(self) -> dict[str, Aggregation]:
        """Return the aligned inputs, built against :attr:`window`."""
        raise NotImplementedError

    def grade(self) -> pl.Expr:
        """Return the grade expression over the aligned input columns."""
        raise NotImplementedError

    def observed(self) -> pl.Expr:
        """Where the grade is evaluable; elsewhere the concept emits nothing.

        Defaults to "at least one input carries a value". Override wherever
        that is too permissive: a boolean input from :class:`Exists` is never
        null and so would make this always true, and a grade needing two inputs
        together (a ratio, a rate) is not evaluable from either alone.
        """
        return pl.any_horizontal(*[pl.col(name).is_not_null() for name in self.inputs])

    def compute(self) -> pl.Expr:
        return pl.when(self.observed()).then(self.grade()).otherwise(None).cast(pl.Float32)


class WindowedSumTransformer(WindowedConceptTransformer):
    """Sum of the most recent value of each input within a trailing window.

    Fully declarative: the summed concepts are the declared ``concepts`` of the
    mapping, or the (unversioned) names listed in ``kwargs.terms``; ``window``
    controls how long a term's last value stays current (default 24h). A term
    with no value inside the window contributes 0, so the sum is defined as
    soon as any one term is. Used for aggregate concepts such as a GCS total
    (eye + motor + verbal) or the total SOFA over its sub-scores.
    """

    default_window = "24h"

    @property
    def window(self) -> str:
        return self._kwargs.get("window", self.default_window)

    def transform(self, dependencies: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        if not self.inputs:
            terms = self._kwargs.get("terms") or list(dependencies)
            self.inputs = {name: WindowedLocf(self.window) for name in terms}
        return super().transform(dependencies)

    def compute(self) -> pl.Expr:
        return pl.sum_horizontal(*[pl.col(name).fill_null(0) for name in self.inputs]).cast(pl.Float32)


class WindowedMaxTransformer(WindowedSumTransformer):
    """Worst of the most recent value of each input within a trailing window.

    The combining counterpart to :class:`WindowedSumTransformer`, for concepts
    graded as the highest of several criteria rather than the total of several
    components — the KDIGO AKI stage over its creatinine and urine-output
    criteria, say. An input with no value inside the window contributes
    *nothing* rather than 0, so a criterion that could not be assessed cannot
    hold the combined grade down; where no input is current, no event is
    emitted at all.
    """

    def compute(self) -> pl.Expr:
        return pl.max_horizontal(*[pl.col(name) for name in self.inputs]).cast(pl.Float32)
