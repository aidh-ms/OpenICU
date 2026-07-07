"""Sepsis-related concept transformers.

``SuspectedInfectionTransformer`` implements ricu's ``susp_inf`` — the
Sepsis-3 suspected-infection criterion (Seymour et al. 2016): the
co-occurrence of antibiotics and a body-fluid culture within asymmetric
windows.

    antibiotic first  -> culture within 24h after   (abx_win)
    culture first     -> antibiotic within 72h after (samp_win)

Detection reduces to trailing windows by evaluating at the *later* event of
each pair (at a culture: was there an antibiotic in the last 24h; at an
antibiotic: a culture in the last 72h). But the suspected-infection *onset* is
the *earlier* event of the pair — the timestamp that anchors a downstream SOFA
window. So instead of a boolean ``Exists``, each input is carried as
:class:`LastEventTime` (the partner's most recent event time within the
window); the event is emitted with ``numeric_value = 1`` at
``min(this event, partner event)`` — the earlier of the two. Multiple later
events pointing back to the same onset collapse to a single event (the base
deduplicates on output timestamp).

Both inputs are marker/boolean concepts without a value, detected by event
presence rather than ``numeric_value``.
"""

import polars as pl

from open_icu.steps.concept.transformers.windowed import LastEventTime, WindowedConceptTransformer

ANTIBIOTIC_WINDOW = "24h"
SAMPLING_WINDOW = "72h"
_TIME = "time"


class SuspectedInfectionTransformer(WindowedConceptTransformer):
    """Sepsis-3 suspected infection, emitted once per onset at min(antibiotic, culture)."""

    inputs = {
        "antibiotics": LastEventTime(ANTIBIOTIC_WINDOW),
        "body_fluid_sampling": LastEventTime(SAMPLING_WINDOW),
    }

    def _qualifies(self) -> pl.Expr:
        antibiotic_in_last_24h = pl.col("antibiotics").is_not_null()
        culture_in_last_72h = pl.col("body_fluid_sampling").is_not_null()
        return (self.measured("body_fluid_sampling") & antibiotic_in_last_24h) | (
            self.measured("antibiotics") & culture_in_last_72h
        )

    def compute(self) -> pl.Expr:
        return pl.when(self._qualifies()).then(1).otherwise(None).cast(pl.Float32)

    def event_time(self) -> pl.Expr:
        # onset = earlier of this event and the partner event carried in the window
        return pl.min_horizontal(pl.col(_TIME), pl.col("antibiotics"), pl.col("body_fluid_sampling"))
