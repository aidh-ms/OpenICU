"""Utilities for applying OpenICU's canonical event ordering."""

import polars as pl

from open_icu.config.event_order import EventOrderConfig


def apply_event_order(
    lf: pl.LazyFrame,
    config: EventOrderConfig,
) -> pl.LazyFrame:
    """Add temporary columns representing canonical event ordering.

    Event groups are matched against the first component of ``code`` before
    ``//``. Lower group orders come first. Within a group, explicitly ordered
    events come first in their configured order; all remaining events are
    ordered alphabetically by ``code``.
    """
    event_code = (
        pl.col("code")
        .str.split_exact("//", 1)
        .struct.field("field_0")
    )

    group_order = pl.lit(config.default_group_order)
    item_order = pl.lit(0)

    for group in reversed(list(config.groups.values())):
        matches_group = pl.lit(False)

        for pattern in group.patterns:
            matches_group = matches_group | event_code.str.contains(pattern)

        explicit_order = pl.lit(len(group.explicit_order))

        for index, code in reversed(list(enumerate(group.explicit_order))):
            explicit_order = (
                pl.when(event_code == code)
                .then(pl.lit(index))
                .otherwise(explicit_order)
            )

        group_order = (
            pl.when(matches_group)
            .then(pl.lit(group.order))
            .otherwise(group_order)
        )

        item_order = (
            pl.when(matches_group)
            .then(explicit_order)
            .otherwise(item_order)
        )

    return lf.with_columns(
        group_order.cast(pl.Int64).alias("_event_group_order"),
        item_order.cast(pl.Int64).alias("_event_item_order"),
    )


def sort_events(
    lf: pl.LazyFrame,
    config: EventOrderConfig,
) -> pl.LazyFrame:
    """Sort a MEDS event stream according to OpenICU's canonical ordering."""
    return (
        apply_event_order(lf, config)
        .sort(
            [
                "subject_id",
                "time",
                "_event_group_order",
                "_event_item_order",
                "code",
            ]
        )
        .drop(
            [
                "_event_group_order",
                "_event_item_order",
            ]
        )
    )
