from pydantic import Field

from open_icu.config.base import BaseConfig


class ShardingConfig(BaseConfig):
    """Configuration for a sharding table.

    Attributes:
        TODO:
        ...
    """

    __open_icu_config_type__ = "sharding"

    concepts: list[str] = Field()       # all-include, include, exclude

    # # Nächste

    # subject_ids: list[str] = Field()    # all-includes, include, exclude

    # # ERstmal ignorieren
    # time_spacing: str = Field()         # TODO: naming
    # presplit: bool = Field()            # true, false (split before sharding)
    # grouping_size: int = Field()        # patients per parquet

    # """ """


# multithread auf parquet ein thread paar subjects
