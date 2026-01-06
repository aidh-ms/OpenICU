from open_icu.config.registery import BaseConfigRegistery
from open_icu.steps.extraction.config.table import TableConfig


class DatasetConfigRegistery(BaseConfigRegistery[TableConfig]):
    pass

dataset_config_registery = DatasetConfigRegistery()
