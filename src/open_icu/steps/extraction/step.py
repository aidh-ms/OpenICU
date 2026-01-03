from open_icu.steps.base.step import ConfigurableBaseStep
from open_icu.steps.extraction.config.step import ExtractionStepConfig
from open_icu.steps.extraction.config.table import TableConfig
from open_icu.steps.registery import register_step


@register_step
class ExtractionStep(ConfigurableBaseStep[ExtractionStepConfig, TableConfig]):

    def run(self) -> None:
        pass
