from open_icu.steps.source.concept import DeviceUsageExtractor


class RAWObservationExtractor(DeviceUsageExtractor):
    """
    A class to extract device usage data from a mimic database with a raw sql query.

    Parameters
    ----------
    subject_id : str
        The subject ID to extract data for.
    source : SourceConfig
        The source configuration.
    concept : ConceptConfig
        The concept configuration.
    concept_source : ConceptSource
        The concept source configuration.
    """
