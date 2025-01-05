from pydantic import BaseModel


class SampleConfig(BaseModel):
    sampler: str
    params: dict[str, str]


class SourceConfig(BaseModel):
    name: str
    connection_uri: str
    sample: SampleConfig
