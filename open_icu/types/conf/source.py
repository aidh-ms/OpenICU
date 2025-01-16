from pydantic import BaseModel


class SampleConfig(BaseModel):
    samples: list[str] = []
    sampler: str
    params: dict[str, str]


class SourceConfig(BaseModel):
    name: str
    connection_uri: str
    sample: SampleConfig
