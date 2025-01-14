from pydantic import BaseModel, Field


class SampleConfig(BaseModel):
    samples: list[str] = Field([])
    sampler: str
    params: dict[str, str]


class SourceConfig(BaseModel):
    name: str
    connection_uri: str
    sample: SampleConfig
