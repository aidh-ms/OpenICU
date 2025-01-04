from pydantic import BaseModel


class SampleConfig(BaseModel):
    table: str
    field: str


class SourceConfig(BaseModel):
    name: str
    connection_uri: str
    sample: SampleConfig
