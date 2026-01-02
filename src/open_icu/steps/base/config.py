from abc import ABCMeta

from pydantic import BaseModel


class BaseStepConfig(BaseModel, metaclass=ABCMeta):
    pass
