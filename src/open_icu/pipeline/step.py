from abc import ABC, abstractmethod


class Step(ABC):
    @abstractmethod
    def transform(self) -> None:
        pass

# copy config
# dataset linking from workspace
# add ref to state
# write to wrorkspace
# logging
