""" TODO - Module DOC """

from abc import ABC, abstractmethod
from typing import Any


class DataLoader(ABC):
    """ TODO - Class DOC """

    @abstractmethod
    def load_dataset(self) -> Any:
        pass
