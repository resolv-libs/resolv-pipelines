""" TODO - Module DOC """

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Union


class DataLoader(ABC):
    """ TODO - Class DOC """

    @abstractmethod
    def load_dataset(self) -> Any:
        pass
