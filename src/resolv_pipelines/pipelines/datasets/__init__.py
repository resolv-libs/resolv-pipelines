"""
Author: Matteo Pettenò.
Copyright (c) 2024, Matteo Pettenò
License: Apache License 2.0 (https://www.apache.org/licenses/LICENSE-2.0)
"""
from . import utilities
from .base import DatasetPipeline
from .import_dataset import ImportArchiveDatasetPipeline
from .canonicalize_dataset import CanonicalizeDatasetPipeline
from .process_dataset import ProcessDatasetPipeline
from .representation_dataset import RepresentationDatasetPipeline
