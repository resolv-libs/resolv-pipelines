""" Module that contains all constants necessary to run the scripts.

Constants are separated into classes in order to make the module more readable and maintainable.
"""
import os
import sys
from pathlib import Path


class System:
    """ Constants related to the running system. """
    PY_INTERPRETER = sys.executable


class Paths:
    """ Constants that represent useful file system paths.

     This class use pathlib library to declare the paths.
     """

    # ------------ COMMON ------------
    ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent
    SCRIPTS_DIR = ROOT_DIR / 'scripts'
    RESOURCES_DIR = ROOT_DIR / 'resources'

    # ------------ BEAM ------------
    BEAM_DIR = ROOT_DIR / 'beam'
    BEAM_PIPELINES_DIR = BEAM_DIR / 'pipelines'
    DATA_BEAM_PIPELINES_DIR = BEAM_PIPELINES_DIR / 'data'
    MIR_BEAM_PIPELINES_DIR = BEAM_PIPELINES_DIR / 'mir'
    SYMBOLIC_MUSIC_BEAM_PIPELINES_DIR = MIR_BEAM_PIPELINES_DIR / 'symbolic_music'

    # ------------ DATASETS ------------
    DATASET_SCRIPTS_DIR = SCRIPTS_DIR / 'datasets'
    # DATA
    DATA_SCRIPTS_DIR = DATASET_SCRIPTS_DIR / 'data'
    DATA_SCRIPTS_CONFIG_DIR = DATA_SCRIPTS_DIR / 'config'
    DATA_IMPORT_CONFIG_DIR = DATA_SCRIPTS_CONFIG_DIR / 'importer'
    DATA_CANONICALIZER_CONFIG_DIR = DATA_SCRIPTS_CONFIG_DIR / 'canonicalizer'
    # MIR
    MIR_DATASET_SCRIPTS_DIR = DATASET_SCRIPTS_DIR / 'mir'
    SYMBOLIC_MUSIC_DATASET_SCRIPTS_DIR = MIR_DATASET_SCRIPTS_DIR / 'symbolic_music'
    SYMBOLIC_MUSIC_DATASET_CONFIG_DIR = SYMBOLIC_MUSIC_DATASET_SCRIPTS_DIR / 'config'
    SYMBOLIC_MUSIC_DATASET_GENERATOR_CONFIG_DIR = SYMBOLIC_MUSIC_DATASET_CONFIG_DIR / 'generator'
    SYMBOLIC_MUSIC_DATASET_METRICS_CONFIG_DIR = SYMBOLIC_MUSIC_DATASET_CONFIG_DIR / 'metrics'
    SYMBOLIC_MUSIC_DATASET_REPR_CONFIG_DIR = SYMBOLIC_MUSIC_DATASET_CONFIG_DIR / 'representation'
    # DEFAULT OUTPUT DIRECTORIES
    DATASETS_DIR = RESOURCES_DIR / 'datasets'
    RAW_DATASETS_DIR = DATASETS_DIR / 'raw'
    CANONICAL_DATASETS_DIR = DATASETS_DIR / 'canonical'
    GENERATED_DATASETS_DIR = DATASETS_DIR / 'generated'

    # ------------ MACHINE LEARNING ------------

class BEAMConfigSections:
    """ Constants that indicate the sections in the configuration file that are relative to the BEAM pipelines. """

    DIRECT_RUNNER = 'DirectRunner'
    FLINK_RUNNER = 'FlinkRunner'
    SPARK_RUNNER = 'SparkRunner'


class IOConfigSections:
    """ Constants that indicate the sections in the configuration file that are relative to I/O operations. """

    S3_FILE_SYSTEM = 'S3FileSystem'


class MLConfigSections:
    """ Constants that indicate the sections in the configuration file that are relative to ML operations. """

    REPRESENTATION = 'Representation'
    DATASETS = 'Datasets'
    MODEL = 'Model'
    TRAINING = 'Training'
    CHECKPOINTS = 'Checkpoints'
    EARLY_STOPPING = 'EarlyStopping'
    BACKUP = 'Backup'
    TENSORBOARD = 'Tensorboard'
