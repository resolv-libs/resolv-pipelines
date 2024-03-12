import argparse
import logging

from resolv_data import DirectoryDataset
from resolv_data import DATASET_TYPE_MAP
from scripts.utilities import config
from scripts.utilities.constants import Paths


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file',
                        dest='config_file_name',
                        type=str,
                        required=True,
                        help='Name of the configuration file for the script (must be placed in ./config/importer).')
    return parser


if __name__ == '__main__':
    arg_parser = _build_arg_parser()
    known_args, _ = arg_parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    conf_file_path = Paths.DATA_IMPORT_CONFIG_DIR / known_args.config_file_name
    importer_config = config.load_configuration_section(conf_file_path, 'Main')
    logging.getLogger().setLevel(importer_config.get('logging_level'))

    output_path = importer_config.get('output_path') if importer_config.get('output_path') \
        else Paths.RAW_DATASETS_DIR
    for dataset_name, dataset_mode in zip(importer_config.get('dataset_names').split(','),
                                          importer_config.get('dataset_modes').split(',')):
        dataset: DirectoryDataset = DATASET_TYPE_MAP[dataset_name](mode=dataset_mode)
        dataset_path = dataset.download(
            output_path=output_path,
            overwrite=importer_config.get('force_overwrite'),
            cleanup=importer_config.get('cleanup'),
            allow_invalid_checksum=importer_config.get('allow_invalid_checksum')
        )
        dataset.compute_index(path_prefix=dataset_path)
