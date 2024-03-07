import argparse
import logging
import os
from pathlib import Path
from typing import List, Union

from modules.libs.datasets.constants import get_dataset_root_dir_name
from scripts import utilities, constants

PY_FILE_PATH = str(constants.Paths.BEAM_PIPELINES_DIR / 'canonicalize_dataset_pipeline.py')


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file',
                        dest='config_file_name',
                        type=str,
                        required=True,
                        help='Name of the configuration file for the script (must be placed in '
                             './config/canonicalizer).')
    return parser


def _get_beam_pipeline_cmd(config_file_path: Union[str, Path]) -> List[str]:
    canonicalizer_config = utilities.load_configuration_section(config_file_path, 'Main')
    cmd = [constants.System.PY_INTERPRETER, PY_FILE_PATH]

    dataset_index_paths = []
    dataset_output_paths = []
    dataset_file_types = canonicalizer_config.get('dataset_file_types').split(',')
    input_path = canonicalizer_config.get('input_path') if canonicalizer_config.get('input_path') \
        else constants.Paths.RAW_DATASETS_DIR
    output_path = canonicalizer_config.get("output_path") if canonicalizer_config.get("output_path") \
        else constants.Paths.CANONICAL_DATASETS_DIR
    for dataset_name, dataset_mode, dataset_file_type in zip(canonicalizer_config.get('dataset_names').split(','),
                                                             canonicalizer_config.get('dataset_modes').split(','),
                                                             dataset_file_types):
        dataset_root_dir = get_dataset_root_dir_name(dataset_name, dataset_mode)
        dataset_index_path = f'{input_path}/{dataset_root_dir}/index.json'
        dataset_index_paths.append(dataset_index_path)
        dataset_output_path = f'{output_path}/{dataset_root_dir}/{dataset_file_type}'
        dataset_output_paths.append(dataset_output_path)

    runner_args = utilities.get_runner_args_for_beam_pipeline(conf_file_path)
    cmd.extend(runner_args)
    pipeline_args = utilities.beam_options_to_args({
        'dataset_index_file_paths': ','.join(dataset_index_paths),
        'dataset_output_paths': ','.join(dataset_output_paths),
        'dataset_file_types': ','.join(dataset_file_types),
        'force_overwrite': canonicalizer_config.get('force_overwrite'),
        'logging_level': canonicalizer_config.get('logging_level'),
        'debug': canonicalizer_config.get('debug'),
        'debug_file_pattern': canonicalizer_config.get('debug_file_pattern')
    })
    cmd.extend(pipeline_args)

    return cmd


if __name__ == '__main__':
    arg_parser = _build_arg_parser()
    known_args, _ = arg_parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    os.chdir(constants.Paths.ROOT_DIR)
    conf_file_path = constants.Paths.DATASET_CANONICALIZER_CONFIG_DIR / known_args.config_file_name
    beam_cmd = _get_beam_pipeline_cmd(conf_file_path)
    utilities.run_beam_cmd(beam_cmd)
