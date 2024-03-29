import argparse
import json
import logging
import os
from pathlib import Path
from typing import List, Union

from beam.dofn.mir.symbolic_music.augmenters import NS_AUG_DO_FN_MAP
from resolv_data import get_dataset_root_dir_name
from scripts.utilities import config, constants
from scripts.utilities import beam as beam_utils

PY_FILE_PATH = str(constants.Paths.SYMBOLIC_MUSIC_BEAM_PIPELINES_DIR / 'representation_dataset_pipeline.py')


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file',
                        dest='config_file_name',
                        type=str,
                        required=True,
                        help='Name of the configuration file for the script (must be placed in ./config/dataset).')
    return parser


def _get_beam_pipeline_cmd(config_file_path: Union[str, Path]) -> List[str]:
    dataset_config = config.load_configuration_section(config_file_path, 'Main')
    cmd = [constants.System.PY_INTERPRETER, PY_FILE_PATH]

    source_datasets_dir_names = [
        f'{get_dataset_root_dir_name(dataset_name, dataset_mode)}/{dataset_file_type}'
        for (dataset_name, dataset_mode, dataset_file_type) in
        zip(dataset_config.get('source_datasets_names').split(','),
            dataset_config.get('source_datasets_modes').split(','),
            dataset_config.get('source_datasets_file_types').split(','))
    ]
    input_path = dataset_config.get('input_path') if dataset_config.get('input_path') \
        else constants.Paths.GENERATED_DATASETS_DIR
    source_dataset_paths = [f'{input_path}/{dataset_config.get("input_dataset_name")}/{source_data_dir}'
                            for source_data_dir in source_datasets_dir_names]

    augmenters_configs = {}
    augmenters_config_dir = dataset_config.get('augmenters_config_dir')
    if augmenters_config_dir:
        processor_config_dir_path = constants.Paths.SYMBOLIC_MUSIC_DATASET_REPR_CONFIG_DIR / augmenters_config_dir
        if not processor_config_dir_path.is_dir():
            raise ValueError(f'Specified augmenters configuration directory {processor_config_dir_path} not valid.')
        else:
            for augmenter_id in [do_fn_id.replace("_ps_do_fn", "") for do_fn_id in NS_AUG_DO_FN_MAP.keys()]:
                path = processor_config_dir_path / f'{augmenter_id}_config.json'
                if path.is_file():
                    with open(path, mode='r') as config_file:
                        augmenters_configs[augmenter_id] = json.dumps(json.load(config_file))
                else:
                    logging.warning(f'No configuration file for processor {augmenter_id} specified in '
                                    f'{processor_config_dir_path}. Ignoring it.')
            processors_flags = beam_utils.beam_options_to_args(augmenters_configs)
            runner_args = beam_utils.get_runner_args_for_beam_pipeline(conf_file_path)
            cmd.extend(runner_args)
            pipeline_args = beam_utils.beam_options_to_args({
                'source_dataset_paths': ','.join(source_dataset_paths),
                'representation': dataset_config.get('representation'),
                'keep_attributes': dataset_config.get('keep_attributes'),
                'force_overwrite': dataset_config.get('force_overwrite'),
                'logging_level': dataset_config.get('logging_level'),
                'debug': dataset_config.get('debug'),
                'debug_output_type': dataset_config.get('debug_output_type'),
                'debug_file_pattern': dataset_config.get('debug_file_pattern')
            })
            cmd.extend(pipeline_args + processors_flags)
            return cmd
    else:
        raise ValueError("No processors configuration directory specified.")


if __name__ == '__main__':
    arg_parser = _build_arg_parser()
    known_args, _ = arg_parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    os.chdir(constants.Paths.ROOT_DIR)
    conf_file_path = constants.Paths.SYMBOLIC_MUSIC_DATASET_REPR_CONFIG_DIR / known_args.config_file_name
    beam_cmd = _get_beam_pipeline_cmd(conf_file_path)
    beam_utils.run_beam_cmd(beam_cmd)
