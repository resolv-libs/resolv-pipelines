import argparse
import json
import logging
import os
from pathlib import Path
from typing import List, Union

from beam.dofn.metrics import METRIC_DO_FN_MAP
from resolv_data import get_dataset_root_dir_name
from scripts import utilities, constants

PY_FILE_PATH = str(constants.Paths.BEAM_PIPELINES_DIR / 'compute_metrics_pipeline.py')


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file',
                        dest='config_file_name',
                        type=str,
                        required=True,
                        help='Name of the configuration file for the script (must be placed in ./config/metrics).')
    return parser


def _get_beam_pipeline_cmd(config_file_path: Union[str, Path]) -> List[str]:
    metrics_config = utilities.load_configuration_section(config_file_path, 'Main')
    cmd = [constants.System.PY_INTERPRETER, PY_FILE_PATH]

    source_datasets_dir_names = [
        f'{get_dataset_root_dir_name(dataset_name, dataset_mode)}/{dataset_file_type}'
        for (dataset_name, dataset_mode, dataset_file_type) in
        zip(metrics_config.get('source_datasets_names').split(','),
            metrics_config.get('source_datasets_modes').split(','),
            metrics_config.get('source_datasets_file_types').split(','))
    ]
    input_path = metrics_config.get('input_path') if metrics_config.get('input_path') \
        else constants.Paths.GENERATED_DATASETS_DIR
    source_dataset_paths = [f'{input_path}/{metrics_config.get("input_dataset_name")}/{source_data_dir}'
                            for source_data_dir in source_datasets_dir_names]

    metrics_configs = {}
    metrics_config_dir = metrics_config.get('metrics_config_dir')
    if metrics_config_dir:
        metric_config_dir_path = constants.Paths.DATASET_METRICS_CONFIG_DIR / metrics_config_dir
        if not metric_config_dir_path.is_dir():
            raise ValueError(f'Specified processors configuration directory {metric_config_dir_path} not valid.')
        else:
            for metric_id in [do_fn_id.replace("_ms_do_fn", "") for do_fn_id in METRIC_DO_FN_MAP.keys()]:
                path = metric_config_dir_path / f'{metric_id}_config.json'
                if path.is_file():
                    with open(path, mode='r') as config_file:
                        metrics_configs[metric_id] = json.dumps(json.load(config_file))
                else:
                    logging.warning(f'No configuration file for processor {metric_id} specified in '
                                    f'{metric_config_dir_path}. Ignoring it.')
            metrics_flags = utilities.beam_options_to_args(metrics_configs)
            runner_args = utilities.get_runner_args_for_beam_pipeline(conf_file_path)
            cmd.extend(runner_args)
            pipeline_args = utilities.beam_options_to_args({
                'source_dataset_paths': ','.join(source_dataset_paths),
                'force_overwrite': metrics_config.get('force_overwrite'),
                'logging_level': metrics_config.get('logging_level'),
                'debug': metrics_config.get('debug'),
                'debug_file_pattern': metrics_config.get('debug_file_pattern')
            })
            cmd.extend(pipeline_args + metrics_flags)
            return cmd
    else:
        raise ValueError("No processors configuration directory specified.")


if __name__ == '__main__':
    arg_parser = _build_arg_parser()
    known_args, _ = arg_parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    os.chdir(constants.Paths.ROOT_DIR)
    conf_file_path = constants.Paths.DATASET_METRICS_CONFIG_DIR / known_args.config_file_name
    beam_cmd = _get_beam_pipeline_cmd(conf_file_path)
    utilities.run_beam_cmd(beam_cmd)
