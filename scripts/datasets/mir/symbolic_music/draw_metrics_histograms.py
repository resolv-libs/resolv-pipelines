import argparse
import logging
import os
from pathlib import Path
from typing import List, Union

from beam.dofn.mir.symbolic_music.metrics import METRIC_DO_FN_MAP
from resolv_data import get_dataset_root_dir_name
from scripts.utilities import config, constants
from scripts.utilities import beam as beam_utils

PY_FILE_PATH = str(constants.Paths.SYMBOLIC_MUSIC_BEAM_PIPELINES_DIR / 'draw_histograms_pipeline.py')


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file',
                        dest='config_file_name',
                        type=str,
                        required=True,
                        help='Name of the configuration file for the script (must be placed in ./config/metrics).')
    return parser


def _get_beam_pipeline_cmd(config_file_path: Union[str, Path]) -> List[str]:
    metrics_config = config.load_configuration_section(config_file_path, 'Main')
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

    histogram_metrics = metrics_config.get('histogram_metrics')
    if not histogram_metrics:
        raise ValueError('No metrics for which to generate histograms defined in config file.')
    if histogram_metrics == 'all':
        metrics_to_draw = ','.join([do_fn_id.replace("_ms_do_fn", "") for do_fn_id in METRIC_DO_FN_MAP.keys()])
    else:
        metrics_to_draw = histogram_metrics

    runner_args = beam_utils.get_runner_args_for_beam_pipeline(conf_file_path)
    cmd.extend(runner_args)
    pipeline_args = beam_utils.beam_options_to_args({
        'source_dataset_paths': ','.join(source_dataset_paths),
        'histogram_metrics': metrics_to_draw,
        'histogram_bins': ','.join([str(i) for i in metrics_config.get('histogram_bins')]),
        'logging_level': metrics_config.get('logging_level'),
    })
    cmd.extend(pipeline_args)
    return cmd


if __name__ == '__main__':
    arg_parser = _build_arg_parser()
    known_args, _ = arg_parser.parse_known_args()
    logging.getLogger().setLevel(logging.INFO)
    os.chdir(constants.Paths.ROOT_DIR)
    conf_file_path = constants.Paths.SYMBOLIC_MUSIC_DATASET_METRICS_CONFIG_DIR / known_args.config_file_name
    beam_cmd = _get_beam_pipeline_cmd(conf_file_path)
    beam_utils.run_beam_cmd(beam_cmd)
