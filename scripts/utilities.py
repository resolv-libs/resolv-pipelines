""" Utility methods to running scripts. """
import ast
import configparser
import logging
import select
import shlex
import subprocess
from pathlib import Path
from typing import Mapping, Any, Union, Dict, List

from scripts import constants


# --------------------------------- BEAM ---------------------------------

def beam_options_to_args(options: Dict[str, Any]) -> list[str]:
    """ Return a formatted pipeline options from a dictionary of arguments.

    The logic of this method should be compatible with Apache Beam:
    https://github.com/apache/beam/blob/b56740f0e8cd80c2873412847d0b336837429fb9/sdks/python/
    apache_beam/options/pipeline_options.py#L230-L251

    Args:
        options (Dict[str, Any]): Dictionary with options

    Return:
        (List[str]): List of arguments
    """
    if not options:
        return []

    args: list[str] = []
    for attr, value in options.items():
        if value is None:
            args.append(f"--{attr}")
        elif isinstance(value, bool):
            if value:
                args.append(f"--{attr}")
        elif isinstance(value, list):
            args.extend([f"--{attr}={v}" for v in value])
        else:
            args.append(f"--{attr}={value}")
    return args


def check_beam_version():
    """ Check the version of Apache Beam by running a subprocess and importing the package.

    Returns:
        str: The version of Apache Beam.

    Raises:
        CalledProcessError: If the command to check the Apache Beam version fails.
    """
    beam_version_cmd = [constants.System.PY_INTERPRETER, "-c", "import apache_beam; print(apache_beam.__version__)"]
    beam_version = subprocess.check_output(beam_version_cmd).decode().strip()
    return beam_version


def get_runner_args_for_beam_pipeline(config_file_path: Union[str, Path]) -> Dict[str, Any]:
    """ Get runner arguments for Apache Beam pipeline based on configuration file.

    Args:
        config_file_path (Union[str, Path]): Path to the configuration file.

    Returns:
        Dict[str, Any]: Runner arguments for the Apache Beam pipeline.

    Raises:
        ValueError: If the runner type specified in the configuration file is not recognized.
    """
    main_conf = load_configuration_section(config_file_path, 'Runner')
    runner_type = main_conf.get('runner_type')
    if runner_type == 'Direct':
        runner_conf = load_configuration_section(config_file_path, constants.BEAMConfigSections.DIRECT_RUNNER)
        direct_runner_options = {
            'runner': 'DirectRunner',
            'direct_running_mode': runner_conf.get('direct_running_mode'),
            'direct_num_workers': runner_conf.get("direct_num_workers"),
            'direct_runner_bundle_repeat': runner_conf.get('direct_runner_bundle_repeat')
        }
        if runner_conf.get("no_direct_runner_use_stacked_bundle"):
            direct_runner_options['no_direct_runner_use_stacked_bundle'] = True
        if runner_conf.get("direct_embed_docker_python"):
            direct_runner_options['direct_embed_docker_python'] = True
        return beam_options_to_args(direct_runner_options)
    elif runner_type == 'Flink':
        runner_conf = load_configuration_section(config_file_path, constants.BEAMConfigSections.FLINK_RUNNER)
        return beam_options_to_args({
            'runner': 'FlinkRunner',
            'flink_master': runner_conf.get("flink_master"),
            'flink_version': runner_conf.get("flink_version"),
            'flink_submit_uber_jar': True,
            'parallelism': runner_conf.get("flink_parallelism"),
            'max_parallelism': runner_conf.get("flink_max_parallelism"),
            'environment_type': main_conf.get("environment_type"),
            'environment_config': main_conf.get("environment_config"),
            'sdk_worker_parallelism': main_conf.get("sdk_worker_parallelism")
        })
    elif runner_type == 'Spark':
        runner_conf = load_configuration_section(config_file_path, constants.BEAMConfigSections.SPARK_RUNNER)
        return beam_options_to_args({
            'runner': 'SparkRunner',
            'spark_master_url': runner_conf.get("spark_master_url"),
            'spark_version': runner_conf.get("spark_version"),
            'spark_submit_uber_jar': True,
            'spark_rest_url': runner_conf.get("spark_rest_url"),
            'environment_type': main_conf.get("environment_type"),
            'environment_config': main_conf.get("environment_config"),
            'sdk_worker_parallelism': main_conf.get("sdk_worker_parallelism")
        })
    else:
        raise ValueError(f'Runner {runner_type} not supported.')


def run_beam_cmd(beam_cmd: List[str]):
    """ Run an Apache BEAM command in a Python subprocess.

    Args:
        beam_cmd (List[str]): The command to run.

    Raises:
        subprocess.CalledProcessError: If the subprocess call returns a non-zero exit status.
    """
    def _process_fd(proc, fd, log: logging.Logger):
        if fd not in (proc.stdout, proc.stderr):
            raise Exception("No data in stderr or in stdout.")

        fd_to_log = {proc.stderr: log.warning, proc.stdout: log.info}
        func_log = fd_to_log[fd]

        for line in iter(fd.readline, b""):
            line = line.decode()
            func_log(line.rstrip("\n"))

    logging.info("Beam version: %s", check_beam_version())
    logging.info("Running command: %s", " ".join(shlex.quote(c) for c in beam_cmd))
    process = subprocess.Popen(beam_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
    logging.info("Start waiting for Apache Beam process to complete.")
    reads = [process.stderr, process.stdout]
    while True:
        # Wait for at least one available fd.
        readable_fds, _, _ = select.select(reads, [], [], 5)
        for readable_fd in readable_fds:
            _process_fd(process, readable_fd, logging)
        if process.poll() is not None:
            break
    # Corner case: check if more output was created between the last read and the process termination
    for readable_fd in reads:
        _process_fd(process, readable_fd, logging)
    logging.info("Process exited with return code: %s", process.returncode)


# --------------------------------- CONFIGURATION ---------------------------------


def load_configuration_file(path: Union[str, Path]) -> configparser.ConfigParser:
    """ Loads the configuration file specified by path. Files that cannot be opened are silently ignored.

    Args:
        path (Union[str, Path]): Path to the configuration file to be loaded

    Returns:
        configparser.ConfigParser: An object representing the configuration file.
    """
    if not path:
        raise ValueError('No configuration file path provided.')

    config_file = configparser.ConfigParser()
    config_file.read(Path(path))
    return config_file


def load_configuration_section(path: Union[str, Path], section_name: str) -> Mapping[str, Any]:
    """ Loads a section of the configuration file specified by path.

    Args:
        path (Union[str, Path]): Path to the configuration file to be loaded
        section_name (str): The name of the section to load.

    Returns:
        (Mapping[str, Any]): The loaded config section.
    """
    config_file = load_configuration_file(path)
    file_section = dict()
    for option in config_file.options(section_name):
        option_value = config_file.get(section_name, option)
        try:
            file_section[option] = ast.literal_eval(option_value)
        except (SyntaxError, ValueError):
            file_section[option] = option_value
    return file_section
