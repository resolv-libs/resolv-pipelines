""" Utility methods for the scripts configuration. """
import ast
import configparser
from pathlib import Path
from typing import Mapping, Any, Union


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
