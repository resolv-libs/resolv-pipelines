#!/bin/bash

CONDA_PREFIX=./venv

# Deactivate and remove existing environment
eval "$(conda shell.bash deactivate)"
conda env remove --prefix $CONDA_PREFIX

# Create new environment
conda env create --prefix $CONDA_PREFIX --file environment.yml
