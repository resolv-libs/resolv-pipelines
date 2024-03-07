#!/bin/bash

usage() {
    echo "Usage: $0 [-h] [-e]" 1>&2
    echo "Options:" 1>&2
    echo "  -h    Display this help message." 1>&2
    echo "  -e    Install extra packages for conda environment." 1>&2
    exit 1
}

# Parse command line options
while getopts ":he" option; do
    case "${option}" in
        h)
            usage
            ;;
        e)
            extras=true
            ;;
        *)
            usage
            ;;
    esac
done

CONDA_PREFIX=./venv

# Deactivate and remove existing environment
eval "$(conda shell.bash deactivate)"
conda env remove --prefix $CONDA_PREFIX

# Create new environment
conda env create --prefix $CONDA_PREFIX --file environment.yml

# Activate new environment
source ./activate_conda_env.sh

# Install requirements
pip install -r ./infrastructure/executor/requirements.txt
if [ "$extras" = true ]; then
  pip install -r ./infrastructure/orchestrator/requirements.txt -r ./infrastructure/executor/extra-requirements.txt
fi
