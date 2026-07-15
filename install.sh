#!/bin/bash

# Create Python virtual environment named "quantum-scheduler"
python3.11 -m venv ~/.quantum-scheduler

# Activate the virtual environment
source ~/.quantum-scheduler/bin/activate

# Install dependencies from requirements.txt
pip install -r requirements.txt

