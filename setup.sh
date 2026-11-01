#!/bin/bash
set -e

# Remove existing .env file if it exists
rm -f .env
rm -rf ./logs ./plugins ./config

# Create required Airflow directories
mkdir -p ./logs ./state

# Write the current user's UID into .env
echo "AIRFLOW_UID=$(id -u)" > .env
