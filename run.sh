#!/bin/bash

if [ ! -f .env ]; then
    if [ -f .env.example ]; then
        echo "No .env file found. Creating from .env.example..."
        cp .env.example .env
        echo "Please edit .env file with your settings and run again."
        exit 0
    else
        echo "No .env or .env.example file found. Please create a .env file with your settings."
        exit 1
    fi
fi

REPO=$(grep GITHUB_REPO .env | cut -d '=' -f2)
echo "Running ETL pipeline for repository: $REPO"
python src/main.py