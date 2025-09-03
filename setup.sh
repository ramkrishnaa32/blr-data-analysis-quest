#!/bin/bash

# Setup script for Rearc Data Quest

echo "Setting up Rearc Data Quest environment..."

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create Lambda layer
mkdir -p lambda_layer/python
pip install requests beautifulsoup4 pandas numpy -t lambda_layer/python/

# Initialize CDK if not already done
cdk init app --language python

echo "Setup complete! Run 'source venv/bin/activate' to activate the environment."