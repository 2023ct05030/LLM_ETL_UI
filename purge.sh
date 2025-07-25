#!/bin/bash

# Purge the environment

echo "🧹 Purging environment..."

# Deactivate virtual environment if active
if [ -n "$VIRTUAL_ENV" ]; then
    echo "🔧 Deactivating virtual environment..."
    deactivate
fi

# Remove Python virtual environment
if [ -d ".venv" ]; then
    echo "🗑️  Removing virtual environment..."
    rm -rf .venv
fi

# Remove frontend node_modules
if [ -d "frontend/node_modules" ]; then
    echo "🗑️  Removing frontend node_modules..."
    rm -rf frontend/node_modules
fi

echo "✅ Environment purged successfully!"
