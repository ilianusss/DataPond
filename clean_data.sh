#!/bin/sh

# Clean data directories
echo "Cleaning data directories..."
rm -rf data/analytics/*
rm -rf data/cache/*
rm -rf data/fundamentals/*
rm -rf data/raw/*
rm -rf data/staged/*

# Clean Streamlit cache
echo "Cleaning Streamlit cache..."
rm -rf ~/.streamlit/cache
rm -rf .streamlit/cache

# Clean Python cache files
echo "Cleaning Python cache files..."
find . -type d -name "__pycache__" -exec rm -rf {} +
find . -type f -name "*.pyc" -delete

# Clean temporary files
echo "Cleaning temporary files..."
find . -type f -name "*.tmp" -delete
find . -type f -name "*.temp" -delete

echo "All caches and data files cleaned successfully!"
