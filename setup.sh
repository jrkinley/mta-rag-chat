#!/bin/bash
# Setup script to download latest MTA GTFS reference data

set -e
echo "Setting up MTA GTFS reference data..."

# Create gtfs-reference directory if it doesn't exist
mkdir -p gtfs-reference

# Remove existing data
echo "Removing existing GTFS reference data..."
rm -rf gtfs-reference/*

# Download latest Google Transit data from MTA
echo "Downloading latest Google Transit data..."
curl -LO http://web.mta.info/developers/data/nyct/subway/google_transit.zip

# Extract the zip file to the gtfs-reference directory
echo "Extracting data to gtfs-reference directory..."
unzip ./google_transit.zip -d ./gtfs-reference/

# Clean up the downloaded zip file
echo "Cleaning up..."
rm -f ./google_transit.zip

echo "Setup complete! GTFS reference data is now available in ./gtfs-reference/"
echo "You can now run your MTA reference code." 