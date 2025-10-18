#!/bin/bash

# This script to download NYC property sales data for each borough for 2024

echo "Start downloading xlsx data files..."

URL="https://www.nyc.gov/assets/finance/downloads/pdf/rolling_sales/annualized-sales/2024"

curl -L "$URL/2024_manhattan.xlsx" -o ../data/raw_xlsx/2024_manhattan.xlsx
curl -L "$URL/2024_bronx.xlsx" -o ../data/raw_xlsx/2024_bronx.xlsx
curl -L "$URL/2024_brooklyn.xlsx" -o ../data/raw_xlsx/2024_brooklyn.xlsx
curl -L "$URL/2024_queens.xlsx" -o ../data/raw_xlsx/2024_queens.xlsx
curl -L "$URL/2024_staten_island.xlsx" -o ../data/raw_xlsx/2024_staten_island.xlsx

echo "Download completed"
