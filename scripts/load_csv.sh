#!/bin/bash

# This script loads all csv file into PostgreSQL.

DB_NAME="nyc_property"
DB_USER="postgres"
TABLE_NAME="public.nyc_sales"
CSV_DIR="../data/raw_csv"

echo "Start loading CSV files into PostgreSQL..."

for FILE in $CSV_DIR/*.csv
	do
		echo "loading data from $FILE into $TABLE_NAME.."
		psql -d $DB_NAME -U $DB_USER -c "\COPY $TABLE_NAME FROM '$FILE' WITH (FORMAT CSV, DELIMITER ',', HEADER TRUE);"

		if test $? -eq 0
			then
			echo "$FILE loaded successfully."
		else
			echo "Error while loading $FILE."
			exit 1	
		fi
	done

echo "All csv files loaded successfully."
