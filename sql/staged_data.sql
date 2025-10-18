CREATE SCHEMA staging;


-- Using data from External table to standardize the address column
-- Definition of the table.
CREATE TABLE staging.nyc_standardized_address(
	property_id TEXT,
	property_address TEXT,
	standardized_address TEXT
);

-- Create a unique standardized address table
CREATE TABLE staging.nyc_std_address AS
	SELECT DISTINCT property_id, standardized_address
	FROM staging.nyc_standardized_address;


-- The main Query for the staging table.
CREATE TABLE staging.stg_nyc_sales AS
	WITH cleaned AS (
	    SELECT 
	        ns.*,
	        -- delete consecutive commas
	        REGEXP_REPLACE("ADDRESS", ',{2,}', ',') AS addr_no_double_commas,
	        CONCAT("BOROUGH"::NUMERIC::INTEGER, '-', "BLOCK"::NUMERIC::INTEGER, '-', "LOT"::NUMERIC::INTEGER) AS property_id
	    FROM public.nyc_sales ns
	    WHERE "SALE PRICE"::NUMERIC > 0
	),
	split_address AS (
	    SELECT
	        c.*,
	        -- take the part before first comma
	        UPPER(TRIM(SPLIT_PART(c.addr_no_double_commas, ',', 1))) AS raw_property_address,
	        -- take the second part
	        UPPER(TRIM(SPLIT_PART(c.addr_no_double_commas, ',', 2))) AS raw_apartment_number
	    FROM cleaned c
	),
	prefix_cleaned AS (
    SELECT 
        sa.*,
        REGEXP_REPLACE(sa.raw_property_address, '\mN[./ -]*A\.?\M', '', 'gi') AS addr_no_na,
        sa.raw_apartment_number AS apartment_number
    FROM split_address sa
	),
	stripped AS (
	    SELECT 
	        pc.*,
	        -- remove trailing period from address
	        REGEXP_REPLACE(pc.addr_no_na, '\.$', '') AS addr_no_period,
	        -- clean apartment number (remove dot)
	        REGEXP_REPLACE(pc.apartment_number, '\.', '', 'g') AS apt_no_period
	    FROM prefix_cleaned pc
	)
	SELECT
		-- All the columns of the schema
	    CASE "BOROUGH" 
	        WHEN '1.0' THEN 'Manhattan'
	        WHEN '2.0' THEN 'Bronx'
	        WHEN '3.0' THEN 'Brooklyn'
	        WHEN '4.0' THEN 'Queens'
	        WHEN '5.0' THEN 'Staten Island'
	    END AS borough_name,
	    UPPER(TRIM("NEIGHBORHOOD")) AS neighborhood_name,
	    SUBSTR("BUILDING CLASS CATEGORY", 1, 2) AS building_class_category_code,
	    SUBSTR(UPPER(TRIM("BUILDING CLASS CATEGORY")), 4) AS building_class_category_description,
	    TRIM(TRAILING '.0' FROM "TAX CLASS AT TIME OF SALE") AS tax_class_specific,
	    s.property_id,
	    "BOROUGH"::NUMERIC::INTEGER AS borough_code,
	    "BLOCK"::NUMERIC::INTEGER AS block_number,
	    "LOT"::NUMERIC::INTEGER AS lot_number,
	    "EASE-MENT"::NUMERIC::INT AS easement,
	    UPPER(TRIM(s.addr_no_period)) AS property_address,
	    UPPER(TRIM(SPLIT_PART(nsd.standardized_address, ',', 1))) AS standardized_address,
	    COALESCE(NULLIF(TRIM("APARTMENT NUMBER"), ''),
	        NULLIF(s.apt_no_period, '')) AS apartment_number,
	    TRIM(TRAILING '.0' FROM "ZIP CODE") AS zip_code,
	    "RESIDENTIAL UNITS"::NUMERIC::INTEGER AS number_of_residential_units,
	    "COMMERCIAL UNITS"::NUMERIC::INTEGER AS number_of_commercial_units,
	    "TOTAL UNITS"::NUMERIC::INTEGER AS total_number_of_units,
	    "LAND SQUARE FEET"::NUMERIC AS land_square_feet,
	    "GROSS SQUARE FEET"::NUMERIC AS gross_square_feet,
	    "YEAR BUILT"::NUMERIC::INTEGER AS year_built,
	    UPPER(TRIM("BUILDING CLASS AT TIME OF SALE")) AS building_class_specific,
	    "SALE PRICE"::NUMERIC AS sale_price,
	    "SALE DATE"::DATE AS sale_date,
	    CASE WHEN "SALE PRICE"::NUMERIC >= 10000 THEN 1 ELSE 0 END AS is_valid_market_sale
	FROM stripped s
	LEFT JOIN staging.nyc_std_address nsd
	ON s.property_id = nsd.property_id;





