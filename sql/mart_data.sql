-- Schema for the dimensional model (Star Schema)
CREATE SCHEMA mart;


-- Defenition of dim_property
CREATE TABLE mart.dim_property (  
    property_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    property_id TEXT,
    borough_name TEXT,
    neighborhood_name TEXT,
    zip_code TEXT,
    building_class_category_description TEXT,
    property_address TEXT,
    building_class_specific TEXT,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

-- Populate the dim_property as a SCD Type 2
WITH source_data AS (
    SELECT 
        property_id,
        borough_name,
        neighborhood_name,
        zip_code,
        building_class_category_description,
        standardized_address,
        building_class_specific,
        sale_date,
        ROW_NUMBER() OVER (
            PARTITION BY property_id
            ORDER BY sale_date
        ) AS rn,
        LAG(building_class_category_description) OVER (PARTITION BY property_id ORDER BY sale_date) AS prev_building_class_category_description,
        LAG(standardized_address) OVER (PARTITION BY property_id ORDER BY sale_date) AS prev_property_address,
        LAG(building_class_specific) OVER (PARTITION BY property_id ORDER BY sale_date) AS prev_building_class_specific
    FROM staging.stg_nyc_sales
), 
detected_changes AS (
    SELECT 
        property_id,
        borough_name,
        neighborhood_name,
        zip_code,
        building_class_category_description,
        standardized_address,
        building_class_specific,
        sale_date AS valid_from,
        LEAD(sale_date) OVER (PARTITION BY property_id ORDER BY sale_date) AS valid_to,
        CASE 
            WHEN LEAD(sale_date) OVER (PARTITION BY property_id ORDER BY sale_date) IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_current
    FROM source_data
    WHERE rn = 1  -- first occurrence
       OR building_class_category_description IS DISTINCT FROM prev_building_class_category_description
       OR standardized_address IS DISTINCT FROM prev_property_address
       OR building_class_specific IS DISTINCT FROM prev_building_class_specific
)
INSERT INTO mart.dim_property (
    property_id,
    borough_name,
    neighborhood_name,
    zip_code,
    building_class_category_description,
    property_address,
    building_class_specific,
    valid_from,
    valid_to,
    is_current
)
SELECT 
    property_id,
    borough_name,
    neighborhood_name,
    zip_code,
    building_class_category_description,
    standardized_address,
    building_class_specific,
    valid_from,
    valid_to,
    is_current
FROM detected_changes;



-- Defenition of dim_date
CREATE TABLE mart.dim_date(
	date_key INT PRIMARY KEY,      
    full_date DATE NOT NULL,         
    day_of_week_num INT,             
    day_of_week_name VARCHAR(10),    
    day_in_month INT,                
    day_in_year INT,                
    week_in_year INT,               
    month_num INT,                   
    month_name VARCHAR(15),           
    quarter_num INT,                
    quarter_name VARCHAR(5),          
    year_num INT,                     
    is_weekend CHAR(1),               
    is_first_day_in_month CHAR(1),    
    is_last_day_in_month CHAR(1)      
);


-- Populating dim_date
INSERT INTO mart.dim_date 
	SELECT TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
	    	d AS full_date,
		    EXTRACT(ISODOW FROM d)::INT AS day_of_week_num,
		    TO_CHAR(d, 'Day')::VARCHAR(10) AS day_of_week_name,
		    EXTRACT(DAY FROM d)::INT AS day_in_month,
		    EXTRACT(DOY FROM d)::INT AS day_in_year,
		    EXTRACT(WEEK FROM d)::INT AS week_in_year,
		    EXTRACT(MONTH FROM d)::INT AS month_num,
		    TO_CHAR(d, 'Month')::VARCHAR(15) AS month_name,
		    EXTRACT(QUARTER FROM d)::INT AS quarter_num,
		    'Q' || EXTRACT(QUARTER FROM d)::TEXT AS quarter_name,
		    EXTRACT(YEAR FROM d)::INT AS year_num,
	    	CASE WHEN EXTRACT(ISODOW FROM d) IN (6,7) THEN 'Y' ELSE 'N' END AS is_weekend,
	    	CASE WHEN d = DATE_TRUNC('month', d)::DATE THEN 'Y' ELSE 'N' END AS is_first_day_in_month,
	    	CASE WHEN d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::DATE THEN 'Y' ELSE 'N' END AS is_last_day_in_month
	FROM GENERATE_SERIES('2003-01-01'::DATE, '2033-01-01'::DATE, '1 day') d;


-- Defenition of fct_property_sales
CREATE TABLE mart.fct_property_sales(
	sale_key INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	property_key INT NOT NULL REFERENCES mart.dim_property(property_key),
	date_key INT NOT NULL REFERENCES mart.dim_date(date_key),
	apartment_number TEXT,
	sale_price NUMERIC,
	is_valid_market_sale INT
);


-- Populating fct_property_sales
INSERT INTO mart.fct_property_sales (property_key, date_key, apartment_number, sale_price, is_valid_market_sale)
	SELECT dp.property_key,
			dd.date_key,
			sn.apartment_number,
			sn.sale_price,
			sn.is_valid_market_sale
	FROM staging.stg_nyc_sales sn
	JOIN mart.dim_property dp
	ON sn.property_id = dp.property_id
	JOIN mart.dim_date dd
	ON sn.sale_date = dd.full_date;




