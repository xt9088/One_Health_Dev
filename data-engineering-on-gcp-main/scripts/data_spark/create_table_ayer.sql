CREATE DATABASE IF NOT EXISTS retail_bronze_db 
LOCATION '${bucket_name}/retail_bronze.db';

CREATE TABLE IF NOT EXISTS ${table_hoy}
USING PARQUET
OPTIONS (
    path '${bucket_name}/retail_bronze_db/${table_ayer}'
);


