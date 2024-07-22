USING retail_bronze

---- TEMPORAL INSERTADOS ----

CREATE OR REPLACE TEMPORARY VIEW {table_name_news} AS
SELECT table2.id, table2.name, table2.value
FROM retail_bronze_db.table_hoy table2
LEFT ANTI JOIN retail_bronze_db.table_ayer table1
ON table2.id = table1.id AND table2.value = table1.value

---- TEMPORAL MODIFICADOS ----

CREATE OR REPLACE TEMPORARY VIEW {table_name_modified} AS
SELECT table2.id, table2.name, table2.value
FROM retail_bronze_db.table_hoy table2
JOIN retail_bronze_db.table_ayer table1
ON table2.id = table1.id AND table2.value != table1.value    ----FALTAN COLUMNAS----

-- UNION DE LOS RESULTADOS EN UN PARQUET QUE SE REFRESCA ---

CREATE OR REPLACE TABLE  {union_table_name} AS
SELECT * FROM {table_name_news}
UNION ALL
SELECT * FROM {table_name_modified}

---- ACTUALIZAR TABLA modificados  ---- 

MERGE INTO retail_bronze_db.table_ayer A
USING retail_bronze_db.{union_table_name} U
ON A.id = U.id
WHEN MATCHED THEN
  UPDATE SET
    A.name = U.name,
    A.value = U.value,
WHEN NOT MATCHED THEN
  INSERT (id, name, value)
  VALUES (U.id, U.name, U.value)
"""

