----CONFIGURAR CLOUD SQL MySQL TABLA PARAMETROS -----

update `db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_FILE_NAME ='nombre'

---- VALIDAR TABLA BIGQUERY HE-DEV-DATA----

DROP TABLE `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`; 

SELECT count(*) FROM `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`; 












---- Permisos de he-dev-compass a he-dev-data---
-----he-dev-composer --465256260181-compute@developer.gserviceaccount.com ----- gs://us-east4-dev-airflow-data-9879bdea-bucket/dags 
-----he-dev-data ---548349234634-compute@developer.gserviceaccount.com----gs://us-east4-dev-airflow-data-728aa61b-bucket/dags

---- 'PROJECT_ID = 'he-dev-composer' ----
update`db-compass`.`DATA_FLOW_CONFIG`
set PROJECT_ID = 'he-dev-data' 
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

---- 'DESTINATION_BUCKET = 'us-east4-dev-airflow-data-9879bdea-bucket' ----
update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_BUCKET = 'gcs_data_us-east4-dev-airflow-data-siniestros-bucket' 
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

---- 'DESTINATION_DIRECTORY = 'data/data-ipress/ipress_clinicas/internacional/' ----

update`db-compass`.`DATA_FLOW_CONFIG`
set  DESTINATION_DIRECTORY = '' 
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';


---- 'DESTINATION_DSET_LANDING = 'dev_data_analytics' ----
update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_DSET_LANDING = 'dev_data_landing'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 

---- 'DESTINATION_TABLE_LANDING  = 'anl_tmp_part_month_siniestro_t' ----
update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_TABLE_LANDING = 'iafas_diferencial_siniestros'   ----Nombre de tabla
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 

----Creacion de tabla en he-dev-data---
update`db-compass`.`DATA_FLOW_CONFIG`
set SQL_SCRIPT = 'CREATE TABLE `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`
(
  id_siniestro STRING,
  id_origen STRING,
  id_siniestro_origen STRING,
  id_poliza STRING,
  id_contratante STRING,
  id_producto STRING,
  id_certificado STRING,
  id_titular STRING,
  fec_hora_ocurrencia TIMESTAMP,
  fec_notificacion TIMESTAMP,
  num_siniestro STRING,
  fec_operacion TIMESTAMP,
  id_estado_siniestro_origen STRING,
  des_estado_siniestro_origen STRING,
  id_estado_siniestro STRING,
  des_estado_siniestro STRING,
  fec_estado TIMESTAMP,
  id_ubicacion_geografica STRING,
  des_departamento_siniestro STRING,
  des_provincia_siniestro STRING,
  des_distrito_siniestro STRING,
  des_ubicacion_detallada STRING,
  id_unidad_asegurable STRING,
  id_asegurado STRING,
  des_unidad_asegurable STRING,
  sexo_asegurable STRING,
  fec_nacimiento_asegurable DATE,
  num_documento_asegurable STRING,
  tip_documento_asegurable STRING,
  rango_etareo_asegurable STRING,
  fec_constitucion_siniestro TIMESTAMP,
  fec_anulacion TIMESTAMP,
  fec_ult_liquidacion TIMESTAMP,
  mto_total_reserva NUMERIC,
  mto_total_reserva_usd NUMERIC,
  mto_total_reserva_ajustador NUMERIC,
  mto_total_reserva_usd_ajustador NUMERIC,
  mto_total_aprobado NUMERIC,
  mto_total_aprobado_usd NUMERIC,
  mto_aprobado_deduci NUMERIC,
  mto_aprobado_deduci_usd NUMERIC,
  mto_total_facturado NUMERIC,
  mto_total_facturado_usd NUMERIC,
  mto_total_pendiente NUMERIC,
  mto_total_pendiente_usd NUMERIC,
  mto_total_pagado NUMERIC,
  mto_total_pagado_usd NUMERIC,
  mto_total_pagado_ajustador NUMERIC,
  mto_total_pagado_usd_ajustador NUMERIC,
  mto_pagado_deduci NUMERIC,
  mto_pagado_deduci_usd NUMERIC,
  mto_potencial_usd NUMERIC,
  id_oficina_reclamo_origen STRING,
  id_oficina_reclamo STRING,
  des_oficina_reclamo STRING,
  id_motivo_estado_origen STRING,
  des_motivo_estado_origen STRING,
  id_motivo_estado STRING,
  des_motivo_estado STRING,
  id_moneda STRING,
  id_compania STRING,
  tipo_flujo STRING,
  porcentaje_coasegurador_rimac FLOAT64,
  mto_reserva_np FLOAT64,
  mto_reserva_usd_np FLOAT64,
  mto_aprobado_np FLOAT64,
  mto_aprobado_usd_np FLOAT64,
  mto_pagado_np FLOAT64,
  mto_pagado_usd_np FLOAT64,
  tip_atencion_siniestro STRING,
  tip_procedencia STRING,
  fec_insercion DATE,
  fec_modificacion DATE,
  bq__soft_deleted BOOL,
  partition_date TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);'   
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 









CREATE TABLE `db-compass`.Parametros_One_Health (
    ID INT NOT NULL AUTO_INCREMENT,
    PROJECT_ID VARCHAR(50),
    NAME VARCHAR(50),
    WORKFLOW_DESCRIPTION VARCHAR(50),
    ORIGIN_COMPANY VARCHAR(50),
    FORMAT VARCHAR(50),
    TYPE VARCHAR(50),
    EXTENSION VARCHAR(50),
    LINK_FILE VARCHAR(50),
    DESTINATION_BUCKET VARCHAR(50),
    DESTINATION_DIRECTORY VARCHAR(50),
    NAME_FILE VARCHAR(50),
    ID_VALIDATION_FILE VARCHAR(50),
    ID_HOMOLOGATION_FILE VARCHAR(50),
    DESTINATION_BQ_LANDING VARCHAR(50),
    DESTINATION_TABLE_LANDING VARCHAR(50),
    DESTINATION_BQ_UNIVERSAL VARCHAR(50),
    DESTINATION_TABLE_UNIVERSAL VARCHAR(50),
    DESTINATION_BQ_ANALYTICS VARCHAR(50),
    DESTINATION_TABLE_ANALYTICS VARCHAR(50),
    NAME_COMPOSER VARCHAR(50),
    NAME_DAG_STORAGE_BG VARCHAR(50),
    NAME_DAG_BGLANDING_BG_UNIVERSAL VARCHAR(50),
    NAME_DAG_BGUNIVERSAL_BG_ANALYTICS VARCHAR(50),
    PRIMARY KEY (ID)
);


SELECT * FROM
  `db-compass`.`DATA_FLOW_CONFIG` LIMIT 1000;
ID
PROJECT_ID
VALIDATION_FILE_ID
HOMOLOGATION_FILE_ID
WORKFLOW_NAME
WORKFLOW_DESCRIPTION
ORIGIN_COMPANY
ORIGIN_FORMAT
ORIGIN_TYPE
ORIGIN_EXTENSION
ORIGIN_LINK_FILE
DESTINATION_BUCKET
DESTINATION_DIRECTORY
DESTINATION_FILE_NAME
DESTINATION_BG_NAME
DESTINATION_DSET_LANDING
DESTINATION_TABLE_LANDING
COMPOSER_NAME
DAG_STORAGE_DSET_NAME



INSERT INTO `db-compass`.`DATA_FLOW_CONFIG` (
  ID, 
  PROJECT_ID,
  VALIDATION_FILE_ID,
  HOMOLOGATION_FILE_ID,
  WORKFLOW_NAME,
  WORKFLOW_DESCRIPTION,
  ORIGIN_COMPANY,
  ORIGIN_FORMAT,
  ORIGIN_TYPE,
  ORIGIN_EXTENSION,
  ORIGIN_LINK_FILE,
  DESTINATION_BUCKET,
  DESTINATION_DIRECTORY,
  DESTINATION_FILE_NAME,
  DESTINATION_BG_NAME,
  DESTINATION_DSET_LANDING,
  DESTINATION_TABLE_LANDING,
  COMPOSER_NAME,
  DAG_STORAGE_DSET_NAME)
VALUES ('5050dccc-d92a-42b9-9fb2-fa9b69505bab',
'he-dev-data',
'338c2e50-1bc2-4b13-9145-95190a55e53d',
'd490efdc-0448-468d-97e4-749de76399e3', 
'Ingesta Siniestros', 
'Trae la informacion de Rimac hacia Compas', 
'RIMAC', 
'ARCHIVO',
'PARQUET', 
'.parquet',
 NULL, 
'he-dev-data', 
'he-dev-data-ipress/ipress_clinicas/internacional/', 
'data',
'he-dev-data',
'dev_data_analytics', 
'anl_tmp_part_month_siniestro_t', 
'dev_airflow_data', 
'dev_dag_siniestros_parquet_gcs_bq');






INSERT INTO `db-compass`.`DATA_FLOW_CONFIG` (
  ID, 
  PROJECT_ID,
  VALIDATION_FILE_ID,
  HOMOLOGATION_FILE_ID,
  WORKFLOW_NAME,
  WORKFLOW_DESCRIPTION,
  ORIGIN_COMPANY,
  ORIGIN_FORMAT,
  ORIGIN_TYPE,
  ORIGIN_EXTENSION,
  ORIGIN_LINK_FILE,
  DESTINATION_BUCKET,
  DESTINATION_DIRECTORY,
  DESTINATION_FILE_NAME,
  DESTINATION_BG_NAME,
  DESTINATION_DSET_LANDING,
  DESTINATION_TABLE_LANDING,
  COMPOSER_NAME,
  DAG_STORAGE_DSET_NAME)
VALUES ('d10532fa-3222-47c9-a5b4-faf62b842a11',
'he-dev-composer',
'338c2e50-1bc2-4b13-9145-95190a55e53d',
'd490efdc-0448-468d-97e4-749de76399e3', 
'Ingesta Siniestros', 
'Trae la informacion de Rimac hacia Compas', 
'RIMAC', 
'ARCHIVO',
'PARQUET', 
'.parquet',
 NULL, 
'us-east4-dev-airflow-data-9879bdea-bucket', 
'data/data-ipress/ipress_clinicas/internacional', 
'data',
'he-dev-data',
'dev_data_analytics', 
'anl_tmp_part_month_siniestro_t', 
'dev_airflow_data', 
'dev_dag_siniestros_parquet_gcs_bq');



INSERT INTO `db-compass`.`DATA_FLOW_CONFIG` (
  ID, 
  PROJECT_ID,
  VALIDATION_FILE_ID,
  HOMOLOGATION_FILE_ID,
  WORKFLOW_NAME,
  WORKFLOW_DESCRIPTION,
  ORIGIN_COMPANY,
  ORIGIN_FORMAT,
  ORIGIN_TYPE,
  ORIGIN_EXTENSION,
  ORIGIN_LINK_FILE,
  DESTINATION_BUCKET,
  DESTINATION_DIRECTORY,
  DESTINATION_FILE_NAME,
  DESTINATION_BG_NAME,
  DESTINATION_DSET_LANDING,
  DESTINATION_TABLE_LANDING,
  COMPOSER_NAME,
  DAG_STORAGE_DSET_NAME)
VALUES ('a379e9be-14fe-4914-9321-76dc32f5b233',
'he-dev-composer',
'338c2e50-1bc2-4b13-9145-95190a55e53d',
'd490efdc-0448-468d-97e4-749de76399e3', 
'Ingesta Tabla', 
'Trae la informacion de Rimac hacia Compas', 
'RIMAC', 
'ARCHIVO',
'PARQUET', 
'.parquet',
 NULL, 
'us-east4-dev-airflow-data-9879bdea-bucket', 
'data/data-ipress/ipress_clinicas/internacional', 
'personas',
'he-dev-data',
'dev_data_analytics', 
'anl_tmp_part_month_siniestro_t', 
'dev_airflow_data', 
'dev_dag_siniestros_parquet_gcs_bq');


----CLOUD SQL MySQL TABLA PARAMETROS CON CREACION DE TABLA -----

SELECT PROJECT_ID,ID  ,DESTINATION_DSET_LANDING,DESTINATION_TABLE_LANDING,SQL_SCRIPT FROM
  `db-compass`.`DATA_FLOW_CONFIG` LIMIT 1000;

SELECT PROJECT_ID,ID   FROM
  `db-compass`.`DATA_FLOW_CONFIG` LIMIT 1000;

update`db-compass`.`DATA_FLOW_CONFIG`
set SQL_SCRIPT = 'CREATE TABLE IF NOT EXISTS `he-dev-composer.dev_data_analytics.anl_tmp_part_month_siniestro_t` ( id_siniestro STRING, id_origen STRING, id_siniestro_origen STRING, id_poliza STRING, id_contratante STRING, id_producto STRING, id_certificado STRING, id_titular STRING, fec_hora_ocurrencia TIMESTAMP, fec_notificacion TIMESTAMP, num_siniestro STRING, fec_operacion TIMESTAMP, id_estado_siniestro_origen STRING, des_estado_siniestro_origen STRING, id_estado_siniestro STRING, des_estado_siniestro STRING, fec_estado TIMESTAMP, id_ubicacion_geografica STRING, des_departamento_siniestro STRING, des_provincia_siniestro STRING, des_distrito_siniestro STRING, des_ubicacion_detallada STRING, id_unidad_asegurable STRING, id_asegurado STRING, des_unidad_asegurable STRING, sexo_asegurable STRING, fec_nacimiento_asegurable DATE, num_documento_asegurable STRING, tip_documento_asegurable STRING, rango_etareo_asegurable STRING, fec_constitucion_siniestro TIMESTAMP, fec_anulacion TIMESTAMP, fec_ult_liquidacion TIMESTAMP, mto_total_reserva NUMERIC, mto_total_reserva_usd NUMERIC, mto_total_reserva_ajustador NUMERIC, mto_total_reserva_usd_ajustador NUMERIC, mto_total_aprobado NUMERIC, mto_total_aprobado_usd NUMERIC, mto_aprobado_deduci NUMERIC, mto_aprobado_deduci_usd NUMERIC, mto_total_facturado NUMERIC, mto_total_facturado_usd NUMERIC, mto_total_pendiente NUMERIC, mto_total_pendiente_usd NUMERIC, mto_total_pagado NUMERIC, mto_total_pagado_usd NUMERIC, mto_total_pagado_ajustador NUMERIC, mto_total_pagado_usd_ajustador NUMERIC, mto_pagado_deduci NUMERIC, mto_pagado_deduci_usd NUMERIC, mto_potencial_usd NUMERIC, id_oficina_reclamo_origen STRING, id_oficina_reclamo STRING, des_oficina_reclamo STRING, id_motivo_estado_origen STRING, des_motivo_estado_origen STRING, id_motivo_estado STRING, des_motivo_estado STRING, id_moneda STRING, id_compania STRING, tipo_flujo STRING, porcentaje_coasegurador_rimac FLOAT64, mto_reserva_np FLOAT64, mto_reserva_usd_np FLOAT64, mto_aprobado_np FLOAT64, mto_aprobado_usd_np FLOAT64, mto_pagado_np FLOAT64, mto_pagado_usd_np FLOAT64, tip_atencion_siniestro STRING, tip_procedencia STRING, fec_insercion DATE, fec_modificacion DATE, bq__soft_deleted BOOL, partition_date TIMESTAMP ) PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

update`db-compass`.`DATA_FLOW_CONFIG`
set SQL_SCRIPT = 'CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_analytics.anl_tmp_part_month_siniestro_t` ( id_siniestro STRING, id_origen STRING, id_siniestro_origen STRING, id_poliza STRING, id_contratante STRING, id_producto STRING, id_certificado STRING, id_titular STRING, fec_hora_ocurrencia TIMESTAMP, fec_notificacion TIMESTAMP, num_siniestro STRING, fec_operacion TIMESTAMP, id_estado_siniestro_origen STRING, des_estado_siniestro_origen STRING, id_estado_siniestro STRING, des_estado_siniestro STRING, fec_estado TIMESTAMP, id_ubicacion_geografica STRING, des_departamento_siniestro STRING, des_provincia_siniestro STRING, des_distrito_siniestro STRING, des_ubicacion_detallada STRING, id_unidad_asegurable STRING, id_asegurado STRING, des_unidad_asegurable STRING, sexo_asegurable STRING, fec_nacimiento_asegurable DATE, num_documento_asegurable STRING, tip_documento_asegurable STRING, rango_etareo_asegurable STRING, fec_constitucion_siniestro TIMESTAMP, fec_anulacion TIMESTAMP, fec_ult_liquidacion TIMESTAMP, mto_total_reserva NUMERIC, mto_total_reserva_usd NUMERIC, mto_total_reserva_ajustador NUMERIC, mto_total_reserva_usd_ajustador NUMERIC, mto_total_aprobado NUMERIC, mto_total_aprobado_usd NUMERIC, mto_aprobado_deduci NUMERIC, mto_aprobado_deduci_usd NUMERIC, mto_total_facturado NUMERIC, mto_total_facturado_usd NUMERIC, mto_total_pendiente NUMERIC, mto_total_pendiente_usd NUMERIC, mto_total_pagado NUMERIC, mto_total_pagado_usd NUMERIC, mto_total_pagado_ajustador NUMERIC, mto_total_pagado_usd_ajustador NUMERIC, mto_pagado_deduci NUMERIC, mto_pagado_deduci_usd NUMERIC, mto_potencial_usd NUMERIC, id_oficina_reclamo_origen STRING, id_oficina_reclamo STRING, des_oficina_reclamo STRING, id_motivo_estado_origen STRING, des_motivo_estado_origen STRING, id_motivo_estado STRING, des_motivo_estado STRING, id_moneda STRING, id_compania STRING, tipo_flujo STRING, porcentaje_coasegurador_rimac FLOAT64, mto_reserva_np FLOAT64, mto_reserva_usd_np FLOAT64, mto_aprobado_np FLOAT64, mto_aprobado_usd_np FLOAT64, mto_pagado_np FLOAT64, mto_pagado_usd_np FLOAT64, tip_atencion_siniestro STRING, tip_procedencia STRING, fec_insercion DATE, fec_modificacion DATE, bq__soft_deleted BOOL, partition_date TIMESTAMP ) PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);'
where ID = '5050dccc-d92a-42b9-9fb2-fa9b69505bab';


ALTER TABLE DATA_FLOW_CONFIG
ADD COLUMN DESTINATION_DSET_UNIVERSAL VARCHAR(255),
ADD COLUMN DESTINATION_TABLE_UNIVERSAL VARCHAR(255);
ADD DAG_ID_PARQUET_BQ VARCHAR(255);
ADD DAG_ID_BQ_LANDING_TO_BQ_UNIVERSAL VARCHAR(255);
ADD DAG_ID_BQ_UNIVERSAL_TO_BQ_ANALYTICS VARCHAR(255);

update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_DSET_UNIVERSAL = 'dev_data_analytics'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_TABLE_UNIVERSAL = 'anl_tmp_part_month_siniestro_t2'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

---Validar dag - parquet a gcs ---

DROP TABLE `he-dev-composer.dev_data_analytics.anl_tmp_part_month_siniestro_t`;

SELECT COUNT(*) FROM `he-dev-composer.dev_data_analytics.anl_tmp_part_month_siniestro_t`;

---- Permisos de he-dev-compass a he-dev-data---
-----he-dev-composer --465256260181-compute@developer.gserviceaccount.com ----- gs://us-east4-dev-airflow-data-9879bdea-bucket/dags 
-----he-dev-data ---548349234634-compute@developer.gserviceaccount.com----gs://us-east4-dev-airflow-data-728aa61b-bucket/dags

update`db-compass`.`DATA_FLOW_CONFIG`
set PROJECT_ID = 'he-dev-data'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11';

update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_DSET_LANDING = 'dev_data_landing'
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 

update`db-compass`.`DATA_FLOW_CONFIG`
set DESTINATION_TABLE_LANDING = 'iafas_diferencial_siniestros'   ----Nombre de tabla
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 

update`db-compass`.`DATA_FLOW_CONFIG`
set SQL_SCRIPT = 'CREATE TABLE `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`
(
  id_siniestro STRING,
  id_origen STRING,
  id_siniestro_origen STRING,
  id_poliza STRING,
  id_contratante STRING,
  id_producto STRING,
  id_certificado STRING,
  id_titular STRING,
  fec_hora_ocurrencia TIMESTAMP,
  fec_notificacion TIMESTAMP,
  num_siniestro STRING,
  fec_operacion TIMESTAMP,
  id_estado_siniestro_origen STRING,
  des_estado_siniestro_origen STRING,
  id_estado_siniestro STRING,
  des_estado_siniestro STRING,
  fec_estado TIMESTAMP,
  id_ubicacion_geografica STRING,
  des_departamento_siniestro STRING,
  des_provincia_siniestro STRING,
  des_distrito_siniestro STRING,
  des_ubicacion_detallada STRING,
  id_unidad_asegurable STRING,
  id_asegurado STRING,
  des_unidad_asegurable STRING,
  sexo_asegurable STRING,
  fec_nacimiento_asegurable DATE,
  num_documento_asegurable STRING,
  tip_documento_asegurable STRING,
  rango_etareo_asegurable STRING,
  fec_constitucion_siniestro TIMESTAMP,
  fec_anulacion TIMESTAMP,
  fec_ult_liquidacion TIMESTAMP,
  mto_total_reserva NUMERIC,
  mto_total_reserva_usd NUMERIC,
  mto_total_reserva_ajustador NUMERIC,
  mto_total_reserva_usd_ajustador NUMERIC,
  mto_total_aprobado NUMERIC,
  mto_total_aprobado_usd NUMERIC,
  mto_aprobado_deduci NUMERIC,
  mto_aprobado_deduci_usd NUMERIC,
  mto_total_facturado NUMERIC,
  mto_total_facturado_usd NUMERIC,
  mto_total_pendiente NUMERIC,
  mto_total_pendiente_usd NUMERIC,
  mto_total_pagado NUMERIC,
  mto_total_pagado_usd NUMERIC,
  mto_total_pagado_ajustador NUMERIC,
  mto_total_pagado_usd_ajustador NUMERIC,
  mto_pagado_deduci NUMERIC,
  mto_pagado_deduci_usd NUMERIC,
  mto_potencial_usd NUMERIC,
  id_oficina_reclamo_origen STRING,
  id_oficina_reclamo STRING,
  des_oficina_reclamo STRING,
  id_motivo_estado_origen STRING,
  des_motivo_estado_origen STRING,
  id_motivo_estado STRING,
  des_motivo_estado STRING,
  id_moneda STRING,
  id_compania STRING,
  tipo_flujo STRING,
  porcentaje_coasegurador_rimac FLOAT64,
  mto_reserva_np FLOAT64,
  mto_reserva_usd_np FLOAT64,
  mto_aprobado_np FLOAT64,
  mto_aprobado_usd_np FLOAT64,
  mto_pagado_np FLOAT64,
  mto_pagado_usd_np FLOAT64,
  tip_atencion_siniestro STRING,
  tip_procedencia STRING,
  fec_insercion DATE,
  fec_modificacion DATE,
  bq__soft_deleted BOOL,
  partition_date TIMESTAMP
)
PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);'   
where ID = 'd10532fa-3222-47c9-a5b4-faf62b842a11'; 

INSERT INTO `db-compass`.`DATA_FLOW_CONFIG` (
  ID, 
  PROJECT_ID,
  VALIDATION_FILE_ID,
  HOMOLOGATION_FILE_ID,
  WORKFLOW_NAME,
  WORKFLOW_DESCRIPTION,
  ORIGIN_COMPANY,
  ORIGIN_FORMAT,
  ORIGIN_TYPE,
  ORIGIN_EXTENSION,
  ORIGIN_LINK_FILE,
  DESTINATION_BUCKET,
  DESTINATION_DIRECTORY,
  DESTINATION_FILE_NAME,
  DESTINATION_BG_NAME,
  DESTINATION_DSET_LANDING,
  DESTINATION_TABLE_LANDING,
  COMPOSER_NAME,
  DAG_STORAGE_DSET_NAME)
VALUES ('a379e9be-14fe-4914-9321-76dc32f5b233',
'he-dev-data',
'338c2e50-1bc2-4b13-9145-95190a55e53d',
'd490efdc-0448-468d-97e4-749de76399e3', 
'Ingesta Tabla', 
'Trae la informacion de Rimac hacia Compas', 
'RIMAC', 
'ARCHIVO',
'PARQUET', 
'.parquet',
 NULL, 
'us-east4-dev-airflow-data-9879bdea-bucket', 
'data/data-ipress/ipress_clinicas/internacional', 
'mdm_siniestro_dif_20240611212129',
'he-dev-data',
'dev_data_landing', 
'iafas_mdm_siniestro_dif', 
'dev_airflow_data', 
'dev_dag_siniestros_parquet_gcs_bq');


update `db-compass`.`DATA_FLOW_CONFIG`
SET SQL_SCRIPT ='CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_siniestro_dif` ( id_siniestro STRING,
    id_origen STRING,
    id_siniestro_origen STRING,
    id_poliza STRING,
    id_contratante STRING,
    id_producto STRING,
    id_certificado STRING,
    id_titular STRING,
    fec_hora_ocurrencia TIMESTAMP,
    fec_notificacion TIMESTAMP,
    num_siniestro STRING,
    fec_operacion TIMESTAMP,
    id_estado_siniestro_origen STRING,
    des_estado_siniestro_origen STRING,
    id_estado_siniestro STRING,
    des_estado_siniestro STRING,
    fec_estado TIMESTAMP,
    id_ubicacion_geografica STRING,
    des_departamento_siniestro STRING,
    des_provincia_siniestro STRING,
    des_distrito_siniestro STRING,
    des_ubicacion_detallada STRING,
    id_unidad_asegurable STRING,
    id_asegurado STRING,
    des_unidad_asegurable STRING,
    sexo_asegurable STRING,
    fec_nacimiento_asegurable DATE,
    num_documento_asegurable STRING,
    tip_documento_asegurable STRING,
    rango_etareo_asegurable STRING,
    fec_constitucion_siniestro TIMESTAMP,
    fec_anulacion TIMESTAMP,
    fec_ult_liquidacion TIMESTAMP,
    mto_total_reserva NUMERIC,
    mto_total_reserva_usd NUMERIC,
    mto_total_reserva_ajustador NUMERIC,
    mto_total_reserva_usd_ajustador NUMERIC,
    mto_total_aprobado NUMERIC,
    mto_total_aprobado_usd NUMERIC,
    mto_aprobado_deduci NUMERIC,
    mto_aprobado_deduci_usd NUMERIC,
    mto_total_facturado NUMERIC,
    mto_total_facturado_usd NUMERIC,
    mto_total_pendiente NUMERIC,
    mto_total_pendiente_usd NUMERIC,
    mto_total_pagado NUMERIC,
    mto_total_pagado_usd NUMERIC,
    mto_total_pagado_ajustador NUMERIC,
    mto_total_pagado_usd_ajustador NUMERIC,
    mto_pagado_deduci NUMERIC,
    mto_pagado_deduci_usd NUMERIC,
    mto_potencial_usd NUMERIC,
    id_oficina_reclamo_origen STRING,
    id_oficina_reclamo STRING,
    des_oficina_reclamo STRING,
    id_motivo_estado_origen STRING,
    des_motivo_estado_origen STRING,
    id_motivo_estado STRING,
    des_motivo_estado STRING,
    id_moneda STRING,
    id_compania STRING,
    tipo_flujo STRING,
    porcentaje_coasegurador_rimac FLOAT64,
    mto_reserva_np FLOAT64,
    mto_reserva_usd_np FLOAT64,
    mto_aprobado_np FLOAT64,
    mto_aprobado_usd_np FLOAT64,
    mto_pagado_np FLOAT64,
    mto_pagado_usd_np FLOAT64,
    tip_atencion_siniestro STRING,
    tip_procedencia STRING,
    fec_insercion DATE,
    fec_modificacion DATE,
    bq__soft_deleted BOOL,
    partition_date TIMESTAMP,
    est_cambios STRING
)PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);'
WHERE ID = 'a379e9be-14fe-4914-9321-76dc32f5b233'


SELECT ID,DESTINATION_TABLE_LANDING,DESTINATION_FILE_NAME FROM
`db-compass`.`DATA_FLOW_CONFIG`;


 ---- PREPARACION -----
 ---DESTINATION_FILE_NAME ---  nombre_archivo---
 ---DESTINATION_TABLE_LANDING --- iafas_diferencial_siniestros


UPDATE `db-compass`.`DATA_FLOW_CONFIG`
SET DESTINATION_FILE_NAME = 'siniestro'
WHERE ID ='d10532fa-3222-47c9-a5b4-faf62b842a11'


UPDATE `db-compass`.`DATA_FLOW_CONFIG`
SET DESTINATION_TABLE_LANDING = 'iafas_mdm_siniestro'
WHERE ID ='d10532fa-3222-47c9-a5b4-faf62b842a11';


------ VALIDACION CLOUD SQL--- 

UPDATE `db-compass`.`DATA_FLOW_CONFIG`
SET DESTINATION_FILE_NAME = 'COLOCAR_NOMBRE'
WHERE ID ='d10532fa-3222-47c9-a5b4-faf62b842a11'

------ VALIDACION BIG QUERY--- 

DROP TABLE `he-dev-data.dev_data_landing.iafas_mdm_siniestro`;
SELECT COUNT(*) FROM `he-dev-data.dev_data_landing.iafas_mdm_siniestro`;


