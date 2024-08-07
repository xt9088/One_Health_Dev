--- ESQUEMA Y COLUMNAS DE NUEVA TABLAS COMPASS ----
CREATE OR REPLACE EXTERNAL TABLE `he-dev-data.dev_data_landing.iafas_mdm_prima_inducida`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://he-dev-data/he-dev-data-ipress/ipress_clinicas/internacional/prima_inducida/mdm_prima_inducida_20240807.parquet']); 

--- Obtener las columnas---
SELECT column_name, data_type
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'iafas_mdm_prima_inducida';

---- Bajar las columnas a csv y luego pedir en internet un CREATE TABLE IF NOT EXISTS --- 

---- ELIMINAR TABLA EXTERNA ---
---- VALIDAR EL DDL DE CREACION EN BIGQUERY ----
---- GUARDAR EN EL INSERT  -----

Drop table `he-dev-data.dev_data_landing.iafas_mdm_carta_garantia`; 
Drop table `he-dev-data.dev_data_landing.iafas_mdm_persona`;
Drop table `he-dev-data.dev_data_landing.iafas_mdm_poliza`;
Drop table `he-dev-data.dev_data_landing.iafas_mdm_siniestro`;
Drop table `he-dev-data.dev_data_landing.iafas_mdm_prima_inducida`;

select * from `he-dev-data.dev_data_landing.iafas_mdm_carta_garantia`;
select * from `he-dev-data.dev_data_landing.iafas_mdm_persona`;
select * from `he-dev-data.dev_data_landing.iafas_mdm_poliza`;
select * from `he-dev-data.dev_data_landing.iafas_mdm_siniestro`;
select * from `he-dev-data.dev_data_landing.iafas_mdm_prima_inducida`;

----- tabla normal ----- 

SELECT t.ddl 
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.TABLES` t
WHERE t.table_name = 'iafas_mdm_siniestro_dif';


---- SCRIPT DE TABLA A CREAR -----

SELECT t.ddl 
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.TABLES` t
WHERE t.table_name = 'anl_tmp_part_month_siniestro_t';

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column est_cambios;

SELECT * FROM `he-dev-data.dev_data_landing.iafas_mdm_siniestro2` LIMIT 100;  

SELECT COUNT(*) AS column_count
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'iafas_mdm_siniestro2';

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column est_cambios;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column des_sexo_afiliado;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column edad_afiliado_ocurrencia;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column edad_afiliado;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column fec_nacimiento_afiliado;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column fec_ult_liquidacion;



alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column fec_hora_ocurrencia;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column fec_ult_liquidacion;	

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column fec_notificacion;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column	fec_operacion;	

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column mnt_gasto_total_con_igv;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column nro_nota_credito;

alter table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`
drop column	mnt_nota_credito;

drop table `he-dev-data.dev_data_landing.iafas_mdm_siniestro2`;




--- VALIDAR TRIGGER Y CARGA DIFERENCIAL (6) Y CARGA 1M ------

DROP TABLE `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`;
SELECT COUNT(*) FROM `he-dev-data.dev_data_landing.iafas_diferencial_siniestros`;

DROP TABLE `he-dev-data.dev_data_landing.iafas_mdm_siniestro_dif`;
SELECT count(*) FROM `he-dev-data.dev_data_landing.iafas_mdm_siniestro_dif`;

------- SCHEMA TABLA 19/06/24 ----- 

SELECT column_name, data_type
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'iafas_mdm_siniestro';


CREATE TABLE IF NOT EXISTS your_dataset.your_table (
  id_siniestro STRING,
  tip_ingreso_siniestro NUMERIC,
  tip_reclamo STRING,
  des_tipo_reclamo STRING,
  num_obligacion STRING,
  tip_procedencia STRING,
  ind_tedef_salud STRING,
  fec_hora_ocurrencia TIMESTAMP,
  fec_ult_liquidacion TIMESTAMP,
  fec_notificacion TIMESTAMP,
  fec_operacion TIMESTAMP,
  mnt_gasto_total_con_igv NUMERIC,
  nro_nota_credito STRING,
  mnt_nota_credito NUMERIC,
  mnt_planilla_afiliado_usd NUMERIC,
  mnt_planilla_afiliado_sol NUMERIC,
  mnt_observado NUMERIC,
  id_persona_proveedor_siniestro STRING,
  tip_documento_proveedor_siniestro STRING,
  num_documento_proveedor_siniestro STRING,
  nom_completo_proveedor_siniestro STRING,
  dir_proveedor_siniestro STRING,
  cod_sede_proveedor_siniestro STRING,
  nom_sede_proveedor_siniestro STRING,
  dir_sede_proveedor_siniestro STRING,
  id_persona_afiliado STRING,
  tip_documento_afiliado STRING,
  num_documento_afiliado STRING,
  nom_completo_afiliado STRING,
  fec_nacimiento_afiliado DATE,
  edad_afiliado NUMERIC,
  edad_afiliado_ocurrencia NUMERIC,
  des_sexo_afiliado STRING,
  est_cambios STRING
);

  UPDATE `db-compass`.`DATA_FLOW_CONFIG`
  SET DESTINATION_FILE_NAME = siniestro
  WHERE ID =''