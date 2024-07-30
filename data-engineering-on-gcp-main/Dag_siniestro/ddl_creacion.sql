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








CREATE TABLE `he-dev-data.dev_data_analytics.anl_tmp_part_month_siniestro_t`
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
PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);

--- ESQUEMA Y COLUMNAS DE NUEVA TABLA SINIESTRO ----
CREATE OR REPLACE EXTERNAL TABLE `he-dev-data.dev_data_landing.iafas_cartas-garantia`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://he-dev-data/he-dev-data-ipress/ipress_clinicas/internacional/cartas-garantia/mdm_cartas-garantia_30072024.parquet']
); 

SELECT t.ddl 
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.TABLES` t
WHERE t.table_name = 'iafas_mdm_siniestro_dif';

SELECT column_name, data_type
FROM `he-dev-data.dev_data_landing.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'iafas_mdm_siniestro';


---- SCRIPT DE TABLA A CREAR -----

CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_siniestro_dif` (
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
    partition_date TIMESTAMP,
    est_cambios STRING
)
PARTITION BY TIMESTAMP_TRUNC(fec_hora_ocurrencia, MONTH);

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