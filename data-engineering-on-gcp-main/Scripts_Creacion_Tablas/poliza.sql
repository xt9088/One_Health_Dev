/*********************************************************************************************************************************************
# Organización: Rimac
# Programa: ef_poliza_modificados.sql
# Creado por: Percy Campos L.
# Fecha Creación: 01/08/2024
# Propósito: Generar informacion de cartera corredorres
# Fuentes de datos:
		
            - {project_id_anl}.anl_produccion.poliza

# Destino:
			- {project_id_anl}.delivery_onehealth.ef_poliza_modificados.sql
#
# Historial de Modificaciones
 
# Versión		Autor								Fecha					Detalle
- 001			Percy Campos L.				        02-07-2024      	    Proceso de creación de tabla poliza_modificados
 
*********************************************************************************************************************************************/

DECLARE number_of_files INT64;
DECLARE date_variable DATE;

SET date_variable = (
SELECT MAX(periodo) from `{project_id_anl}.anl_produccion.poliza`
);

CREATE OR REPLACE TEMP TABLE temp_hoy AS
SELECT DISTINCT id_poliza,
  id_contratante,
  nom_contratante,
  nom_persona_contratante,
  ape_paterno_contratante,
  ape_materno_contratante,
  des_sexo_contratante,
  fec_nacimiento_contratante,
  tip_persona_contratante,
  tip_documento_contratante,
  num_documento_contratante,
  des_distrito_contratante,
  des_provincia_contratante,
  des_departamento_contratante,
  id_subsegmento_contratante,
  des_subsegmento_contratante,
  id_segmento_contratante,
  des_segmento_contratante,
  id_grupo_economico_contratante,
  des_grupo_economico_contratante,
  des_empresa_sector_economico_contratante,
  id_producto,
  nom_producto,
  ind_fronting,
  num_poliza,
  id_poliza_origen,
  tip_suscripcion,
  id_poliza_referencia,
  mnt_prima_emitida_bruta_anualizada_poliza,
  fec_emision,
  fec_inicio_vigencia,
  fec_fin_vigencia,
  tip_vigencia_poliza,
  id_est_poliza_origen,
  des_est_poliza_origen,
  id_est_poliza,
  des_est_poliza,
  fec_anulacion,
  des_periodo_anulacion_poliza,
  fec_renovacion,
  id_motivo_anulacion_origen,
  id_motivo_anulacion,
  des_motivo_anulacion,
  id_canal,
  des_canal,
  id_subcanal,
  des_subcanal,
  des_subtipocanal,
  id_intermediario,
  nom_intermediario,
  id_corredor_nt,
  nom_corredor_nt,
  cod_producto_origen,
  frecuencia_pago,
  id_moneda,
  ind_gestionable,
  des_nivel_valor_prod,
  des_nivel_riesgo_prod,
  ind_renovacion_autom,
  id_compania,
  id_origen,
  ind_poliza_vigente,
  id_certificado,
  num_certificado_origen,
  id_estado_certificado_origen,
  id_certificado_origen,
  id_est_certificado,
  est_certificado,
  fec_inicio_vigencia_certificado,
  fec_fin_vigencia_certificado,
  fec_ingreso_certificado,
  fec_exclusion_certificado,
  id_motivo_exclusion_origen_certificado,
  id_motivo_exclusion_certificado,
  des_motivo_exclusion_certificado,
  id_via_cobro_certificado,
  des_via_cobro_certificado,
  tip_modalidad_cobro_certificado,
  fec_operacion_anulacion_certificado,
  des_periodo_operacion_anulacion,
  ind_certificado_vigente,
  ind_certificado_digital,
  mnt_prima_convenida,
  id_titular,
  nom_completo_titular,
  nom_persona_titular,
  ape_paterno_titular,
  ape_materno_titular,
  des_sexo_titular,
  fec_nacimiento_titular,
  tip_persona_titular,
  tip_documento_titular,
  des_distrito_titular,
  des_provincia_titular,
  des_departamento_titular,
  unidad_asegurable,
  endoso_cesion_derecho,
  periodo,
  fec_procesamiento,
"nuevo" as est_cambios, 
date(null) as fec_insercion,
date(null) as fec_modificacion,
false as bq__soft_deleted
 FROM `{project_id_anl}.anl_produccion.poliza`
WHERE periodo = DATE(CONCAT(EXTRACT(YEAR FROM date_variable), '-', EXTRACT(MONTH FROM date_variable), '-', '01')); --order by id_poliza limit 1000000;

--SELECT (CONCAT(EXTRACT(YEAR FROM CURRENT_DATE()), '-', EXTRACT(MONTH FROM CURRENT_DATE()), '-', '01'));
-- Step 2: Update a value if the table is not empty
IF NOT (
          SELECT COUNT(*) = 0
          FROM `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
        ) 
  THEN
  --Resetear el estado de todos a  universo 
  UPDATE `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
  SET est_cambios = 'universo'
  WHERE true;

  CREATE OR REPLACE TEMP TABLE temp_ayer AS
  SELECT * FROM `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`;

  --empezar proceso
    -- crear tabla temporal que contiene ids cambiados
   
   CREATE OR REPLACE TEMP TABLE ids_modif AS
(
  SELECT id_poliza FROM (
    SELECT
      CASE
        WHEN t1.id_poliza <> t2.id_poliza THEN 'id_poliza'
        WHEN t1.id_contratante <> t2.id_contratante THEN 'id_contratante'
        WHEN t1.nom_contratante <> t2.nom_contratante THEN 'nom_contratante'
        WHEN t1.nom_persona_contratante <> t2.nom_persona_contratante THEN 'nom_persona_contratante'
        WHEN t1.ape_paterno_contratante <> t2.ape_paterno_contratante THEN 'ape_paterno_contratante'
        WHEN t1.ape_materno_contratante <> t2.ape_materno_contratante THEN 'ape_materno_contratante'
        WHEN t1.des_sexo_contratante <> t2.des_sexo_contratante THEN 'des_sexo_contratante'
        WHEN t1.fec_nacimiento_contratante <> t2.fec_nacimiento_contratante THEN 'fec_nacimiento_contratante'
        WHEN t1.tip_persona_contratante <> t2.tip_persona_contratante THEN 'tip_persona_contratante'
        WHEN t1.tip_documento_contratante <> t2.tip_documento_contratante THEN 'tip_documento_contratante'
        WHEN t1.num_documento_contratante <> t2.num_documento_contratante THEN 'num_documento_contratante'
        WHEN t1.des_distrito_contratante <> t2.des_distrito_contratante THEN 'des_distrito_contratante'
        WHEN t1.des_provincia_contratante <> t2.des_provincia_contratante THEN 'des_provincia_contratante'
        WHEN t1.des_departamento_contratante <> t2.des_departamento_contratante THEN 'des_departamento_contratante'
        WHEN t1.id_subsegmento_contratante <> t2.id_subsegmento_contratante THEN 'id_subsegmento_contratante'
        WHEN t1.des_subsegmento_contratante <> t2.des_subsegmento_contratante THEN 'des_subsegmento_contratante'
        WHEN t1.id_segmento_contratante <> t2.id_segmento_contratante THEN 'id_segmento_contratante'
        WHEN t1.des_segmento_contratante <> t2.des_segmento_contratante THEN 'des_segmento_contratante'
        WHEN t1.id_grupo_economico_contratante <> t2.id_grupo_economico_contratante THEN 'id_grupo_economico_contratante'
        WHEN t1.des_grupo_economico_contratante <> t2.des_grupo_economico_contratante THEN 'des_grupo_economico_contratante'
        WHEN t1.des_empresa_sector_economico_contratante <> t2.des_empresa_sector_economico_contratante THEN 'des_empresa_sector_economico_contratante'
        WHEN t1.id_producto <> t2.id_producto THEN 'id_producto'
        WHEN t1.nom_producto <> t2.nom_producto THEN 'nom_producto'
        WHEN t1.ind_fronting <> t2.ind_fronting THEN 'ind_fronting'
        WHEN t1.num_poliza <> t2.num_poliza THEN 'num_poliza'
        WHEN t1.id_poliza_origen <> t2.id_poliza_origen THEN 'id_poliza_origen'
        WHEN t1.tip_suscripcion <> t2.tip_suscripcion THEN 'tip_suscripcion'
        WHEN t1.id_poliza_referencia <> t2.id_poliza_referencia THEN 'id_poliza_referencia'
        WHEN t1.mnt_prima_emitida_bruta_anualizada_poliza <> t2.mnt_prima_emitida_bruta_anualizada_poliza THEN 'mnt_prima_emitida_bruta_anualizada_poliza'
        WHEN t1.fec_emision <> t2.fec_emision THEN 'fec_emision'
        WHEN t1.fec_inicio_vigencia <> t2.fec_inicio_vigencia THEN 'fec_inicio_vigencia'
        WHEN t1.fec_fin_vigencia <> t2.fec_fin_vigencia THEN 'fec_fin_vigencia'
        WHEN t1.tip_vigencia_poliza <> t2.tip_vigencia_poliza THEN 'tip_vigencia_poliza'
        WHEN t1.id_est_poliza_origen <> t2.id_est_poliza_origen THEN 'id_est_poliza_origen'
        WHEN t1.des_est_poliza_origen <> t2.des_est_poliza_origen THEN 'des_est_poliza_origen'
        WHEN t1.id_est_poliza <> t2.id_est_poliza THEN 'id_est_poliza'
        WHEN t1.des_est_poliza <> t2.des_est_poliza THEN 'des_est_poliza'
        WHEN t1.fec_anulacion <> t2.fec_anulacion THEN 'fec_anulacion'
        WHEN t1.des_periodo_anulacion_poliza <> t2.des_periodo_anulacion_poliza THEN 'des_periodo_anulacion_poliza'
        WHEN t1.fec_renovacion <> t2.fec_renovacion THEN 'fec_renovacion'
        WHEN t1.id_motivo_anulacion_origen <> t2.id_motivo_anulacion_origen THEN 'id_motivo_anulacion_origen'
        WHEN t1.id_motivo_anulacion <> t2.id_motivo_anulacion THEN 'id_motivo_anulacion'
        WHEN t1.des_motivo_anulacion <> t2.des_motivo_anulacion THEN 'des_motivo_anulacion'
        WHEN t1.id_canal <> t2.id_canal THEN 'id_canal'
        WHEN t1.des_canal <> t2.des_canal THEN 'des_canal'
        WHEN t1.id_subcanal <> t2.id_subcanal THEN 'id_subcanal'
        WHEN t1.des_subcanal <> t2.des_subcanal THEN 'des_subcanal'
        WHEN t1.des_subtipocanal <> t2.des_subtipocanal THEN 'des_subtipocanal'
        WHEN t1.id_intermediario <> t2.id_intermediario THEN 'id_intermediario'
        WHEN t1.nom_intermediario <> t2.nom_intermediario THEN 'nom_intermediario'
        WHEN t1.id_corredor_nt <> t2.id_corredor_nt THEN 'id_corredor_nt'
        WHEN t1.nom_corredor_nt <> t2.nom_corredor_nt THEN 'nom_corredor_nt'
        WHEN t1.cod_producto_origen <> t2.cod_producto_origen THEN 'cod_producto_origen'
        WHEN t1.frecuencia_pago <> t2.frecuencia_pago THEN 'frecuencia_pago'
        WHEN t1.id_moneda <> t2.id_moneda THEN 'id_moneda'
        WHEN t1.ind_gestionable <> t2.ind_gestionable THEN 'ind_gestionable'
        WHEN t1.des_nivel_valor_prod <> t2.des_nivel_valor_prod THEN 'des_nivel_valor_prod'
        WHEN t1.des_nivel_riesgo_prod <> t2.des_nivel_riesgo_prod THEN 'des_nivel_riesgo_prod'
        WHEN t1.ind_renovacion_autom <> t2.ind_renovacion_autom THEN 'ind_renovacion_autom'
        WHEN t1.id_compania <> t2.id_compania THEN 'id_compania'
        WHEN t1.id_origen <> t2.id_origen THEN 'id_origen'
        WHEN t1.ind_poliza_vigente <> t2.ind_poliza_vigente THEN 'ind_poliza_vigente'
        WHEN t1.id_certificado <> t2.id_certificado THEN 'id_certificado'
        WHEN t1.num_certificado_origen <> t2.num_certificado_origen THEN 'num_certificado_origen'
        WHEN t1.id_estado_certificado_origen <> t2.id_estado_certificado_origen THEN 'id_estado_certificado_origen'
        WHEN t1.id_certificado_origen <> t2.id_certificado_origen THEN 'id_certificado_origen'
        WHEN t1.id_est_certificado <> t2.id_est_certificado THEN 'id_est_certificado'
        WHEN t1.est_certificado <> t2.est_certificado THEN 'est_certificado'
        WHEN t1.fec_inicio_vigencia_certificado <> t2.fec_inicio_vigencia_certificado THEN 'fec_inicio_vigencia_certificado'
        WHEN t1.fec_fin_vigencia_certificado <> t2.fec_fin_vigencia_certificado THEN 'fec_fin_vigencia_certificado'
        WHEN t1.fec_ingreso_certificado <> t2.fec_ingreso_certificado THEN 'fec_ingreso_certificado'
        WHEN t1.fec_exclusion_certificado <> t2.fec_exclusion_certificado THEN 'fec_exclusion_certificado'
        WHEN t1.id_motivo_exclusion_origen_certificado <> t2.id_motivo_exclusion_origen_certificado THEN 'id_motivo_exclusion_origen_certificado'
        WHEN t1.id_motivo_exclusion_certificado <> t2.id_motivo_exclusion_certificado THEN 'id_motivo_exclusion_certificado'
        WHEN t1.des_motivo_exclusion_certificado <> t2.des_motivo_exclusion_certificado THEN 'des_motivo_exclusion_certificado'
       
 
    --SELECT * FROM temp_ayer WHERE id_poliza = 'RS-2-1-10-65756246';
    --SELECT * FROM temp_hoy WHERE id_poliza = 'RS-2-1-10-65756246';
    -- crear tabla temporal que contiene ids nuevos
    CREATE OR REPLACE TEMP TABLE ids_nuevos AS
    (
          SELECT t2.id_poliza FROM temp_hoy t2 
          LEFT JOIN temp_ayer t1 
          ON t1.id_poliza = t2.id_poliza
              WHERE
              t1.id_poliza IS NULL
            
    );


    truncate table `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`;

    insert into `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
    SELECT * FROM temp_hoy WHERE id_poliza in (select id_poliza from ids_nuevos)
    UNION ALL
    (
      SELECT * FROM temp_hoy WHERE id_poliza in (select id_poliza from ids_modif)
    )
    UNION ALL
    (
      SELECT * FROM temp_ayer WHERE id_poliza not in (select id_poliza from ids_modif union all select id_poliza from ids_nuevos)
    );

    UPDATE `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
    SET est_cambios = 'modificado'
    WHERE id_poliza in (select id_poliza from ids_modif);

    UPDATE `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
    SET est_cambios = 'nuevo'
    WHERE id_poliza in (select id_poliza from ids_nuevos);

ELSE
  --insert records from ANL poliza con el campo estado en "universo"
  INSERT INTO `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
SELECT DISTINCT id_poliza,
  id_contratante,
  nom_contratante,
  nom_persona_contratante,
  ape_paterno_contratante,
  ape_materno_contratante,
  des_sexo_contratante,
  fec_nacimiento_contratante,
  tip_persona_contratante,
  tip_documento_contratante,
  num_documento_contratante,
  des_distrito_contratante,
  des_provincia_contratante,
  des_departamento_contratante,
  id_subsegmento_contratante,
  des_subsegmento_contratante,
  id_segmento_contratante,
  des_segmento_contratante,
  id_grupo_economico_contratante,
  des_grupo_economico_contratante,
  des_empresa_sector_economico_contratante,
  id_producto,
  nom_producto,
  ind_fronting,
  num_poliza,
  id_poliza_origen,
  tip_suscripcion,
  id_poliza_referencia,
  mnt_prima_emitida_bruta_anualizada_poliza,
  fec_emision,
  fec_inicio_vigencia,
  fec_fin_vigencia,
  tip_vigencia_poliza,
  id_est_poliza_origen,
  des_est_poliza_origen,
  id_est_poliza,
  des_est_poliza,
  fec_anulacion,
  des_periodo_anulacion_poliza,
  fec_renovacion,
  id_motivo_anulacion_origen,
  id_motivo_anulacion,
  des_motivo_anulacion,
  id_canal,
  des_canal,
  id_subcanal,
  des_subcanal,
  des_subtipocanal,
  id_intermediario,
  nom_intermediario,
  id_corredor_nt,
  nom_corredor_nt,
  cod_producto_origen,
  frecuencia_pago,
  id_moneda,
  ind_gestionable,
  des_nivel_valor_prod,
  des_nivel_riesgo_prod,
  ind_renovacion_autom,
  id_compania,
  id_origen,
  ind_poliza_vigente,
  id_certificado,
  num_certificado_origen,
  id_estado_certificado_origen,
  id_certificado_origen,
  id_est_certificado,
  est_certificado,
  fec_inicio_vigencia_certificado,
  fec_fin_vigencia_certificado,
  fec_ingreso_certificado,
  fec_exclusion_certificado,
  id_motivo_exclusion_origen_certificado,
  id_motivo_exclusion_certificado,
  des_motivo_exclusion_certificado,
  id_via_cobro_certificado,
  des_via_cobro_certificado,
  tip_modalidad_cobro_certificado,
  fec_operacion_anulacion_certificado,
  des_periodo_operacion_anulacion,
  ind_certificado_vigente,
  ind_certificado_digital,
  mnt_prima_convenida,
  id_titular,
  nom_completo_titular,
  nom_persona_titular,
  ape_paterno_titular,
  ape_materno_titular,
  des_sexo_titular,
  fec_nacimiento_titular,
  tip_persona_titular,
  tip_documento_titular,
  des_distrito_titular,
  des_provincia_titular,
  des_departamento_titular,
  unidad_asegurable,
  endoso_cesion_derecho,
  periodo,
  fec_procesamiento,  
"nuevo" as est_cambios, 
date(null) as fec_insercion,
date(null) as fec_modificacion,
false as bq__soft_deleted
  FROM `{project_id_anl}.anl_produccion.poliza` 
  WHERE periodo = DATE(CONCAT(EXTRACT(YEAR FROM date_variable), '-', EXTRACT(MONTH FROM date_variable), '-', '01')); --order by id_poliza limit 1000000;

  UPDATE `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
  SET est_cambios = 'nuevo'
  WHERE true;

END IF;

DROP TABLE IF EXISTS `{project_id_anl}.delivery_onehealth.diferencial_{script_name}`;

CREATE OR REPLACE TABLE `{project_id_anl}.delivery_onehealth.diferencial_{script_name}`
AS
SELECT * FROM `{project_id_anl}.delivery_onehealth.ef_poliza_modificados`
WHERE est_cambios in ("modificado", "nuevo");

SET number_of_files = 
(SELECT
  CAST(TRUNC(SUM(size_bytes)/POW(10,9))+1 AS INT64) AS size
FROM
  `{project_id_anl}.delivery_onehealth.__TABLES__`
WHERE
  table_id = 'diferencial_{script_name}');


DROP TABLE IF EXISTS `{project_id_anl}.delivery_onehealth.export_data`;

CREATE TABLE `{project_id_anl}.delivery_onehealth.export_data`
PARTITION BY RANGE_BUCKET(export_id, GENERATE_ARRAY(0, number_of_files, 1))
AS (
    SELECT
        *,
        CAST(FLOOR(number_of_files*RAND()) AS INT64) AS export_id
    FROM `{project_id_anl}.delivery_onehealth.diferencial_{script_name}`
);

FOR item IN (SELECT idx FROM UNNEST(GENERATE_ARRAY(0, number_of_files, 1)) AS idx WHERE idx < number_of_files)
DO
    EXPORT DATA
        OPTIONS(
            uri=CONCAT('gs://{bucket_export}/mdm_{script_name}/mdm_{script_name}_{datetime}-', item.idx, '-*.parquet'),
            format='PARQUET',
            OVERWRITE=TRUE)
    AS
        SELECT * EXCEPT(export_id) FROM `{project_id_anl}.delivery_onehealth.export_data`
        WHERE export_id = item.idx
        LIMIT 9223372036854775807;
END FOR;


DROP TABLE IF EXISTS `{project_id_anl}.delivery_onehealth.diferencial_{script_name}`;
DROP TABLE IF EXISTS `{project_id_anl}.delivery_onehealth.export_data`;