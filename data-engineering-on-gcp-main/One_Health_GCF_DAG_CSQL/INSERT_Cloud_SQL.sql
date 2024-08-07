----CONFIGURACION DE PARAMETROS EN CLOUD_SQL PARA LA TABLA SINIESTROS -----

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
    DAG_STORAGE_DSET_NAME,
    SQL_SCRIPT,
    DESTINATION_DSET_UNIVERSAL,
    DESTINATION_TABLE_UNIVERSAL,
    ARCHIVE_DIRECTORY,
    DESTINATION_BUCKET_HIST
) VALUES (
    'd10532fa-3222-47c9-a5b4-faf62b842a11',
    'he-dev-data',
    '338c2e50-1bc2-4b13-9145-95190a55e53d',
    'd490efdc-0448-468d-97e4-749de76399e3',
    'Ingesta Siniestros',
    'Trae la informacion de Rimac hacia Compass',
    'RIMAC',
    'ARCHIVO',
    'PARQUET',
    '.parquet',
    'File',
    'he-dev-data',
    'he-dev-data-ipress/ipress_clinicas/internacional/siniestro/',
    'siniestro',
    'he-dev-data',
    'dev_data_landing',
    'iafas_mdm_siniestro',
    'dev_airflow_data',
    'dag_compass_gcs_to_bq',
    'CREATE TABLE IF NOT EXISTS he-dev-data.dev_data_landing.iafas_mdm_siniestro ( id_siniestro STRING, id_siniestro_origen STRING, id_poliza STRING, id_contratante STRING, id_producto STRING, id_certificado STRING, id_titular STRING, fec_hora_ocurrencia TIMESTAMP, fec_notificacion TIMESTAMP, num_siniestro STRING, fec_operacion TIMESTAMP, id_estado_siniestro_origen STRING, des_estado_siniestro_origen STRING, des_estado_siniestro STRING, id_ubicacion_geografica STRING, des_departamento_siniestro STRING, des_provincia_siniestro STRING, des_distrito_siniestro STRING, des_ubicacion_detallada STRING, fec_ult_liquidacion TIMESTAMP, mto_pagado_deduci NUMERIC, mto_pagado_deduci_usd NUMERIC, id_moneda STRING, tip_procedencia STRING, tip_ingreso_siniestro NUMERIC, tip_reclamo STRING, des_tipo_reclamo STRING, num_obligacion STRING, ind_tedef_salud STRING, mnt_gasto_total_con_igv NUMERIC, nro_nota_credito STRING, mnt_nota_credito NUMERIC, mnt_planilla_afiliado_usd NUMERIC, mnt_planilla_afiliado_sol NUMERIC, mnt_observado NUMERIC, id_persona_proveedor_siniestro STRING, tip_documento_proveedor_siniestro STRING, num_documento_proveedor_siniestro STRING, nom_completo_proveedor_siniestro STRING, dir_proveedor_siniestro STRING, cod_sede_proveedor_siniestro STRING, nom_sede_proveedor_siniestro STRING, dir_sede_proveedor_siniestro STRING, id_persona_afiliado STRING, tip_documento_afiliado STRING, num_documento_afiliado STRING, nom_completo_afiliado STRING, fec_nacimiento_afiliado DATE, edad_afiliado NUMERIC, edad_afiliado_ocurrencia NUMERIC, des_sexo_afiliado STRING, periodo DATE, est_cambios STRING, fec_insercion DATE, fec_modificacion DATE, bq__soft_deleted BOOLEAN ) PARTITION BY periodo CLUSTER BY id_siniestro;',
    'dev_data_analytics',
    'anl_mdm_universal_siniestro',
    'he-dev-data-ipress/ipress_clinicas/internacional/Inventario_siniestros/',
    'he-dev-data-historicos'
);

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
    DAG_STORAGE_DSET_NAME,
    SQL_SCRIPT,
    DESTINATION_DSET_UNIVERSAL,
    DESTINATION_TABLE_UNIVERSAL,
    ARCHIVE_DIRECTORY,
    DESTINATION_BUCKET_HIST
) VALUES (
    '09023b59-d78f-415c-862b-1751c732bc84',
    'he-dev-data',
    '338c2e50-1bc2-4b13-9145-95190a55e53d',
    'd490efdc-0448-468d-97e4-749de76399e3',
    'Ingesta Persona',
    'Trae la informacion de Rimac hacia Compass',
    'RIMAC',
    'ARCHIVO',
    'PARQUET',
    '.parquet',
    'File',
    'he-dev-data',
    'he-dev-data-ipress/ipress_clinicas/internacional/persona/',
    'persona',
    'he-dev-data',
    'dev_data_landing',
    'iafas_mdm_persona',
    'dev_airflow_data',
    'dag_compass_gcs_to_bq',
    'CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_persona` (
    id_cliente_persona STRING,
    cuc STRING,
    tip_documento STRING,
    num_documento STRING,
    cod_acselx STRING,
    ape_paterno STRING,
    ape_materno STRING,
    nombres STRING,
    nombre_completo STRING,
    fec_nacimiento DATE,
    num_edad INT64,
    des_rango_edad STRING,
    des_generacion STRING,
    des_sexo STRING,
    des_estado_civil STRING,
    des_nacionalidad STRING,
    des_pais_origen STRING,
    des_grado_instruccion STRING,
    nom_grado_academico STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    especialidad STRING,
                    fec_acreditacion STRING
                >
            >
        >
    >,
    ind_fallecido STRING,
    nse_rimac STRING,
    nse_agrup STRING,
    des_segmentacion_growth STRING,
    des_sub_segmentacion_growth STRING,
    des_rango_linea_max_tcred STRING,
    des_rango_deuda_total_tcred STRING,
    cnt_entidad_sbs INT64,
    des_rango_saldo_sbs STRING,
    est_deudor_rcc STRING,
    ind_critico STRING,
    ind_yellowlist STRING,
    ind_watchlist STRING,
    ind_blacklist STRING,
    ind_datoscontacto_sinvalidar STRING,
    departamento_gestion_servicio STRING,
    provincia_gestion_servicio STRING,
    distrito_gestion_servicio STRING,
    direccion_gestion_servicio STRING,
    num_ubigeo_gestion_servicio STRING,
    des_lima_prov STRING,
    ind_empleado_rimac STRING,
    ind_empleado_bbva STRING,
    ind_empleado_breca STRING,
    ind_flag_bbva STRING,
    ind_cliente_cnt STRING,
    ind_cliente_corredor_no_soat STRING,
    ind_cliente_corredor_gerencia_riesgo STRING,
    ind_cliente_corredor_marsh STRING,
    ind_consentimiento_comercial STRING,
    ind_ley_datos_personales STRING,
    ext_ind_dependiente_laboral INT64,
    ext_id_empresa_laboral STRING,
    ext_ruc_empresa_laboral STRING,
    ext_des_empresa_laboral STRING,
    ext_des_score_ingresos STRING,
    ind_tenencia_hijos INT64,
    ext_es_bancarizado INT64,
    ext_des_calificacion_rcc STRING,
    ext_des_banco_principal STRING,
    ext_des_entidad_principal STRING,
    ext_entidad_max_linea_tc STRING,
    ext_ind_tiene_tarjeta_credito INT64,
    ext_ind_tiene_prestamo_personal INT64,
    ext_ind_tiene_prestamo_vehicular INT64,
    ext_ind_tiene_prestamo_hipotecario INT64,
    ext_ind_rcc_negativo STRING,
    num_antiguedad_cliente INT64,
    num_polizas_vig INT64,
    num_certificados_vig INT64,
    num_productos INT64,
    num_riesgos INT64,
    num_canales INT64,
    num_corredores INT64,
    num_intermediarios INT64,
    des_combo_productos STRING,
    des_combo_riesgos STRING,
    des_combo_canales STRING,
    mnt_prima_cliente_usd NUMERIC,
    mnt_prima_promedio_cliente_usd NUMERIC,
    ind_gestionable STRING,
    ind_rentable STRING,
    ind_alto_valor STRING,
    ind_cliente_digital STRING,
    ind_titular_alto_valor STRING,
    ind_royal STRING,
    ind_tiene_celular_comercial STRING,
    ind_tiene_email_comercial STRING,
    des_profesion STRING,
    datos_laft STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    id_persona STRING,
                    ind_inv STRING,
                    ind_pep STRING,
                    ind_ref STRING,
                    ind_bloq STRING,
                    ind_ong STRING,
                    ind_ofac STRING,
                    ind_obligado_declarar STRING,
                    est_investigado STRING,
                    ind_no_domiciliado STRING,
                    des_calificacion_rcc_general STRING,
                    datos_investigado STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    fec_creacion DATE,
                                    des_observacion STRING,
                                    des_usuario STRING
                                >
                            >
                        >
                    >,
                    datos_pep STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    fec_creacion DATE,
                                    tip_entidad STRING,
                                    des_entidad STRING,
                                    des_cargo STRING,
                                    des_relacion STRING,
                                    des_resolucion STRING
                                >
                            >
                        >
                    >,
                    datos_reforzado STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    fec_creacion DATE,
                                    des_observacion STRING,
                                    tip_alerta STRING,
                                    des_motivo_anul STRING
                                >
                            >
                        >
                    >,
                    datos_bloqueo STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    des_observacion STRING,
                                    cod_vinculo STRING,
                                    des_vinculo STRING,
                                    cod_bloqueo STRING,
                                    des_bloqueo STRING,
                                    fec_ini_bloqueo DATE,
                                    fec_fin_bloqueo DATE
                                >
                            >
                        >
                    >,
                    datos_ong STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    fec_creacion DATE,
                                    des_usuario STRING,
                                    des_origen STRING,
                                    cod_tipo_inst STRING,
                                    tip_inst STRING,
                                    cod_tipo_cont STRING,
                                    tip_cont STRING
                                >
                            >
                        >
                    >,
                    datos_ofac STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_estado STRING,
                                    fec_creacion DATE,
                                    des_observacion STRING,
                                    des_usuario STRING,
                                    des_tipo STRING,
                                    des_origen STRING,
                                    des_nombre STRING,
                                    cod_ofac STRING,
                                    id_proceso STRING,
                                    nom_cent_trabajo STRING
                                >
                            >
                        >
                    >
                >
            >
        >
    >,
    ind_pais_sin_riesgo INT64,
    ind_pais_riesgo_no_cooperante INT64,
    ind_pais_riesgo_paraiso_fiscal INT64,
    rep_legal STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    des_cargo STRING,
                    fec_inicio_cargo DATE,
                    periodo DATE,
                    num_ruc INT64,
                    nom_empresa STRING,
                    des_estado_contribuyente STRING,
                    fec_fin_baja DATE
                >
            >
        >
    >,
    identificador_sistema STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    id_tercero_sistema STRING,
                    id_origen STRING
                >
            >
        >
    >,
    ind_riesgo_salud STRING,
    ind_riesgo_patrimonial STRING,
    ind_riesgo_vida STRING,
    ind_cliente_gestionable STRING,
    ind_presenta_productos_masivos STRING,
    ind_presenta_productos_no_masivos STRING,
    ind_email_enviado STRING,
    cnt_correo_enviado NUMERIC,
    periodo DATE,
    fec_procesamiento DATE
)
;',
    'dev_data_analytics',
    'anl_mdm_universal_persona',
    'he-dev-data-ipress/ipress_clinicas/internacional/Inventario_persona/',
    'he-dev-data-historicos'
);

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
    DAG_STORAGE_DSET_NAME,
    SQL_SCRIPT,
    DESTINATION_DSET_UNIVERSAL,
    DESTINATION_TABLE_UNIVERSAL,
    ARCHIVE_DIRECTORY,
    DESTINATION_BUCKET_HIST
) VALUES (
    'b92f65cb-3e7f-47b0-bd6d-e43b2b9c83c2',
    'he-dev-data',
    '338c2e50-1bc2-4b13-9145-95190a55e53d',
    'd490efdc-0448-468d-97e4-749de76399e3',
    'Ingesta Poliza',
    'Trae la informacion de Rimac hacia Compass',
    'RIMAC',
    'ARCHIVO',
    'PARQUET',
    '.parquet',
    'File',
    'he-dev-data',
    'he-dev-data-ipress/ipress_clinicas/internacional/poliza/',
    'poliza',
    'he-dev-data',
    'dev_data_landing',
    'iafas_mdm_poliza',
    'dev_airflow_data',
    'dag_compass_gcs_to_bq',
    'CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_poliza` (
  id_poliza STRING,
  id_contratante STRING,
  nom_contratante STRING,
  nom_persona_contratante STRING,
  ape_paterno_contratante STRING,
  ape_materno_contratante STRING,
  des_sexo_contratante STRING,
  fec_nacimiento_contratante DATE,
  tip_persona_contratante STRING,
  tip_documento_contratante STRING,
  num_documento_contratante STRING,
  des_distrito_contratante STRING,
  des_provincia_contratante STRING,
  des_departamento_contratante STRING,
  id_subsegmento_contratante INT64,
  des_subsegmento_contratante STRING,
  id_segmento_contratante INT64,
  des_segmento_contratante STRING,
  id_grupo_economico_contratante INT64,
  des_grupo_economico_contratante STRING,
  des_empresa_sector_economico_contratante STRING,
  id_producto STRING,
  nom_producto STRING,
  ind_fronting STRING,
  num_poliza STRING,
  id_poliza_origen STRING,
  tip_suscripcion STRING,
  id_poliza_referencia STRING,
  mnt_prima_emitida_bruta_anualizada_poliza NUMERIC,
  fec_emision DATE,
  fec_inicio_vigencia DATE,
  fec_fin_vigencia DATE,
  tip_vigencia_poliza STRING,
  id_est_poliza_origen STRING,
  des_est_poliza_origen STRING,
  id_est_poliza STRING,
  des_est_poliza STRING,
  fec_anulacion DATE,
  des_periodo_anulacion_poliza STRING,
  fec_renovacion DATE,
  id_motivo_anulacion_origen STRING,
  id_motivo_anulacion STRING,
  des_motivo_anulacion STRING,
  id_canal INT64,
  des_canal STRING,
  id_subcanal INT64,
  des_subcanal STRING,
  des_subtipocanal STRING,
  id_intermediario INT64,
  nom_intermediario STRING,
  id_corredor_nt INT64,
  nom_corredor_nt STRING,
  cod_producto_origen STRING,
  frecuencia_pago STRING,
  id_moneda STRING,
  ind_gestionable STRING,
  des_nivel_valor_prod STRING,
  des_nivel_riesgo_prod STRING,
  ind_renovacion_autom STRING,
  id_compania STRING,
  id_origen STRING,
  ind_poliza_vigente STRING,
  id_certificado STRING,
  num_certificado_origen INT64,
  id_estado_certificado_origen STRING,
  id_certificado_origen STRING,
  id_est_certificado STRING,
  est_certificado STRING,
  fec_inicio_vigencia_certificado DATE,
  fec_fin_vigencia_certificado DATE,
  fec_ingreso_certificado DATE,
  fec_exclusion_certificado DATE,
  id_motivo_exclusion_origen_certificado STRING,
  id_motivo_exclusion_certificado STRING,
  des_motivo_exclusion_certificado STRING,
  id_via_cobro_certificado STRING,
  des_via_cobro_certificado STRING,
  tip_modalidad_cobro_certificado STRING,
  fec_operacion_anulacion_certificado DATE,
  des_periodo_operacion_anulacion STRING,
  ind_certificado_vigente STRING,
  ind_certificado_digital STRING,
  mnt_prima_convenida NUMERIC,
  id_titular STRING,
  nom_completo_titular STRING,
  nom_persona_titular STRING,
  ape_paterno_titular STRING,
  ape_materno_titular STRING,
  des_sexo_titular STRING,
  fec_nacimiento_titular DATE,
  tip_persona_titular STRING,
  tip_documento_titular STRING,
  des_distrito_titular STRING,
  des_provincia_titular STRING,
  des_departamento_titular STRING,
  unidad_asegurable STRUCT<
    list ARRAY<STRUCT<
      element STRUCT<
        id_unidad_asegurable STRING,
        tip_unidad_asegurable STRING,
        fec_inicio_vigencia DATE,
        fec_fin_vigencia DATE,
        fec_anulacion DATE,
        id_tipo_exclusion STRING,
        des_motivo_exclusion STRING,
        id_est_origen_unidad_asegurable STRING,
        des_est_origen_unidad_asegurable STRING,
        id_est_unidad_asegurable STRING,
        des_est_unidad_asegurable STRING,
        fec_creacion DATE,
        ind_titular STRING,
        id_persona STRING,
        id_parentesco STRING,
        des_parentesco STRING,
        id_origen STRING,
        id_inmueble INT64,
        id_vehiculo INT64,
        ramo_unidad_asegurable STRUCT<
          list ARRAY<STRUCT<
            element STRUCT<
              id_ramo_unidad_asegurable STRING,
              id_ramo STRING,
              id_plan STRING,
              id_version_plan STRING,
              des_tip_version_plan STRING,
              des_sub_tipo_version_plan STRING,
              tip_plan_credito STRING,
              ind_responsabilidad_civil STRING,
              version_plan_agrupador_n1 STRING,
              version_plan_agrupador_n2 STRING,
              version_plan_agrupador_n3 STRING,
              id_origen STRING,
              cobertura_unidad_asegurable STRUCT<
                list ARRAY<STRUCT<
                  element STRUCT<
                    id_cobertura_unidad_asegurable STRING,
                    id_cobertura STRING,
                    mnt_suma_asegurada_usd NUMERIC,
                    ind_principal STRING
                  >
                >>
              >
            >
          >>
        >
      >
    >>
  >,
  endoso_cesion_derecho STRUCT<
    list ARRAY<STRUCT<
      element STRUCT<
        id_endosatario_certificado STRING,
        id_persona_endoso STRING,
        nom_persona_endoso STRING,
        id_persona_endosatario STRING,
        nom_persona_endosatario STRING,
        fec_inicio_vigencia_endoso DATE,
        fec_fin_vigencia_endoso DATE,
        cod_estado_endoso_origen STRING,
        des_estado_endoso_origen STRING,
        cod_estado_endoso STRING,
        des_estado_endoso STRING,
        por_participacion_endoso NUMERIC,
        id_moneda_endoso STRING,
        mnt_endoso NUMERIC,
        fec_ingreso_endoso DATE,
        des_texto_bien_endoso STRING,
        fec_exclusion_endoso DATE,
        des_motivo_exclusion_origen STRING
      >
    >>
  >,
  periodo DATE,
  fec_procesamiento DATE
);
',
    'dev_data_analytics',
    'anl_mdm_universal_poliza',
    'he-dev-data-ipress/ipress_clinicas/internacional/Inventario_poliza/',
    'he-dev-data-historicos'
);

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
    DAG_STORAGE_DSET_NAME,
    SQL_SCRIPT,
    DESTINATION_DSET_UNIVERSAL,
    DESTINATION_TABLE_UNIVERSAL,
    ARCHIVE_DIRECTORY,
    DESTINATION_BUCKET_HIST
) VALUES (
    '2ffd5dc9-5bfe-471c-9647-d3dd2ffda66e',
    'he-dev-data',
    '338c2e50-1bc2-4b13-9145-95190a55e53d',
    'd490efdc-0448-468d-97e4-749de76399e3',
    'Ingesta Cartas Garantia',
    'Trae la informacion de Rimac hacia Compass',
    'RIMAC',
    'ARCHIVO',
    'PARQUET',
    '.parquet',
    'File',
    'he-dev-data',
    'he-dev-data-ipress/ipress_clinicas/internacional/carta_garantia/',
    'carta_garantia',
    'he-dev-data',
    'dev_data_landing',
    'iafas_mdm_carta_garantia',
    'dev_airflow_data',
    'dag_compass_gcs_to_bq',
    'CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_carta_garantia` (
    id_carta_garantia STRING,
    id_origen STRING,
    num_carta_garantia_origen STRING,
    anio_carta_garantia INT64,
    num_carta_garantia INT64,
    id_actividad STRING,
    id_siniestro STRING,
    fec_emision DATE,
    mnt_carta_garantia_total FLOAT64,
    des_est_carta_garantia_homologado STRING,
    des_est_carta_garantia_origen STRING,
    id_persona_proveedor STRING,
    tip_documento_proveedor STRING,
    num_documento_proveedor STRING,
    des_razon_social_proveedor STRING,
    nom_comercial_proveedor STRING,
    id_sede_proveedor STRING,
    nom_sede_proveedor STRING,
    des_departamento_sede_proveedor STRING,
    des_provincia_sede_proveedor STRING,
    des_distrito_sede_proveedor STRING,
    id_persona_contratante STRING,
    tip_documento_contratante STRING,
    num_documento_contratante STRING,
    nom_completo_contratante STRING,
    cod_grupo_renovacion_salud STRING,
    des_grupo_renovacion_salud STRING,
    id_persona_afiliado STRING,
    tip_documento_afiliado STRING,
    num_documento_afiliado STRING,
    nom_completo_afiliado STRING,
    edad_afiliado NUMERIC,
    id_persona_afiliado_titular STRING,
    tip_documento_titular STRING,
    num_documento_titular STRING,
    nom_completo_titular STRING,
    cod_cobertura STRING,
    nom_cobertura STRING,
    agrupacion_cobertura STRING,
    id_producto STRING,
    des_producto STRING,
    des_producto_agrupado STRING,
    des_diagnostico STRING,
    des_tip_capa STRING,
    num_diagnotico_origen STRING,
    ind_covid STRING,
    id_moneda STRING,
    id_plan STRING,
    num_alternativa_copago INT64,
    num_alternativa_copago_forzada INT64,
    carta_garantia_version STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    ind_solicitud_aprobada STRING,
                    num_version INT64,
                    fec_aprobacion DATE,
                    des_est_version STRING,
                    mnt_autorizado_con_igv FLOAT64,
                    id_origen STRING,
                    des_tipo_procedimiento STRING,
                    num_procedimiento_origen STRING,
                    des_procedimiento STRING,
                    id_poliza STRING,
                    num_solicitud_origen STRING,
                    num_solicitud STRING,
                    des_tipo_solicitud STRING,
                    fec_solicitud DATE,
                    fec_atencion DATE,
                    des_motivo_rechazo STRING,
                    des_observacion_solicitud STRING,
                    num_colegio_medico_solicitud STRING,
                    nom_medico_solicitud STRING,
                    especialidades_medico_solicitud STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_especialidad STRING
                                >
                            >
                        >
                    >,
                    mnt_solicitado NUMERIC,
                    mnt_igv_solicitado NUMERIC,
                    mnt_autorizado NUMERIC,
                    mnt_igv_autorizado NUMERIC,
                    por_cobertura_carta NUMERIC,
                    des_subtipo_solicitud STRING,
                    des_est_solicitud_homologado STRING,
                    des_est_solicitud_origen STRING
                >
            >
        >
    >,
    carta_garantia_auditoria_concurrente STRUCT<
        list ARRAY<
            STRUCT<
                element STRUCT<
                    id_carta_garantia_auditoria STRING,
                    num_colegio_medico_auditoria STRING,
                    nom_medico_auditoria STRING,
                    especialidades_medico_auditoria STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_especialidad STRING
                                >
                            >
                        >
                    >,
                    fec_ingreso DATE,
                    fec_alta DATE,
                    fec_auditoria DATE,
                    des_tipo_hospitalizacion STRING,
                    ind_ficha_observada STRING,
                    mnt_carta_auditada NUMERIC,
                    mnt_preliquidado NUMERIC,
                    mnt_recuperado NUMERIC,
                    des_est_auditoria STRING,
                    des_est_auditoria_origen STRING,
                    observacion_detalle STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    des_observacion STRING,
                                    fec_registro_observacion DATE
                                >
                            >
                        >
                    >,
                    diagnostico_detalle STRUCT<
                        list ARRAY<
                            STRUCT<
                                element STRUCT<
                                    num_diagnotico_origen_auditoria STRING,
                                    des_diagnostico_auditoria STRING,
                                    des_tip_capa_auditoria STRING,
                                    fec_registro_diagnostico_auditoria DATE
                                >
                            >
                        >
                    >
                >
            >
        >
    >,
    periodo DATE
);
',
    'dev_data_analytics',
    'anl_mdm_universal_carta_garantia',
    'he-dev-data-ipress/ipress_clinicas/internacional/Inventario_carta_garantia/',
    'he-dev-data-historicos'
);

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
    DAG_STORAGE_DSET_NAME,
    SQL_SCRIPT,
    DESTINATION_DSET_UNIVERSAL,
    DESTINATION_TABLE_UNIVERSAL,
    ARCHIVE_DIRECTORY,
    DESTINATION_BUCKET_HIST
) VALUES (
    '8c685521-c77e-4f7e-a2ec-88f45a5b9f56',
    'he-dev-data',
    '338c2e50-1bc2-4b13-9145-95190a55e53d',
    'd490efdc-0448-468d-97e4-749de76399e3',
    'Ingesta prima inducida',
    'Trae la informacion de Rimac hacia Compass',
    'RIMAC',
    'ARCHIVO',
    'PARQUET',
    '.parquet',
    'File',
    'he-dev-data',
    'he-dev-data-ipress/ipress_clinicas/internacional/prima_inducida/',
    'prima_inducida',
    'he-dev-data',
    'dev_data_landing',
    'iafas_mdm_prima_inducida',
    'dev_airflow_data',
    'dag_compass_gcs_to_bq',
    'CREATE TABLE IF NOT EXISTS `he-dev-data.dev_data_landing.iafas_mdm_prima_inducida`
     PARTITION BY periodo CLUSTER BY id_prima_inducida',
    'dev_data_analytics',
    'anl_mdm_universal_prima_inducida',
    'he-dev-data-ipress/ipress_clinicas/internacional/Inventario_prima_inducida/',
    'he-dev-data-historicos'
);

