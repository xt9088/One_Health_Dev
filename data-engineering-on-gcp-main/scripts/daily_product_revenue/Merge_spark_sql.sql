    MERGE INTO silver.orders AS a
    USING bronze.orders AS b
    ON a.id = b.id
    WHEN MATCHED THEN
      UPDATE SET a.local = b.local
    WHEN NOT MATCHED THEN
      INSERT (id, local) VALUES (b.id, b.local)

---big--query
MERGE INTO `silver.orders` AS a
USING `bronze.orders` AS b
ON a.id = b.id
WHEN MATCHED THEN
  UPDATE SET a.local = b.local
WHEN NOT MATCHED THEN
  INSERT (id, local) VALUES (b.id, b.local)

  ----SINIESTROS--- 

MERGE INTO `he-dev-composer.dev_data_analytics.anl_tmp_part_month_siniestro_t2` AS a
USING `he-dev-composer.dev_data_analytics.anl_tmp_part_month_siniestro_t` AS b
ON a.id_siniestro = b.id_siniestro
WHEN MATCHED THEN
  UPDATE SET a.des_ubicacion_detallada = b.des_ubicacion_detallada
WHEN NOT MATCHED THEN
  INSERT (id_siniestro, id_origen, id_siniestro_origen, id_poliza, id_contratante, id_producto, id_certificado, id_titular, fec_hora_ocurrencia, fec_notificacion, num_siniestro, fec_operacion, id_estado_siniestro_origen, des_estado_siniestro_origen, id_estado_siniestro, des_estado_siniestro, fec_estado, id_ubicacion_geografica, des_departamento_siniestro, des_provincia_siniestro, des_distrito_siniestro, des_ubicacion_detallada, id_unidad_asegurable, id_asegurado, des_unidad_asegurable, sexo_asegurable, fec_nacimiento_asegurable, num_documento_asegurable, tip_documento_asegurable, rango_etareo_asegurable, fec_constitucion_siniestro, fec_anulacion, fec_ult_liquidacion, mto_total_reserva, mto_total_reserva_usd, mto_total_reserva_ajustador, mto_total_reserva_usd_ajustador, mto_total_aprobado, mto_total_aprobado_usd, mto_aprobado_deduci, mto_aprobado_deduci_usd, mto_total_facturado, mto_total_facturado_usd, mto_total_pendiente, mto_total_pendiente_usd, mto_total_pagado, mto_total_pagado_usd, mto_total_pagado_ajustador, mto_total_pagado_usd_ajustador, mto_pagado_deduci, mto_pagado_deduci_usd, mto_potencial_usd, id_oficina_reclamo_origen, id_oficina_reclamo, des_oficina_reclamo, id_motivo_estado_origen, des_motivo_estado_origen, id_motivo_estado, des_motivo_estado, id_moneda, id_compania, tipo_flujo, porcentaje_coasegurador_rimac, mto_reserva_np, mto_reserva_usd_np, mto_aprobado_np, mto_aprobado_usd_np, mto_pagado_np, mto_pagado_usd_np, tip_atencion_siniestro, tip_procedencia, fec_insercion, fec_modificacion, bq__soft_deleted, partition_date)
  VALUES (b.id_siniestro, b.id_origen, b.id_siniestro_origen, b.id_poliza, b.id_contratante, b.id_producto, b.id_certificado, b.id_titular, b.fec_hora_ocurrencia, b.fec_notificacion, b.num_siniestro, b.fec_operacion, b.id_estado_siniestro_origen, b.des_estado_siniestro_origen, b.id_estado_siniestro, b.des_estado_siniestro, b.fec_estado, b.id_ubicacion_geografica, b.des_departamento_siniestro, b.des_provincia_siniestro, b.des_distrito_siniestro, b.des_ubicacion_detallada, b.id_unidad_asegurable, b.id_asegurado, b.des_unidad_asegurable, b.sexo_asegurable, b.fec_nacimiento_asegurable, b.num_documento_asegurable, b.tip_documento_asegurable, b.rango_etareo_asegurable, b.fec_constitucion_siniestro, b.fec_anulacion, b.fec_ult_liquidacion, b.mto_total_reserva, b.mto_total_reserva_usd, b.mto_total_reserva_ajustador, b.mto_total_reserva_usd_ajustador, b.mto_total_aprobado, b.mto_total_aprobado_usd, b.mto_aprobado_deduci, b.mto_aprobado_deduci_usd, b.mto_total_facturado, b.mto_total_facturado_usd, b.mto_total_pendiente, b.mto_total_pendiente_usd, b.mto_total_pagado, b.mto_total_pagado_usd, b.mto_total_pagado_ajustador, b.mto_total_pagado_usd_ajustador, b.mto_pagado_deduci, b.mto_pagado_deduci_usd, b.mto_potencial_usd, b.id_oficina_reclamo_origen, b.id_oficina_reclamo, b.des_oficina_reclamo, b.id_motivo_estado_origen, b.des_motivo_estado_origen, b.id_motivo_estado, b.des_motivo_estado, b.id_moneda, b.id_compania, b.tipo_flujo, b.porcentaje_coasegurador_rimac, b.mto_reserva_np, b.mto_reserva_usd_np, b.mto_aprobado_np, b.mto_aprobado_usd_np, b.mto_pagado_np, b.mto_pagado_usd_np, b.tip_atencion_siniestro, b.tip_procedencia, b.fec_insercion, b.fec_modificacion, b.bq__soft_deleted, b.partition_date);
