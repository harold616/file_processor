# main.py - versión completa con BigQuery para my-cf-learning
import logging
import pandas as pd
from google.cloud import storage, bigquery
from io import StringIO
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuración BigQuery para my-cf-learning
PROJECT_ID = "my-cf-learning"
DATASET_ID = "file_processing"
TABLE_LOG = "archivo_log"
TABLE_RESULTADOS = "cliente_renta_resultado"

def save_to_bigquery_log(bq_client, archivo_nombre, bucket_origen, 
                       registros_totales, clientes_unicos, estado, error_mensaje=None):
   """Guarda log del procesamiento en BigQuery"""
   
   table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_LOG}"
   
   rows_to_insert = [{
       "archivo_nombre": archivo_nombre,
       "fecha_procesamiento": datetime.now().isoformat(),
       "bucket_origen": bucket_origen,
       "registros_totales": registros_totales,
       "clientes_unicos": clientes_unicos,
       "estado": estado,
       "error_mensaje": error_mensaje
   }]
   
   errors = bq_client.insert_rows_json(table_id, rows_to_insert)
   
   if errors:
       logger.error(f"❌ Error insertando log: {errors}")
       raise Exception(f"Error en BigQuery log: {errors}")
   else:
       logger.info(f"✅ Log guardado en BigQuery: {archivo_nombre}")

def save_results_to_bigquery(bq_client, archivo_nombre, resultado_df):
   """Guarda resultados en BigQuery"""
   
   table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_RESULTADOS}"
   
   # Agregar metadatos al DataFrame
   resultado_df = resultado_df.copy()
   resultado_df['archivo_procesado'] = archivo_nombre
   resultado_df['fecha_procesamiento'] = datetime.now()
   
   # Reordenar columnas
   resultado_df = resultado_df[['archivo_procesado', 'fecha_procesamiento', 
                               'id_cliente', 'periodo_max', 'renta_total']]
   
   # Subir a BigQuery
   job_config = bigquery.LoadJobConfig(
       write_disposition="WRITE_APPEND",
       schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
   )
   
   job = bq_client.load_table_from_dataframe(
       resultado_df, table_id, job_config=job_config
   )
   
   job.result()  # Esperar que complete
   
   logger.info(f"✅ {len(resultado_df)} registros guardados en BigQuery: {table_id}")

def process_client_renta_data(csv_content):
   """
   Procesa datos de clientes:
   1. Agrupa por id_cliente 
   2. Encuentra periodo máximo por cliente
   3. Suma renta de todos los registros con ese periodo máximo
   """
   
   # Leer CSV con delimiter ;
   df = pd.read_csv(StringIO(csv_content), sep=';', header=None)
   
   # Extraer solo las columnas que necesitamos
   df_clean = df[[1, 2, 12]].copy()  # periodo, renta, id_cliente
   df_clean.columns = ['periodo', 'renta', 'id_cliente']
   
   # Convertir a numérico
   df_clean['periodo'] = pd.to_numeric(df_clean['periodo'])
   df_clean['renta'] = pd.to_numeric(df_clean['renta'])
   df_clean['id_cliente'] = pd.to_numeric(df_clean['id_cliente'])
   
   logger.info(f"📊 Datos cargados: {len(df_clean)} registros")
   logger.info(f"👥 Clientes únicos: {df_clean['id_cliente'].nunique()}")
   
   # Paso 1: Encontrar periodo máximo por cada cliente
   max_periodos = df_clean.groupby('id_cliente')['periodo'].max().reset_index()
   max_periodos.columns = ['id_cliente', 'periodo_max']
   
   logger.info("🔍 Periodos máximos por cliente calculados")
   
   # Paso 2: Filtrar solo registros con periodo máximo
   df_max_periodo = df_clean.merge(
       max_periodos, 
       left_on=['id_cliente', 'periodo'], 
       right_on=['id_cliente', 'periodo_max']
   )
   
   # Paso 3: Sumar renta por cliente (múltiples registros del mismo periodo max)
   resultado = df_max_periodo.groupby('id_cliente').agg({
       'renta': 'sum',
       'periodo': 'first'  # Guardar el periodo (será el mismo para todos)
   }).reset_index()
   
   resultado.columns = ['id_cliente', 'renta_total', 'periodo_max']
   
   logger.info(f"✅ Resultado: {len(resultado)} clientes procesados")
   
   # Logging de ejemplo
   for _, row in resultado.head(3).iterrows():
       logger.info(f"Cliente {row['id_cliente']}: Periodo {row['periodo_max']}, Renta {row['renta_total']:,}")
   
   return resultado, len(df_clean), df_clean['id_cliente'].nunique()

def process_file(event, context):
   """Cloud Function expandida con BigQuery para procesar rentas por cliente"""
   
   bucket_name = event['bucket']
   file_name = event['name']
   
   logger.info(f"📄 Procesando archivo de rentas: {file_name}")
   
   if not file_name.endswith('.csv'):
       return {"error": "No es archivo CSV"}
   
   # Inicializar clientes
   storage_client = storage.Client()
   bq_client = bigquery.Client()
   
   try:
       # 1. Extraer datos de Cloud Storage
       bucket = storage_client.bucket(bucket_name)
       blob = bucket.blob(file_name)
       csv_content = blob.download_as_text()
       
       logger.info(f"📥 Archivo descargado: {len(csv_content)} caracteres")
       
       # 2. Procesar rentas por cliente
       resultado, registros_totales, clientes_unicos = process_client_renta_data(csv_content)
       
       # 3. Guardar log en BigQuery
       save_to_bigquery_log(
           bq_client, file_name, bucket_name, 
           registros_totales, clientes_unicos, "SUCCESS"
       )
       
       # 4. Guardar resultados en BigQuery
       save_results_to_bigquery(bq_client, file_name, resultado)
       
       # 5. Respuesta exitosa
       resultado_dict = {
           'total_clientes': len(resultado),
           'archivo_procesado': file_name,
           'registros_totales': registros_totales,
           'clientes_unicos': clientes_unicos,
           'guardado_en_bigquery': True,
           'proyecto': PROJECT_ID,
           'dataset': DATASET_ID,
           'tablas': {
               'logs': f"{PROJECT_ID}.{DATASET_ID}.{TABLE_LOG}",
               'resultados': f"{PROJECT_ID}.{DATASET_ID}.{TABLE_RESULTADOS}"
           },
           'muestra_clientes': resultado.head(5).to_dict('records')
       }
       
       logger.info(f"🎯 Procesamiento completo guardado en BigQuery")
       return resultado_dict
       
   except Exception as e:
       logger.error(f"❌ Error procesando archivo {file_name}: {e}")
       
       # Guardar log de error en BigQuery
       try:
           save_to_bigquery_log(
               bq_client, file_name, bucket_name, 
               0, 0, "ERROR", str(e)
           )
           logger.info("📝 Log de error guardado en BigQuery")
       except Exception as log_error:
           logger.error(f"❌ Error guardando log de error: {log_error}")
       
       return {
           "error": str(e),
           "archivo": file_name,
           "bucket": bucket_name
       }