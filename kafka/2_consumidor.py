# -*- coding: utf-8 -*-
"""
2_consumidor.py - Consumidor Spark Structured Streaming de reseñas
==================================================================

Adaptación del laboratorio de sensores al caso gastronómico. Lee el flujo de
reseñas que `1_producer.py` publica en el topic `reviews-nuevas` y lo procesa con
Spark Structured Streaming en micro-batches.

CORRECCIÓN IMPORTANTE vs. el lab original
-----------------------------------------
El streaming NO carga los 3.4M de registros de golpe: procesa por micro-batches
incrementales y, con `startingOffsets=latest`, solo lee lo NUEVO. El problema del
lab era el sink `format("memory")`, que acumula TODO en la RAM del driver (solo
sirve para demos diminutas). Aquí se usan dos sinks correctos:

  * `memory`  -> SOLO para inspección rápida en notebook (con un cap mental).
  * `delta`   -> sink persistente y escalable con checkpoint (producción / Bronze).

Nada de esto "mapea el dataset completo": Kafka es el tubo de lo nuevo, no el
almacén de los 3.4M (esos viven en el data lake / capa Medallion).
"""

import os

# Carga .env (de la raíz del proyecto) si python-dotenv está instalado.
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
except ImportError:
    pass

import pyspark
print("PySpark:", pyspark.__version__)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
)

# -----------------------------------------------------------------------------
# 0) Conector Kafka (debe coincidir con la versión de Spark/Scala)
# -----------------------------------------------------------------------------
# Versiones overridables por entorno (defaults técnicos, no son credenciales).
SPARK_VERSION = os.getenv("SPARK_VERSION", "4.0.2")
SCALA_VERSION = os.getenv("SCALA_VERSION", "2.13")
KAFKA_PKG = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {KAFKA_PKG} pyspark-shell"

spark = (
    SparkSession.builder
    .appName("ReviewsKafkaStreaming")
    .config("spark.jars.packages", KAFKA_PKG)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("SparkSession creada. Versión:", spark.version)

# -----------------------------------------------------------------------------
# 1) Conexión al cluster (TODO vía entorno / .env — nada hardcodeado)
# -----------------------------------------------------------------------------
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
TOPIC = os.getenv("KAFKA_TOPIC")
CA_PATH = os.getenv("KAFKA_CA")

_faltantes = [n for n, v in {
    "KAFKA_HOST": KAFKA_HOST, "KAFKA_USER": KAFKA_USER, "KAFKA_PASSWORD": KAFKA_PASSWORD,
    "KAFKA_TOPIC": TOPIC, "KAFKA_CA": CA_PATH,
}.items() if not v]
if _faltantes:
    raise RuntimeError(f"Faltan variables de entorno: {', '.join(_faltantes)}. "
                       f"Defínelas en el .env de la raíz (ver .env.example).")

jaas_config = (
    f'org.apache.kafka.common.security.scram.ScramLoginModule required '
    f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";'
)

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_HOST)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")   # solo reseñas NUEVAS; "earliest" para reprocesar el topic
    .option("maxOffsetsPerTrigger", 5000)  # cota de tamaño por micro-batch (back-pressure)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.truststore.type", "PEM")
    .option("kafka.ssl.truststore.location", CA_PATH)
    .load()
)

# -----------------------------------------------------------------------------
# 2) Esquema de la reseña (debe coincidir con lo que emite 1_producer.py)
# -----------------------------------------------------------------------------
esquema = StructType([
    StructField("place_id",       StringType()),
    StructField("id_review",      StringType()),
    StructField("caption",        StringType()),
    StructField("relative_date",  StringType()),
    StructField("review_date",    StringType()),
    StructField("retrieval_date", StringType()),
    StructField("rating",         DoubleType()),
    StructField("username",       StringType()),
    StructField("n_review_user",  DoubleType()),
    StructField("n_photo_user",   DoubleType()),
    StructField("url_user",       StringType()),
    StructField("url_source",     StringType()),
    StructField("ingest_ts",      StringType()),
])

df_parseado = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), esquema).alias("d"))
    .select("d.*")
    .withColumn("review_date", to_timestamp("review_date"))
    .withColumn("ingest_ts", to_timestamp("ingest_ts"))
)

# -----------------------------------------------------------------------------
# 3a) SINK de inspección rápida (memory) - SOLO para mirar en notebook
# -----------------------------------------------------------------------------
query_preview = (
    df_parseado.writeStream
    .format("memory")
    .queryName("reviews_stream")
    .outputMode("append")
    .start()
)
# spark.sql("SELECT * FROM reviews_stream ORDER BY ingest_ts DESC LIMIT 20").show(truncate=False)

# -----------------------------------------------------------------------------
# 3b) SINK persistente (Delta) - así NO se satura la RAM del driver
# -----------------------------------------------------------------------------
# Rutas del sink Delta — desde el .env (ver CHK_PATH / BRONZE_TABLE).
CHECKPOINT = os.getenv("CHK_PATH")
BRONZE_TABLE = os.getenv("BRONZE_TABLE")

# query_bronze = (
#     df_parseado.writeStream
#     .format("delta")
#     .outputMode("append")
#     .option("checkpointLocation", CHECKPOINT)
#     .trigger(processingTime="10 seconds")   # o .trigger(availableNow=True) para batch incremental
#     .toTable(BRONZE_TABLE)
# )

# -----------------------------------------------------------------------------
# 4) Caso de uso de negocio: alertas de reputación (rating <= 2 en vivo)
# -----------------------------------------------------------------------------
df_alertas = df_parseado.filter(col("rating") <= 2.0)
query_alertas = (
    df_alertas.writeStream
    .format("memory")
    .queryName("alertas_reputacion")
    .outputMode("append")
    .start()
)
# spark.sql("""
#   SELECT place_id, rating, caption, ingest_ts
#   FROM alertas_reputacion ORDER BY ingest_ts DESC
# """).show(truncate=False)

# -----------------------------------------------------------------------------
# 5) Engagement por ventana temporal (conteo de reseñas cada 1 min)
# -----------------------------------------------------------------------------
df_engagement = (
    df_parseado
    .withWatermark("ingest_ts", "2 minutes")
    .groupBy(window(col("ingest_ts"), "1 minute"))
    .count()
)
query_engagement = (
    df_engagement.writeStream
    .format("memory")
    .queryName("engagement_min")
    .outputMode("complete")
    .start()
)
# spark.sql("SELECT * FROM engagement_min ORDER BY window DESC").show(truncate=False)

# -----------------------------------------------------------------------------
# 6) Detener todas las consultas activas
# -----------------------------------------------------------------------------
# for q in spark.streams.active:
#     print("Deteniendo:", q.name)
#     q.stop()
