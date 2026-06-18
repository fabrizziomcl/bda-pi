# Módulo de Streaming en Tiempo Real (Kafka + Spark)

Este módulo es la capa de **ingesta en tiempo real** del pipeline gastronómico
BDA. Simula la llegada continua de reseñas nuevas de Google Maps y las procesa
con **Spark Structured Streaming**, alimentando la capa Bronze de la arquitectura
Medallion.

```
1_producer.py  ──►  Kafka (Aiven)  ──►  2_consumidor.py  ──►  Delta / Bronze
(simulador)         topic: reviews-nuevas   (Spark Streaming)     + alertas en vivo
```

---

## Rol en el pipeline

Las **3.4M de reseñas históricas** viven en el data lake (`dataset/`, capa
Medallion). **Kafka no almacena ese histórico**: es el tubo de tránsito de lo
**nuevo**. Para demostrar el flujo incremental sin reprocesar millones de filas,
usamos una partición del **1% del dataset** (`dataset/reviews_stream_sim.csv`,
~34k reseñas) como fuente de la simulación: cada fila se publica como si fuera
una reseña recién scrapeada.

> **Aclaración técnica:** el consumidor **no** carga el dataset completo en
> memoria. Spark Structured Streaming procesa **micro-batches incrementales** y,
> con `startingOffsets=latest`, solo lee lo nuevo. El sink persistente (Delta con
> *checkpoint*) evita saturar la RAM del driver; el sink `memory` queda reservado
> para inspección rápida en notebook.

---

## Estructura

| Archivo | Descripción |
|---|---|
| `1_producer.py` | **Productor / simulador.** Lee la partición del 1% y publica reseñas al topic en intervalos configurables (cortos o súper cortos). |
| `2_consumidor.py` | **Consumidor Spark Structured Streaming.** Lee el topic, parsea el JSON al esquema de reseñas y escribe a sinks (`memory` para demo, `delta` para Bronze). Incluye casos de uso de negocio. |
| `1_Producer.ipynb` / `2_Consumidor.ipynb` | Versiones notebook (Colab/Databricks). |

---

## Requisitos

- Python ≥ 3.9
- Cuenta en [Aiven](https://console.aiven.io/) con un servicio Kafka (o cualquier broker Kafka con SASL_SSL)
- Para el consumidor: Databricks (recomendado) o Spark ≥ 3.5 en local

```bash
# Dependencias generales del proyecto (a nivel raíz)
pip install -r ../requirements.txt
```

---

## Configuración (Aiven)

1. Crea un servicio Kafka en Aiven.
2. Activa `kafka_authentication_methods.sasl` y `auto_create_topics_enable`.
3. Crea el topic **`reviews-nuevas`** con 2 particiones y 2 réplicas.
   *(Se particiona por `place_id` → las reseñas de un mismo local mantienen orden.)*
4. En **Connection information → Apache Kafka** anota el host, usuario (`avnadmin`)
   y contraseña, y descarga el certificado **`ca.pem`**.

### Variables de entorno

Las credenciales **no se hardcodean**. Copia la plantilla y rellénala:

```bash
cp .env.example .env      # en la raíz del proyecto
# edita .env y completa KAFKA_PASSWORD
```

| Variable | Descripción |
|---|---|
| `KAFKA_HOST` | Broker de entrada (`host:puerto`). |
| `KAFKA_USER` | Usuario SASL (`avnadmin`). |
| `KAFKA_PASSWORD` | Contraseña SASL (**nunca commitear**). |
| `KAFKA_CA` | Ruta al `ca.pem`. |
| `KAFKA_TOPIC` | Topic destino (`reviews-nuevas`). |
| `CHK_PATH` | Ruta de checkpoint del sink Delta (consumidor). |
| `BRONZE_TABLE` | Tabla Bronze destino (consumidor). |

> ⚠️ `.env` y `*.pem` están en `.gitignore`. Si alguna credencial se filtró
> previamente, **rótala** en la consola de Aiven.

---

## Uso

### 1) Generar la partición del 1% (una vez)

```python
import pandas as pd
df = pd.read_parquet("dataset/reviews_dataset.parquet")
(df.sample(frac=0.01, random_state=42)
   .sort_values("review_date")
   .to_csv("dataset/reviews_stream_sim.csv", index=False))
```

### 2) Productor / simulador

```bash
# Llegadas cortas (una reseña cada 0.2 s)
python 1_producer.py --interval 0.2

# Súper corto + ráfagas (50 reseñas/seg)
python 1_producer.py --interval 0.05 --batch-size 50

# Demo acotada (500 reseñas y termina)
python 1_producer.py --limit 500 --interval 0.1

# Bucle infinito al agotar la muestra
python 1_producer.py --interval 0.1 --loop

# Probar sin Kafka (imprime el JSON por stdout)
python 1_producer.py --dry-run --interval 0.1
```

| Flag | Descripción | Default |
|---|---|---|
| `--sample` | CSV de la partición del 1%. | `dataset/reviews_stream_sim.csv` |
| `--interval` | Segundos entre mensajes/ráfagas. | `0.2` |
| `--batch-size` | Reseñas por ráfaga antes de dormir. | `1` |
| `--limit` | Máximo de reseñas a emitir (`0` = todas). | `0` |
| `--loop` | Reiniciar al agotar la muestra. | `False` |
| `--topic` | Topic destino. | `reviews-nuevas` |
| `--dry-run` | No publica; imprime por stdout. | `False` |

### 3) Consumidor (Spark Structured Streaming)

Ejecuta `2_consumidor.py` (o el notebook) en Databricks/Spark. Levanta tres
consultas de streaming sobre el mismo flujo:

- **`reviews_stream`** — preview en memoria (solo inspección).
- **`alertas_reputacion`** — reseñas nuevas con `rating ≤ 2` (alerta temprana de reputación).
- **`engagement_min`** — conteo de reseñas por ventana de 1 minuto.

Y deja preparado el sink **Delta** hacia la tabla Bronze
(`bronze_reviews_stream`) con *checkpoint* para procesamiento escalable y
exactly-once.

```python
spark.sql("SELECT * FROM reviews_stream ORDER BY ingest_ts DESC LIMIT 20").show()
spark.sql("SELECT place_id, rating, caption FROM alertas_reputacion").show()
```

---

## Esquema del mensaje

Cada reseña publicada es un JSON con las 12 columnas del dataset más `ingest_ts`
(momento de llegada al stream):

```json
{
  "place_id": "ChIJ...", "id_review": "Ci9D...", "caption": "Buena atención",
  "relative_date": "Hace 2 meses", "review_date": "2026-02-23 22:36:28",
  "retrieval_date": "2026-04-24 22:36:28", "rating": "5.0",
  "username": "Kenny A.", "n_review_user": "108.0", "n_photo_user": null,
  "url_user": "https://...", "url_source": "https://...",
  "ingest_ts": "2026-06-18T12:34:26.230387+00:00"
}
```

La **key** del mensaje es el `place_id`, garantizando orden por local dentro de
cada partición.

---

## Integración con el resto del pipeline

El sink Delta (`bronze_reviews_stream`) se conecta naturalmente con la
arquitectura Medallion (`notebooks/Medallion.ipynb`): las reseñas que llegan en
streaming aterrizan en Bronze y de ahí pueden fluir a Silver → Gold (limpieza,
sentimiento, agregados) en micro-batch o streaming continuo.
