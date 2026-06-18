# -*- coding: utf-8 -*-
"""
1_producer.py - Productor / simulador de llegada de reseñas en tiempo real
==========================================================================

Reemplaza al productor de sensores del laboratorio original. En lugar de generar
lecturas de temperatura aleatorias, este script LEE la partición del 1% del
dataset gastronómico (`dataset/reviews_stream_sim.csv`, ~34k reseñas) y la
publica al topic de Kafka fila por fila, simulando que las reseñas van
"llegando" en tiempo real.

Idea clave del proyecto
-----------------------
Los 3.4M de reseñas NO viajan por Kafka. Kafka es solo el tubo de tránsito de lo
NUEVO. Por eso simulamos el flujo incremental con una muestra pequeña: cada fila
se trata como una reseña recién scrapeada que se empuja al topic.

Uso
---
    # Llegadas súper cortas (cada 0.2 s), una reseña por mensaje
    python 1_producer.py --interval 0.2

    # Ráfagas: 50 reseñas cada 1 segundo
    python 1_producer.py --interval 1 --batch-size 50

    # Solo 500 reseñas y termina (útil para demos)
    python 1_producer.py --limit 500 --interval 0.1

    # Repetir en bucle infinito cuando se agote la muestra
    python 1_producer.py --interval 0.1 --loop

    # Probar sin Kafka: imprime a stdout en vez de publicar
    python 1_producer.py --interval 0.1 --dry-run

Credenciales
------------
TODA la configuración se lee del `.env` de la raíz del proyecto (cargado con
python-dotenv) o de variables de entorno. No hay valores hardcodeados. Copia
`.env.example` a `.env` y complétalo (ver KAFKA_HOST, KAFKA_USER,
KAFKA_PASSWORD, KAFKA_CA, KAFKA_TOPIC).
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import pandas as pd

# Carga .env (de la raíz del proyecto) si python-dotenv está instalado.
try:
    from dotenv import load_dotenv

    load_dotenv(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
        )
    )
except ImportError:
    pass  # opcional; las variables igual pueden venir del entorno

try:
    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None  # permite --dry-run sin tener kafka-python instalado


# -----------------------------------------------------------------------------
# 1) Configuración de conexión (TODA vía entorno / .env — nada hardcodeado)
# -----------------------------------------------------------------------------
KAFKA_HOST = os.getenv("KAFKA_HOST")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_CA = os.getenv("KAFKA_CA")
TOPIC = os.getenv("KAFKA_TOPIC")

# Ruta a la partición del 1% generada para la simulación.
DEFAULT_SAMPLE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "dataset",
    "reviews_stream_sim.csv",
)

# Columnas del dataset que se emiten como reseña.
REVIEW_COLUMNS = [
    "place_id",
    "id_review",
    "caption",
    "relative_date",
    "review_date",
    "retrieval_date",
    "rating",
    "username",
    "n_review_user",
    "n_photo_user",
    "url_user",
    "url_source",
]


def build_producer():
    """Crea el KafkaProducer (dict de Python -> JSON -> bytes)."""
    if KafkaProducer is None:
        sys.exit(
            "kafka-python no está instalado. Usa --dry-run o `pip install kafka-python`."
        )
    faltantes = [
        n
        for n, v in {
            "KAFKA_HOST": KAFKA_HOST,
            "KAFKA_USER": KAFKA_USER,
            "KAFKA_PASSWORD": KAFKA_PASSWORD,
            "KAFKA_CA": KAFKA_CA,
        }.items()
        if not v
    ]
    if faltantes:
        sys.exit(
            f"Faltan variables de entorno: {', '.join(faltantes)}. "
            f"Defínelas en el .env de la raíz (ver .env.example)."
        )
    return KafkaProducer(
        bootstrap_servers=KAFKA_HOST,
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-256",
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_cafile=KAFKA_CA,
        value_serializer=lambda v: json.dumps(
            v, ensure_ascii=False, default=str
        ).encode("utf-8"),
        # key = place_id -> todas las reseñas de un local caen en la misma
        # partición, preservando orden por restaurante.
        key_serializer=lambda k: (k or "").encode("utf-8"),
    )


def row_to_review(row: pd.Series) -> dict:
    """Convierte una fila del CSV en el dict de la reseña + sello de ingesta."""
    review = {
        col: (None if pd.isna(row.get(col)) else row.get(col)) for col in REVIEW_COLUMNS
    }
    # ingest_ts: marca el momento de "llegada" al stream (no es review_date).
    review["ingest_ts"] = datetime.now(timezone.utc).isoformat()
    return review


def main():
    parser = argparse.ArgumentParser(
        description="Productor / simulador de llegada de reseñas a Kafka"
    )
    parser.add_argument(
        "--sample",
        default=DEFAULT_SAMPLE,
        help="CSV de la partición del 1% (default: dataset/reviews_stream_sim.csv)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="Segundos entre mensajes/ráfagas (default: 0.2; usa 0.05 para súper corto)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help="Reseñas por ráfaga antes de dormir --interval (default: 1)",
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Máximo de reseñas a emitir (0 = todas)"
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Reiniciar desde el principio cuando se agote la muestra",
    )
    parser.add_argument(
        "--topic", default=TOPIC, help="Topic destino (default: $KAFKA_TOPIC del .env)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="No publica a Kafka; imprime los mensajes por stdout",
    )
    args = parser.parse_args()

    if not args.topic and not args.dry_run:
        sys.exit("Falta el topic: define KAFKA_TOPIC en el .env o pásalo con --topic.")

    if not os.path.exists(args.sample):
        sys.exit(
            f"No existe la muestra: {args.sample}\n"
            f"Genérala muestreando el 1% del dataset antes de correr el productor."
        )

    print(f"[producer] Cargando muestra: {args.sample}")
    df = pd.read_csv(args.sample, dtype=str, keep_default_na=True)
    print(
        f"[producer] {len(df):,} reseñas listas para simular | topic='{args.topic}' "
        f"| interval={args.interval}s | batch={args.batch_size} | dry_run={args.dry_run}"
    )

    producer = None if args.dry_run else build_producer()
    sent = 0
    try:
        while True:
            for _, row in df.iterrows():
                review = row_to_review(row)
                if args.dry_run:
                    print(json.dumps(review, ensure_ascii=False, default=str))
                else:
                    producer.send(args.topic, key=review["place_id"], value=review)
                sent += 1

                if args.limit and sent >= args.limit:
                    print(f"[producer] Límite alcanzado ({args.limit}).")
                    return
                # Dormir solo al cerrar cada ráfaga de --batch-size mensajes.
                if sent % args.batch_size == 0:
                    if producer is not None:
                        producer.flush()
                    if sent % (args.batch_size * 20) == 0:
                        print(f"[producer] emitidas {sent:,} reseñas...")
                    time.sleep(args.interval)

            if not args.loop:
                break
            print("[producer] Muestra agotada, reiniciando (--loop).")
    except KeyboardInterrupt:
        print("\n[producer] Detenido por el usuario.")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        print(f"[producer] Total emitido: {sent:,} reseñas.")


if __name__ == "__main__":
    main()
