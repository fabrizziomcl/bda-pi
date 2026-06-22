# App Streamlit — BDA-PI

Demostración interactiva del proyecto de Big Data Analytics sobre reseñas de
restaurantes en Google Maps (Perú).

## Secciones
- **Resumen** — KPIs del corpus calculados en vivo (Polars sobre el parquet).
- **Exploración en vivo** — filtros por calificación / palabra y gráficos
  (distribución de ratings, volumen mensual, engagement textual).
- **Resultados del análisis distribuido** — galería de figuras generadas por los
  notebooks de Spark/Databricks (geo, temporal, categorías, K-Means, LDA).
- **Inferencia · Segmentación de usuarios** — K-Means: dado el comportamiento de
  un revisor, predice su segmento.
- **Inferencia · Tópicos de reseñas** — LDA: clasifica el texto de una reseña en
  una dimensión de la experiencia.

> Los modelos de la app son versiones locales ligeras (scikit-learn) que
> **aproximan** a los modelos distribuidos (Spark MLlib) descritos en el informe;
> sirven para demostrar la inferencia, no son los artefactos de producción.

## Requisitos
- El dataset `dataset/reviews_dataset.parquet` (o `.csv`) en la raíz del repo.
  Alternativamente, define la ruta con la variable de entorno `BDA_DATA_PATH`.
- Dependencias: `pip install -r streamlit/requirements.txt`

## Ejecutar
Desde la raíz del repositorio:

```bash
pip install -r streamlit/requirements.txt
streamlit run streamlit/app.py
```

La galería de resultados funciona aunque no esté el dataset (usa las imágenes de
`docs/figuras/`); las secciones de datos en vivo e inferencia requieren el dataset.
