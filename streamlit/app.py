"""App Streamlit — Framework de Big Data Analytics sobre reseñas de Google Maps.

Showcase del proyecto: KPIs, consultas en vivo sobre el corpus, galería de
resultados del análisis distribuido (Spark/Databricks) e inferencia interactiva
de los modelos (segmentación de usuarios con K-Means y tópicos con LDA).

Ejecutar desde la raíz del repo:
    streamlit run streamlit/app.py
"""
import pandas as pd
import streamlit as st

import lib

st.set_page_config(
    page_title="BDA-PI · Reseñas gastronómicas",
    page_icon="🍽️",
    layout="wide",
)

st.title("🍽️ Big Data Analytics sobre reseñas de restaurantes en el Perú")
st.caption(
    "Reseñas públicas de Google Maps · Arquitectura Lambda (medallón + Kafka) "
    "sobre Spark y Delta Lake en Databricks"
)

DATA_OK = lib.data_path() is not None
if not DATA_OK:
    st.warning(
        "No se encontró `dataset/reviews_dataset.parquet`. Las secciones de datos "
        "en vivo e inferencia quedan deshabilitadas; configura la variable de "
        "entorno `BDA_DATA_PATH` o coloca el dataset en `dataset/`. "
        "La galería de resultados sí está disponible."
    )

seccion = st.sidebar.radio(
    "Secciones",
    [
        "Resumen",
        "Exploración en vivo",
        "Resultados del análisis distribuido",
        "Inferencia · Segmentación de usuarios",
        "Inferencia · Tópicos de reseñas",
    ],
)
st.sidebar.markdown("---")
st.sidebar.caption(
    "Los modelos de esta app son versiones locales ligeras (scikit-learn) que "
    "aproximan a los modelos distribuidos (Spark MLlib) descritos en el informe."
)


# ============================================================ Resumen
if seccion == "Resumen":
    st.subheader("Resumen del corpus")
    if DATA_OK:
        k = lib.global_kpis()
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Reseñas (crudas)", f"{k['filas']:,}")
        c2.metric("Reseñas únicas", f"{k['unicas']:,}")
        c3.metric("Usuarios", f"{k['usuarios']:,}")
        c4.metric("Con texto libre", f"{k['con_texto']:,}")
        c5, c6 = st.columns(2)
        c5.metric("Calificación media", f"{k['rating_medio']:.2f} / 5")
        c6.metric("Reseñas de 5 estrellas", f"{k['pct_cinco']:.1f} %")
    else:
        st.info("Conecta el dataset para ver los KPIs en vivo.")

    st.markdown(
        """
**Qué hace este proyecto.** Recolecta reseñas públicas de restaurantes en Google
Maps a escala nacional y las procesa de forma **distribuida** sobre Apache Spark
(Databricks), bajo una **arquitectura Lambda**: una ruta *batch* organizada en
medallón (Bronze → Silver → Gold) sobre Delta Lake, y una ruta de **velocidad**
que ingiere reseñas nuevas vía **Kafka + Spark Structured Streaming** con un
filtro anti-duplicados por *hash*. Sobre las vistas Gold operan la segmentación
de usuarios (**K-Means**) y el modelado de tópicos (**LDA**).

Esta aplicación permite **explorar el corpus en vivo**, revisar los **resultados
del análisis distribuido** y **probar la inferencia** de ambos modelos.
"""
    )


# ============================================================ Exploración en vivo
elif seccion == "Exploración en vivo":
    st.subheader("Consultas en vivo sobre el corpus")
    if not DATA_OK:
        st.stop()

    with st.spinner("Cargando muestra del corpus…"):
        df = lib.sample_reviews()
    st.caption(f"Muestra interactiva de {len(df):,} reseñas (el cómputo real corre en Spark).")

    f1, f2, f3 = st.columns([1, 1, 2])
    rmin, rmax = f1.select_slider(
        "Rango de calificación", options=[1, 2, 3, 4, 5], value=(1, 5)
    )
    palabra = f2.text_input("Buscar palabra en la reseña", "")
    solo_texto = f3.checkbox("Solo reseñas con texto", value=False)

    q = df[(df["rating"] >= rmin) & (df["rating"] <= rmax)]
    if palabra.strip():
        q = q[q["caption"].str.contains(palabra.strip(), case=False, na=False)]
    if solo_texto:
        q = q[q["caption"].str.strip().str.len() > 0]

    st.metric("Reseñas que cumplen el filtro (en la muestra)", f"{len(q):,}")

    g1, g2 = st.columns(2)
    with g1:
        st.markdown("**Distribución de calificaciones**")
        dist = q["rating"].value_counts().sort_index()
        st.bar_chart(dist)
    with g2:
        st.markdown("**Volumen mensual de reseñas**")
        serie = (
            q.dropna(subset=["fecha"])
            .assign(mes=lambda d: d["fecha"].dt.to_period("M").dt.to_timestamp())
            .groupby("mes")
            .size()
        )
        st.line_chart(serie)

    st.markdown("**Extensión del texto según calificación** (palabras promedio)")
    eng = q.groupby("rating")["n_palabras"].mean()
    st.bar_chart(eng)

    st.markdown("**Ejemplos de reseñas filtradas**")
    st.dataframe(
        q[["rating", "caption", "username"]].head(50), use_container_width=True
    )


# ============================================================ Galería resultados
elif seccion == "Resultados del análisis distribuido":
    st.subheader("Resultados del análisis distribuido (Spark · Databricks)")
    st.caption(
        "Figuras generadas por los notebooks distribuidos sobre las vistas Gold."
    )
    figuras = [
        ("dist_geo_regYprov.png", "Distribución geográfica de reseñas (departamento y provincia)"),
        ("dist_temp.png", "Evolución temporal del volumen de reseñas"),
        ("dist_temp_semanal.png", "Engagement por día de la semana"),
        ("stats_categoria.png", "Top categorías por rating y aprobación"),
        ("kmeans_seleccion.png", "Selección de k (silueta y codo)"),
        ("kmeans_spark.png", "Segmentación de usuarios (K-Means)"),
        ("kmeans_perfil.png", "Perfil normalizado de los clústeres"),
        ("lda_spark.png", "Tópicos latentes (LDA)"),
    ]
    fd = lib.fig_dir()
    cols = st.columns(2)
    for i, (fname, cap) in enumerate(figuras):
        fpath = fd / fname
        if fpath.exists():
            cols[i % 2].image(str(fpath), caption=cap, use_container_width=True)


# ============================================================ Segmentación
elif seccion == "Inferencia · Segmentación de usuarios":
    st.subheader("Inferencia · Segmentación de usuarios (K-Means)")
    if not DATA_OK:
        st.stop()
    st.markdown(
        "Introduce el comportamiento de un usuario/revisor y el modelo predice a "
        "qué **segmento** pertenece. Las variables replican las del informe: "
        "número de reseñas, calificación media, dispersión y extensión textual."
    )

    with st.spinner("Preparando el modelo de segmentación…"):
        model = lib.segmentation_model()

    c1, c2, c3, c4 = st.columns(4)
    rc = c1.number_input("N.º de reseñas", min_value=1, max_value=500, value=3)
    ar = c2.slider("Calificación media", 1.0, 5.0, 4.5, 0.1)
    sr = c3.slider("Dispersión (std rating)", 0.0, 2.0, 0.5, 0.1)
    wc = c4.number_input("Palabras promedio", min_value=0, max_value=200, value=10)

    if st.button("Predecir segmento", type="primary"):
        res = lib.predict_segment(model, rc, ar, sr, wc)
        st.success(f"Segmento estimado: **{res['perfil']}**  (clúster {res['cluster']})")

    st.markdown("**Perfiles encontrados por el modelo** (muestra de usuarios)")
    perfil = model["perfil"].copy()
    perfil = perfil.rename(
        columns={
            "review_count": "reseñas_prom",
            "avg_rating": "rating_prom",
            "std_rating": "disp_rating",
            "avg_word_count": "palabras_prom",
        }
    ).round(2)
    st.dataframe(perfil, use_container_width=True)


# ============================================================ Tópicos
elif seccion == "Inferencia · Tópicos de reseñas":
    st.subheader("Inferencia · Tópicos de reseñas (LDA)")
    if not DATA_OK:
        st.stop()
    st.markdown(
        "Escribe una reseña y el modelo de tópicos estima a qué **dimensión de la "
        "experiencia** corresponde, a partir de los términos aprendidos del corpus."
    )

    with st.spinner("Entrenando el modelo de tópicos (una sola vez)…"):
        model = lib.topic_model()

    texto = st.text_area(
        "Reseña",
        "El servicio fue lento y la comida llegó fría, una pésima experiencia.",
        height=100,
    )
    if st.button("Clasificar tópico", type="primary") and texto.strip():
        res = lib.predict_topic(model, texto)
        terms = ", ".join(model["top_terms"][res["topico"]][:6])
        st.success(
            f"Tópico dominante: **#{res['topico']}**  (probabilidad {res['prob']:.2f})"
        )
        st.write(f"Términos representativos del tópico: _{terms}_")

    st.markdown("**Tópicos aprendidos y sus términos dominantes**")
    tabla = pd.DataFrame(
        {
            "Tópico": [f"#{i}" for i in range(len(model["top_terms"]))],
            "Términos dominantes": [", ".join(t) for t in model["top_terms"]],
        }
    )
    st.dataframe(tabla, use_container_width=True, hide_index=True)
