"""Utilidades de datos y modelos para la app Streamlit del proyecto BDA-PI.

Trabaja sobre el dataset local (dataset/reviews_dataset.parquet) usando Polars
para consultas rápidas, y entrena versiones ligeras (scikit-learn) de los
modelos que en producción corren distribuidos en Spark MLlib, con el fin de
demostrar la inferencia de forma interactiva. Las cifras coinciden en orden de
magnitud con las del informe; los modelos locales son una aproximación a los
modelos distribuidos, no los mismos artefactos.
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
import streamlit as st

# --- Rutas -------------------------------------------------------------------
_REPO = Path(__file__).resolve().parent.parent


def data_path() -> Path | None:
    """Ruta al parquet de reseñas. Configurable con BDA_DATA_PATH."""
    env = os.getenv("BDA_DATA_PATH")
    candidates = [
        Path(env) if env else None,
        _REPO / "dataset" / "reviews_dataset.parquet",
        _REPO / "dataset" / "reviews_dataset.csv",
    ]
    for c in candidates:
        if c and c.exists():
            return c
    return None


def fig_dir() -> Path:
    return _REPO / "docs" / "figuras"


def _scan() -> pl.LazyFrame:
    p = data_path()
    if p is None:
        raise FileNotFoundError("No se encontró el dataset.")
    if p.suffix == ".parquet":
        return pl.scan_parquet(p)
    return pl.scan_csv(p)


# --- Stopwords español (compacto, alineado con el notebook) ------------------
SPANISH_STOPS = set(
    """de la que el en y a los del se las por un para con no una su al lo como mas pero
    sus le ya o este si porque esta entre cuando muy sin sobre tambien me hasta hay donde
    quien desde todo nos durante todos uno les ni contra otros ese eso ante ellos esto mi
    antes algunos que unos yo otro otras otra el tanto esa estos mucho quienes nada muchos
    cual poco ella estar estas algunas algo nosotros mis tu tus ellas vosotros os
    si lugar sitio vez ser hacer ir bien aqui cafe restaurante local comida lima peru
    fue son esta estaba muy solo ademas asi""".split()
)


# --- KPIs globales (consulta perezosa, barata) -------------------------------
@st.cache_data(show_spinner=False)
def global_kpis() -> dict:
    lf = _scan().with_columns(pl.col("rating").cast(pl.Float64, strict=False))
    agg = lf.select(
        pl.len().alias("filas"),
        pl.col("id_review").n_unique().alias("unicas"),
        pl.col("username").n_unique().alias("usuarios"),
        pl.col("rating").mean().alias("rating_medio"),
        (pl.col("caption").fill_null("").str.strip_chars().str.len_chars() > 0)
        .sum()
        .alias("con_texto"),
        (pl.col("rating") == 5).sum().alias("cinco"),
        pl.col("rating").is_not_null().sum().alias("rating_valido"),
    ).collect()
    r = agg.to_dicts()[0]
    r["pct_cinco"] = 100.0 * r["cinco"] / max(r["rating_valido"], 1)
    return r


# --- Muestra para gráficos interactivos --------------------------------------
@st.cache_data(show_spinner=False)
def sample_reviews(n: int = 250_000, seed: int = 42) -> pd.DataFrame:
    lf = (
        _scan()
        .select(["rating", "review_date", "caption", "username"])
        .with_columns(
            pl.col("rating").cast(pl.Float64, strict=False),
            pl.col("review_date").str.to_datetime(strict=False).alias("fecha"),
            pl.col("caption")
            .fill_null("")
            .str.split(" ")
            .list.len()
            .alias("n_palabras"),
        )
    )
    df = lf.collect()
    if df.height > n:
        df = df.sample(n=n, seed=seed)
    return df.to_pandas()


# --- Features por usuario (para segmentación) --------------------------------
@st.cache_data(show_spinner=False)
def user_features(min_resenas: int = 1) -> pd.DataFrame:
    lf = (
        _scan()
        .with_columns(
            pl.col("rating").cast(pl.Float64, strict=False),
            pl.col("caption").fill_null("").str.split(" ").list.len().alias("wc"),
        )
        .filter(pl.col("rating").is_not_null())
        .group_by("username")
        .agg(
            pl.len().alias("review_count"),
            pl.col("rating").mean().round(4).alias("avg_rating"),
            pl.col("rating").std().round(4).alias("std_rating"),
            pl.col("wc").mean().round(4).alias("avg_word_count"),
        )
        .with_columns(pl.col("std_rating").fill_null(0.0))
        .filter(pl.col("review_count") >= min_resenas)
    )
    return lf.collect().to_pandas()


def _feature_matrix(df: pd.DataFrame) -> np.ndarray:
    return np.column_stack(
        [
            np.log1p(df["review_count"].to_numpy()),
            df["avg_rating"].to_numpy(),
            df["std_rating"].to_numpy(),
            np.log1p(df["avg_word_count"].to_numpy()),
        ]
    )


def _nombre_segmento(avg_rating: float, review_count: float, std_rating: float,
                     avg_word_count: float) -> str:
    """Etiqueta heurística alineada con los perfiles del informe."""
    if avg_rating <= 2.5 and avg_word_count >= 12:
        return "Crítico detallista"
    if avg_rating <= 2.5:
        return "Decepcionado silencioso"
    if std_rating >= 1.5:
        return "Experiencia errática"
    if avg_rating >= 4.7 and avg_word_count < 8:
        return "Validador masivo"
    if avg_rating >= 4.6 and avg_word_count >= 8:
        return "Promotor genuino"
    if review_count >= 6:
        return "Foodie activo"
    return "Recurrente satisfecho"


@st.cache_resource(show_spinner=False)
def segmentation_model(k: int = 7, fit_n: int = 60_000):
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import StandardScaler

    feats = user_features()
    fit_df = feats.sample(n=min(fit_n, len(feats)), random_state=42)
    X = _feature_matrix(fit_df)
    scaler = StandardScaler().fit(X)
    km = KMeans(n_clusters=k, random_state=42, n_init=10).fit(scaler.transform(X))

    fit_df = fit_df.copy()
    fit_df["cluster"] = km.labels_
    perfil = (
        fit_df.groupby("cluster")
        .agg(
            usuarios=("cluster", "size"),
            review_count=("review_count", "mean"),
            avg_rating=("avg_rating", "mean"),
            std_rating=("std_rating", "mean"),
            avg_word_count=("avg_word_count", "mean"),
        )
        .reset_index()
    )
    perfil["perfil"] = perfil.apply(
        lambda r: _nombre_segmento(
            r["avg_rating"], r["review_count"], r["std_rating"], r["avg_word_count"]
        ),
        axis=1,
    )
    return {"scaler": scaler, "kmeans": km, "perfil": perfil}


def predict_segment(model: dict, review_count: float, avg_rating: float,
                    std_rating: float, avg_word_count: float) -> dict:
    X = np.array([[review_count, avg_rating, std_rating, avg_word_count]], dtype=float)
    Xf = np.column_stack(
        [np.log1p(X[:, 0]), X[:, 1], X[:, 2], np.log1p(X[:, 3])]
    )
    cluster = int(model["kmeans"].predict(model["scaler"].transform(Xf))[0])
    nombre = _nombre_segmento(avg_rating, review_count, std_rating, avg_word_count)
    return {"cluster": cluster, "perfil": nombre}


# --- Tópicos (LDA ligero) ----------------------------------------------------
@st.cache_resource(show_spinner=False)
def topic_model(n_docs: int = 25_000, n_topics: int = 5):
    from sklearn.decomposition import LatentDirichletAllocation
    from sklearn.feature_extraction.text import CountVectorizer

    lf = (
        _scan()
        .select("caption")
        .with_columns(pl.col("caption").fill_null("").str.to_lowercase())
        .filter(pl.col("caption").str.len_chars() > 20)
    )
    caps = lf.collect()
    if caps.height > n_docs:
        caps = caps.sample(n=n_docs, seed=42)
    docs = caps["caption"].to_list()

    vec = CountVectorizer(
        stop_words=list(SPANISH_STOPS),
        max_df=0.85,
        min_df=5,
        max_features=10_000,
        token_pattern=r"(?u)\b[a-záéíóúñ]{3,}\b",
    )
    dtm = vec.fit_transform(docs)
    lda = LatentDirichletAllocation(
        n_components=n_topics, learning_method="online", random_state=42, max_iter=10
    ).fit(dtm)

    vocab = np.array(vec.get_feature_names_out())
    top_terms = []
    for comp in lda.components_:
        idx = comp.argsort()[::-1][:10]
        top_terms.append([str(t) for t in vocab[idx]])
    return {"vectorizer": vec, "lda": lda, "top_terms": top_terms}


def predict_topic(model: dict, texto: str) -> dict:
    dtm = model["vectorizer"].transform([texto.lower()])
    dist = model["lda"].transform(dtm)[0]
    top = int(dist.argmax())
    return {"topico": top, "prob": float(dist[top]), "dist": dist.tolist()}
