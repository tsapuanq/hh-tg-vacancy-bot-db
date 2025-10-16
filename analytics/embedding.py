import pandas as pd
import numpy as np
import logging
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

# ========== НАСТРОЙКИ ==========
INPUT_CSV = "local/vacancies_rows-2.csv"
OUTPUT_CSV = "local/vacancies_clusters_auto.csv"
MIN_CLUSTERS = 4
MAX_CLUSTERS = 12  # можно расширить до 15
SAMPLE_FOR_SILHOUETTE = 1000  # если >1000 строк — для ускорения

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ========== 1. ЗАГРУЗКА CSV ==========
logging.info("📂 Загружаем CSV...")
df = pd.read_csv(INPUT_CSV)
logging.info(f"✅ Загружено {len(df)} вакансий")

required_cols = {"title", "description"}
if not required_cols.issubset(df.columns):
    raise ValueError(f"❌ В CSV нет нужных колонок: {required_cols - set(df.columns)}")

# Объединяем title + description
df["text"] = (
    df["title"].fillna("").astype(str).str.strip()
    + " — "
    + df["description"].fillna("").astype(str).str.strip()
)
df["text"] = df["text"].replace(["nan — nan", " — ", "nan", ""], np.nan)
df = df.dropna(subset=["text"])
logging.info(f"🧩 Готово к эмбеддингу: {len(df)} строк")

# ========== 2. МОДЕЛЬ ==========
logging.info("🧠 Загружаем модель intfloat/multilingual-e5-large...")
model = SentenceTransformer("intfloat/multilingual-e5-large")
logging.info("✅ Модель загружена")

# ========== 3. ЭМБЕДДИНГИ ==========
logging.info("⚙️ Считаем эмбеддинги (может занять до 1 мин)...")
embeddings = model.encode(df["text"].tolist(), show_progress_bar=True, batch_size=16, normalize_embeddings=True)
logging.info(f"✅ Эмбеддинги рассчитаны: {embeddings.shape}")

# ========== 4. ПОДБОР КОЛ-ВА КЛАСТЕРОВ ==========
logging.info("🔍 Подбираем оптимальное количество кластеров...")
scores = {}
for k in range(MIN_CLUSTERS, MAX_CLUSTERS + 1):
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(embeddings)
    sample_idx = np.random.choice(len(embeddings), min(SAMPLE_FOR_SILHOUETTE, len(embeddings)), replace=False)
    score = silhouette_score(embeddings[sample_idx], labels[sample_idx])
    scores[k] = score
    logging.info(f"k={k}: silhouette={score:.4f}")

best_k = max(scores, key=scores.get)
logging.info(f"✅ Лучшее количество кластеров: {best_k}")

# ========== 5. КЛАСТЕРИЗАЦИЯ ==========
logging.info(f"📊 Запускаем KMeans с {best_k} кластерами...")
kmeans = KMeans(n_clusters=best_k, random_state=42, n_init=10)
df["cluster"] = kmeans.fit_predict(embeddings)

# ========== 6. НАЗВАНИЯ КЛАСТЕРОВ ==========
def extract_cluster_keywords(texts, top_n=5):
    words = " ".join(texts).lower().split()
    stopwords = {"и", "в", "на", "по", "для", "the", "of", "to", "a", "in", "—"}
    freq = pd.Series([w for w in words if len(w) > 3 and w not in stopwords]).value_counts()
    return ", ".join(freq.head(top_n).index)

cluster_names = {}
for i in range(best_k):
    titles = df[df["cluster"] == i]["title"].tolist()
    cluster_names[i] = extract_cluster_keywords(titles)
    logging.info(f"📦 Кластер {i}: {cluster_names[i]} ({len(titles)} вакансий)")

df["cluster_name"] = df["cluster"].map(cluster_names)

# ========== 7. АНАЛИТИКА ==========
logging.info("\n📈 Распределение по кластерам:")
cluster_stats = (
    df["cluster_name"]
    .value_counts(normalize=True)
    .mul(100)
    .round(1)
    .reset_index()
    .rename(columns={"index": "Cluster", "cluster_name": "Percent"})
)
print(cluster_stats.to_string(index=False))

# ========== 8. ПРОВЕРКА ==========
logging.info("\n🔍 Примеры по кластерам:")
for i, name in cluster_names.items():
    subset = df[df["cluster"] == i].head(5)
    logging.info(f"\n=== КЛАСТЕР {i} ({name}) ===")
    for _, row in subset.iterrows():
        logging.info(f"• {row['title']}")

# ========== 9. СОХРАНЕНИЕ ==========
df.to_csv(OUTPUT_CSV, index=False)
logging.info(f"\n✅ Результат сохранён в {OUTPUT_CSV}")