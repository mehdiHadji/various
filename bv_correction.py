from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    row_number,
    coalesce,
    current_timestamp,
    broadcast
)
from pyspark.sql.window import Window

# =========================================================
# SPARK SESSION
# =========================================================

spark = SparkSession.builder \
    .appName("BV_CORRECTION_EXEMPLE") \
    .getOrCreate()

# =========================================================
# PARAMÈTRES
# =========================================================

date_flux = "2026-04-27"   # J-1

# =========================================================
# 1. DATAFRAMES D'EXEMPLE (createDataFrame)
# =========================================================

# ---------------------------------------------------------
# MAS (Multi Active Satellite)
# ---------------------------------------------------------
# C'est la source principale de la BV

data_mas = [
    ("BK001", "2026-04-27", 1000, "A", "X"),
    ("BK002", "2026-04-27", 2000, "B", "Y"),
    ("BK003", "2026-04-27", 3000, "C", "Z"),
    ("BK004", "2026-04-20", 4000, "D", "W")  # ancienne partition
]

columns_mas = [
    "business_key",
    "date_valorisation",
    "montant",
    "colonne_1",
    "colonne_2"
]

df_mas = spark.createDataFrame(data_mas, columns_mas)

# ---------------------------------------------------------
# HUB
# ---------------------------------------------------------

data_hub = [
    ("BK001", "CLIENT_1"),
    ("BK002", "CLIENT_2"),
    ("BK003", "CLIENT_3"),
    ("BK004", "CLIENT_4")
]

columns_hub = [
    "business_key",
    "client_id"
]

df_hub = spark.createDataFrame(data_hub, columns_hub)

# ---------------------------------------------------------
# AUTRE SATELLITE
# ---------------------------------------------------------

data_other = [
    ("BK001", "ACTIF"),
    ("BK002", "ACTIF"),
    ("BK003", "SUSPENDU"),
    ("BK004", "ACTIF")
]

columns_other = [
    "business_key",
    "statut"
]

df_other = spark.createDataFrame(data_other, columns_other)

# ---------------------------------------------------------
# TABLE DE CORRECTION
# ---------------------------------------------------------
# Exemple :
# le métier corrige une ancienne date_valorisation
# et une correction sur la date du flux

data_corr = [
    ("BK002", "2026-04-27", 2500, "HD_001", "2026-04-28"),
    ("BK004", "2026-04-20", 4500, "HD_002", "2026-04-28"),
    ("BK004", "2026-04-20", 4700, "HD_003", "2026-04-29")  # latest version
]

columns_corr = [
    "business_key",
    "date_valorisation",
    "valeur_correction",
    "hashdiff",
    "load_date"
]

df_corr = spark.createDataFrame(data_corr, columns_corr)

# ---------------------------------------------------------
# TRACKING
# ---------------------------------------------------------
# Ici BK002 déjà traité auparavant

data_tracking = [
    ("BK002", "2026-04-27", "HD_001")
]

columns_tracking = [
    "business_key",
    "date_valorisation",
    "hashdiff"
]

df_tracking = spark.createDataFrame(
    data_tracking,
    columns_tracking
)

# =========================================================
# 2. IDENTIFIER LES CORRECTIONS NON TRAITÉES
# =========================================================

join_tracking_cond = [
    df_corr["business_key"] == df_tracking["business_key"],
    df_corr["date_valorisation"] == df_tracking["date_valorisation"],
    df_corr["hashdiff"] == df_tracking["hashdiff"]
]

df_corr_non_traitees = df_corr.alias("c") \
    .join(
        df_tracking.alias("t"),
        join_tracking_cond,
        "left_anti"
    )

print("=== Corrections non traitées ===")
df_corr_non_traitees.show(truncate=False)

# =========================================================
# 3. DÉDUIRE LES PARTITIONS À RECALCULER
# =========================================================

df_dates_corr = df_corr_non_traitees.select(
    "date_valorisation"
).distinct()

df_date_flux = spark.createDataFrame(
    [(date_flux,)],
    ["date_valorisation"]
)

df_dates_a_recalculer = df_dates_corr.union(
    df_date_flux
).distinct()

dates_list = [
    row["date_valorisation"]
    for row in df_dates_a_recalculer.collect()
]

print("=== Partitions à recalculer ===")
print(dates_list)

# =========================================================
# 4. GARDER LE LATEST RECORD
# =========================================================

window_corr = Window.partitionBy(
    "business_key",
    "date_valorisation"
).orderBy(
    col("load_date").desc()
)

df_corr_latest = df_corr_non_traitees \
    .withColumn(
        "rn",
        row_number().over(window_corr)
    ) \
    .filter(col("rn") == 1) \
    .drop("rn")

print("=== Dernière version des corrections ===")
df_corr_latest.show(truncate=False)

# =========================================================
# 5. FILTRER LE MAS SUR LES PARTITIONS IMPACTÉES
# =========================================================

df_mas_filtered = df_mas.filter(
    col("date_valorisation").isin(dates_list)
)

print("=== MAS filtré ===")
df_mas_filtered.show(truncate=False)

# =========================================================
# 6. JOINTURES VAULT
# =========================================================

df_base = df_mas_filtered.alias("mas") \
    .join(
        df_hub.alias("hub"),
        col("mas.business_key") == col("hub.business_key"),
        "inner"
    ) \
    .join(
        df_other.alias("oth"),
        col("mas.business_key") == col("oth.business_key"),
        "left"
    )

print("=== Dataset de base ===")
df_base.show(truncate=False)

# =========================================================
# 7. APPLICATION DES CORRECTIONS
# =========================================================

df_final = df_base.alias("b") \
    .join(
        broadcast(df_corr_latest.alias("c")),
        [
            col("b.business_key") == col("c.business_key"),
            col("b.date_valorisation") == col("c.date_valorisation")
        ],
        "left"
    ) \
    .select(
        col("b.business_key"),
        col("b.date_valorisation"),
        col("b.client_id"),
        col("b.statut"),

        coalesce(
            col("c.valeur_correction"),
            col("b.montant")
        ).alias("montant_final"),

        col("b.colonne_1"),
        col("b.colonne_2"),
        current_timestamp().alias("date_maj")
    )

print("=== Résultat final BV corrigée ===")
df_final.show(truncate=False)

# =========================================================
# 8. TRACKING FINAL
# =========================================================

df_tracking_new = df_corr_latest.select(
    "business_key",
    "date_valorisation",
    "hashdiff",
    "load_date"
).withColumn(
    "processed_at",
    current_timestamp()
)

print("=== Nouvelles lignes de tracking ===")
df_tracking_new.show(truncate=False)
