# Databricks notebook source
# Notebook 04_stats_enhanced (focado só em salário_base)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt

# 1) Inicia Spark e carrega o Gold
spark = SparkSession.builder.getOrCreate()
df = spark.table("public_informations.siape_remuneracao_gold")

#Filtra só salários strictly positivos
df = df.filter(col("salario_base") > 0)         

# 2) Volume e qualidade de dados (apenas salario_base)
total_reg  = df.count()
total_cpfs = df.select(F.countDistinct("cpf")).first()[0]
print(f"Total registros: {total_reg:,}")
print(f"Total CPFs únicos: {total_cpfs:,}")

dq = df.select(
    (F.count(F.when(F.col("salario_base").isNull(), "salario_base")) 
       / F.count("*") * 100).alias("pct_null_salario_base")
)
print("=== % de valores NULL em salario_base ===")
dq.show(truncate=False)

# 3) Estatísticas descritivas básicas para salario_base
print("=== Describe salario_base ===")
df.select("salario_base").describe().show()

# 4) Quartis via approxQuantile
q = df.approxQuantile("salario_base", [0.25, 0.5, 0.75], 0.01)
print(f"Quartis salario_base (25%,50%,75%): {q}")

# 5) Tendência mensal de média e mediana
w = Window.orderBy("reference_dt")
df_month = (
    df.groupBy("reference_dt")
      .agg(
         F.mean("salario_base").alias("media_salario"),
         F.expr("percentile_approx(salario_base,0.5)").alias("mediana_salario")
      )
      .orderBy("reference_dt")
)
print("=== Evolução mensal ===")
df_month.show(truncate=False)

# 6) Crescimento percentual mês a mês da média
df_growth = df_month.withColumn(
    "pct_growth_media_salario",
    (F.col("media_salario") - F.lag("media_salario").over(w))
      / F.lag("media_salario").over(w) * 100
)
print("=== Crescimento pct mensal da média ===")
df_growth.show(truncate=False)

# 7) Anomalias via Z-score em salario_base
stats = df.agg(
    F.mean("salario_base").alias("m"),
    F.stddev("salario_base").alias("s")
).first()
m, s = stats["m"], stats["s"]

df_anom = (
    df.withColumn("z_score", (F.col("salario_base") - m)/s)
      .filter(F.abs("z_score") > 3)
      .select("cpf","reference_dt","salario_base","z_score")
)
print("=== Top 10 outliers em salario_base (|z|>3) ===")
df_anom.show(10, False)

# 8) Histogram e boxplot de salario_base
pdf = df.select("salario_base").toPandas().dropna()
pdf["salario_base"] = pdf["salario_base"].astype("float64")

plt.figure(figsize=(14,5))
plt.subplot(1,2,1)
plt.hist(pdf["salario_base"], bins=100, range=(0,150000))
plt.title("Histograma: salario_base")
plt.xlabel("Salário base (R$)"); plt.ylabel("Frequência salarial")

plt.subplot(1,2,2)
plt.boxplot(pdf["salario_base"], vert=False)
plt.title("Boxplot: salario_base")
plt.xlabel("Salário base (R$)")
plt.tight_layout()
plt.show()
