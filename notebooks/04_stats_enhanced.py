# Databricks notebook source
"""# Notebook 04_stats_enhanced

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

import matplotlib.pyplot as plt

# 1) Inicia Spark e carrega o Gold
spark = SparkSession.builder.getOrCreate()
df = spark.table("public_informations.siape_remuneracao_gold")

# 2) Volume e qualidade de dados
total_reg   = df.count()
total_cpfs  = df.select(F.countDistinct("cpf")).first()[0]
print(f"Total registros: {total_reg:,}")
print(f"Total CPFs únicos: {total_cpfs:,}")

dq = df.select([
    (F.count(F.when(F.col(c).isNull(), c)) / F.count("*") * 100)
      .alias(f"pct_null_{c}")
    for c in ["salario_base","total_verbas"]
])
print("=== % de valores NULL por coluna ===")
dq.show(truncate=False)

# 3) Estatísticas descritivas básicas
print("=== Describe completo ===")
df.select("salario_base","total_verbas").describe().show()

# 4) Quartis via approxQuantile (mais confiável em grandes DFs)
q_sal = df.approxQuantile("salario_base",   [0.25,0.5,0.75], 0.01)
q_ver = df.approxQuantile("total_verbas",   [0.25,0.5,0.75], 0.01)
print(f"Quartis salario_base   : {q_sal}")
print(f"Quartis total_verbas   : {q_ver}")

# 5) Correlação entre as duas métricas
corr = df.stat.corr("salario_base","total_verbas")
print(f"Correlação salário_base x total_verbas: {corr:.4f}")

# 6) Histogramas e boxplots para as duas métricas
pdf = df.select("salario_base","total_verbas").toPandas().dropna()

# força tudo para float antes de plotar, evitando decimal * float
pdf["salario_base"] = pdf["salario_base"].astype("float64")
pdf["total_verbas"] = pdf["total_verbas"].astype("float64")

plt.figure(figsize=(10,4))
plt.subplot(1,2,1)
plt.hist(pdf["salario_base"], bins=50)
plt.title("Histograma: salário_base")
plt.xlabel("salario_base"); plt.ylabel("freq")

plt.subplot(1,2,2)
plt.hist(pdf["total_verbas"], bins=50)
plt.title("Histograma: total_verbas")
plt.xlabel("total_verbas"); plt.ylabel("freq")
plt.tight_layout()
plt.show()

plt.figure(figsize=(10,3))
plt.subplot(1,2,1)
plt.boxplot(pdf["salario_base"], vert=False)
plt.title("Boxplot: salário_base")

plt.subplot(1,2,2)
plt.boxplot(pdf["total_verbas"], vert=False)
plt.title("Boxplot: total_verbas")
plt.tight_layout()
plt.show()

# 7) Tendência mês a mês de média e mediana
w = Window.orderBy("reference_dt")
df_month = (
    df.groupBy("reference_dt")
      .agg(
         F.mean("salario_base").alias("media_salario"),
         F.expr("percentile_approx(salario_base,0.5)").alias("mediana_salario"),
         F.mean("total_verbas").alias("media_verbas"),
         F.expr("percentile_approx(total_verbas,0.5)").alias("mediana_verbas"),
      )
      .orderBy("reference_dt")
)
print("=== Evolução mensal ===")
df_month.show(10,False)

# 8) Crescimento percentual mês a mês
df_growth = df_month.withColumn(
    "pct_growth_salario",
    (F.col("media_salario") - F.lag("media_salario").over(w))
      / F.lag("media_salario").over(w) * 100
).withColumn(
    "pct_growth_verbas",
    (F.col("media_verbas") - F.lag("media_verbas").over(w))
      / F.lag("media_verbas").over(w) * 100
)
print("=== Crescimento pct mensal ===")
df_growth.show(10,False)

# 9) Anomalias via Z-score para ambas as métricas
stats = df.agg(
    F.mean("salario_base").alias("m_s"),
    F.stddev("salario_base").alias("sd_s"),
    F.mean("total_verbas").alias("m_v"),
    F.stddev("total_verbas").alias("sd_v")
).first()

m_s, sd_s, m_v, sd_v = stats["m_s"], stats["sd_s"], stats["m_v"], stats["sd_v"]

df_anom = df.withColumn("z_salario",    (F.col("salario_base") - m_s)/sd_s) \
            .withColumn("z_verbas",    (F.col("total_verbas") - m_v)/sd_v) \
            .filter((F.abs("z_salario") > 3) | (F.abs("z_verbas") > 3)) \
            .select("cpf","reference_dt","salario_base","total_verbas","z_salario","z_verbas")

print("=== Top 10 possíveis outliers ===")
df_anom.show(10,False)"""

# COMMAND ----------

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
