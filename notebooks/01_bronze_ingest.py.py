# Databricks notebook source
import sys
import os
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType)
from functools import reduce
from datetime import datetime
from dateutil.relativedelta import relativedelta

# importa do src
sys.path.append('/Workspace/siape_pipeline/src')
from data_ingestion import download_and_extract_csv, BASE_URL

# inicia Spark
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------


# Schema raw completo
bronze_schema = StructType([
    StructField("ano", IntegerType(), True),
    StructField("mes", IntegerType(), True),
    StructField("id_servidor_portal", StringType(), True),
    StructField("cpf", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("remuneracao_basica_bruta_rs", DoubleType(), True),
    StructField("remuneracao_basica_bruta_us", DoubleType(), True),
    StructField("abate_teto_rs", DoubleType(), True),
    StructField("abate_teto_us", DoubleType(), True),
    StructField("gratificacao_natalina_rs", DoubleType(), True),
    StructField("gratificacao_natalina_us", DoubleType(), True),
    StructField("abate_teto_grat_natalina_rs", DoubleType(), True),
    StructField("abate_teto_grat_natalina_us", DoubleType(), True),
    StructField("adicional_tempo_servico_rs", DoubleType(), True),
    StructField("adicional_tempo_servico_us", DoubleType(), True),
    StructField("adicional_localidade_rs", DoubleType(), True),
    StructField("adicional_localidade_us", DoubleType(), True),
    StructField("horas_extras_rs", DoubleType(), True),
    StructField("horas_extras_us", DoubleType(), True),
    StructField("insalubridade_rs", DoubleType(), True),
    StructField("insalubridade_us", DoubleType(), True),
    StructField("periculosidade_rs", DoubleType(), True),
    StructField("periculosidade_us", DoubleType(), True),
    StructField("pensao_civil_rs", DoubleType(), True),
    StructField("pensao_civil_us", DoubleType(), True),
    StructField("pensao_militar_rs", DoubleType(), True),
    StructField("pensao_militar_us", DoubleType(), True),
    StructField("fundo_saude_rs", DoubleType(), True),
    StructField("fundo_saude_us", DoubleType(), True),
    StructField("taxa_ocupacao_imovel_funcional_rs", DoubleType(), True),
    StructField("taxa_ocupacao_imovel_funcional_us", DoubleType(), True),
    StructField("remuneracao_pos_deducoes_rs", DoubleType(), True),
    StructField("remuneracao_pos_deducoes_us", DoubleType(), True),
    StructField("verbas_inden_civil_rs", DoubleType(), True),
    StructField("verbas_inden_civil_us", DoubleType(), True),
    StructField("verbas_inden_militar_rs", DoubleType(), True),
    StructField("verbas_inden_militar_us", DoubleType(), True),
    StructField("verbas_inden_deslig_vol_rs", DoubleType(), True),
    StructField("verbas_inden_deslig_vol_us", DoubleType(), True),
    StructField("total_verbas_inden_rs", DoubleType(), True),
    StructField("total_verbas_inden_us", DoubleType(), True),
    StructField("source_file", StringType(), True),
    StructField("reference_dt", DateType(), True),
    StructField("siape_fonte", StringType(), True),
    StructField("data_ingestao", TimestampType(), True)
])
    

# COMMAND ----------

# Cálculo Dinâmico dos Períodos de Ingestão Incremental de Dados

# 1) Lista de fontes
BASE_URL = BASE_URL
BRONZE_TABLE = "public_informations.siape_remuneracao_raw"
names = ["Servidores_SIAPE", "Pensionistas_SIAPE", "Aposentados_SIAPE"]

# 2) Cálculo de períodos M-5 a M-2
tz = pytz.timezone("America/Sao_Paulo")
today = datetime.now(tz)
start_dt = today - relativedelta(months=5)
end_dt   = today - relativedelta(months=2)

periods = []
cur = start_dt.replace(day=1)
while cur <= end_dt.replace(day=1):
    periods.append((cur.year, f"{cur.month:02d}"))
    cur += relativedelta(months=1)

print(f"Todos os períodos possíveis: {periods}")

# 3) Recupera partições já carregadas
existing = (
    spark.table(BRONZE_TABLE)
         .select("reference_dt")
         .distinct()
         .collect()
)
loaded = {row.reference_dt.strftime("%Y-%m-%d") for row in existing}
print(f"Períodos já carregados: {sorted(loaded)}")

# 4) Filtra apenas os novos a ingerir
to_ingest = [
    (y, m) for (y, m) in periods
    if f"{y}-{m}-01" not in loaded
]
print(f"Períodos a ingerir: {to_ingest}")

# 5) Se não houver nada novo, sai
if not to_ingest:
    print("[INFO] Nenhum novo período. Ingestão abortada.")
else:
    df_list = []
    for year_int, month_str in to_ingest:
        for dataset in names:
            print(f"[INFO] Ingerindo {dataset} → {year_int}-{month_str}-01")
            df = download_and_extract_csv(
                year=str(year_int),
                month=month_str,
                dataset=dataset,
                schema=bronze_schema
            )
            df_list.append(df)

    # 6) União e append apenas das novas partições
    df_new = reduce(lambda a, b: a.unionByName(b), df_list)
    df_new.write \
          .format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("reference_dt") \
          .saveAsTable(BRONZE_TABLE)

    print(f"[SUCCESS] Ingestão incremental para M-5 a M-2 concluída: {len(to_ingest)} período(s) adicionados.")
