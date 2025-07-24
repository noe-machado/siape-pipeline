# Databricks notebook source
import time

# 1) Cria o database (idempotente)
spark.sql("""
  CREATE DATABASE IF NOT EXISTS public_informations
  COMMENT 'Schema para produtos de dados SIAPE e similares';
""")
spark.sql("USE public_informations")

# COMMAND ----------

# 2) Cria a tabela de registro de metadados (schema_registry)
spark.sql("""
  CREATE TABLE IF NOT EXISTS schema_registry (
    table_name STRING,
    description STRING,
    created_at TIMESTAMP
  )
  USING DELTA
""")
spark.sql("""
  INSERT INTO schema_registry (table_name, description, created_at)
  SELECT
    'schema_registry',
    'Registro de todas as tabelas do schema public_informations',
    current_timestamp()
  WHERE NOT EXISTS (
    SELECT 1 FROM schema_registry WHERE table_name = 'schema_registry'
  )
""")

# COMMAND ----------

# 3. Criação da tabela Bronze com reference_dt como STRING
spark.sql("""
CREATE TABLE IF NOT EXISTS siape_remuneracao_raw (
  ano                                  INT,
  mes                                  INT,
  id_servidor_portal                   STRING,
  cpf                                  STRING,
  nome                                 STRING,
  remuneracao_basica_bruta_rs          DOUBLE,
  remuneracao_basica_bruta_us          DOUBLE,
  abate_teto_rs                        DOUBLE,
  abate_teto_us                        DOUBLE,
  gratificacao_natalina_rs             DOUBLE,
  gratificacao_natalina_us             DOUBLE,
  abate_teto_da_gratificacao_natalina_rs  DOUBLE,
  abate_teto_da_gratificacao_natalina_us  DOUBLE,
  adicional_tempo_servico_rs           DOUBLE,
  adicional_tempo_servico_us           DOUBLE,
  adicional_localidade_rs              DOUBLE,
  adicional_localidade_us              DOUBLE,
  horas_extras_rs                      DOUBLE,
  horas_extras_us                      DOUBLE,
  insalubridade_rs                     DOUBLE,
  insalubridade_us                     DOUBLE,
  periculosidade_rs                    DOUBLE,
  periculosidade_us                    DOUBLE,
  pensao_civil_rs                      DOUBLE,
  pensao_civil_us                      DOUBLE,
  pensao_militar_rs                    DOUBLE,
  pensao_militar_us                    DOUBLE,
  fundo_saude_rs                       DOUBLE,
  fundo_saude_us                       DOUBLE,
  taxa_ocupacao_imovel_funcional_rs    DOUBLE,
  taxa_ocupacao_imovel_funcional_us    DOUBLE,
  remuneracao_pos_deducoes_rs          DOUBLE,
  remuneracao_pos_deducoes_us          DOUBLE,
  verbas_inden_civil_rs                DOUBLE,
  verbas_inden_civil_us                DOUBLE,
  verbas_inden_militar_rs              DOUBLE,
  verbas_inden_militar_us              DOUBLE,
  verbas_inden_deslig_vol_rs           DOUBLE,
  verbas_inden_deslig_vol_us           DOUBLE,
  total_verbas_inden_rs                DOUBLE,
  total_verbas_inden_us                DOUBLE,
  siape_fonte                          STRING,
  source_file                          STRING,
  reference_dt                         DATE,
  data_ingestao                        TIMESTAMP
)
USING DELTA
PARTITIONED BY (reference_dt)
""")
time.sleep(2)

# COMMAND ----------

# 4. Criação da view tabela Bronze
spark.sql("""
CREATE OR REPLACE VIEW public_informations.vw_siape_remuneracao_raw AS
SELECT
  ano                                  AS ano,
  mes                                  AS mes,
  id_servidor_portal                   AS id_servidor_portal,
  cpf                                  AS cpf,
  nome                                 AS nome,
  remuneracao_basica_bruta_rs          AS remuneracao_basica_bruta_rs,
  remuneracao_basica_bruta_us          AS remuneracao_basica_bruta_us,
  abate_teto_rs                        AS abate_teto_rs,
  abate_teto_us                        AS abate_teto_us,
  gratificacao_natalina_rs             AS gratificacao_natalina_rs,
  gratificacao_natalina_us             AS gratificacao_natalina_us,
  abate_teto_da_gratificacao_natalina_rs  AS abate_teto_da_gratificacao_natalina_rs,
  abate_teto_da_gratificacao_natalina_us  AS abate_teto_da_gratificacao_natalina_us,
  adicional_tempo_servico_rs           AS adicional_tempo_servico_rs,
  adicional_tempo_servico_us           AS adicional_tempo_servico_us,
  adicional_localidade_rs              AS adicional_localidade_rs,
  adicional_localidade_us              AS adicional_localidade_us,
  horas_extras_rs                      AS horas_extras_rs,
  horas_extras_us                      AS horas_extras_us,
  insalubridade_rs                     AS insalubridade_rs,
  insalubridade_us                     AS insalubridade_us,
  periculosidade_rs                    AS periculosidade_rs,
  periculosidade_us                    AS periculosidade_us,
  pensao_civil_rs                      AS pensao_civil_rs,
  pensao_civil_us                      AS pensao_civil_us,
  pensao_militar_rs                    AS pensao_militar_rs,
  pensao_militar_us                    AS pensao_militar_us,
  fundo_saude_rs                       AS fundo_saude_rs,
  fundo_saude_us                       AS fundo_saude_us,
  taxa_ocupacao_imovel_funcional_rs    AS taxa_ocupacao_imovel_funcional_rs,
  taxa_ocupacao_imovel_funcional_us    AS taxa_ocupacao_imovel_funcional_us,
  remuneracao_pos_deducoes_rs          AS remuneracao_pos_deducoes_rs,
  remuneracao_pos_deducoes_us          AS remuneracao_pos_deducoes_us,
  verbas_inden_civil_rs                AS verbas_inden_civil_rs,
  verbas_inden_civil_us                AS verbas_inden_civil_us,
  verbas_inden_militar_rs              AS verbas_inden_militar_rs,
  verbas_inden_militar_us              AS verbas_inden_militar_us,
  verbas_inden_deslig_vol_rs           AS verbas_inden_deslig_vol_rs,
  verbas_inden_deslig_vol_us           AS verbas_inden_deslig_vol_us,
  total_verbas_inden_rs                AS total_verbas_inden_rs,
  total_verbas_inden_us                AS total_verbas_inden_us,
  siape_fonte                          AS siape_fonte,
  source_file                          AS source_file,
  reference_dt                         AS reference_dt,
  data_ingestao                        AS data_ingestao
FROM public_informations.siape_remuneracao_raw;
""")
time.sleep(2)

# 5) Widgets de parametrização para notebooks sequenciais
dbutils.widgets.text("year", "2025", "Year")
dbutils.widgets.text("month", "01", "Month")
