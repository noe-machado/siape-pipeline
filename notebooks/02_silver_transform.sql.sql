-- Databricks notebook source
-- Notebook 02_silver_transform

USE public_informations;

------------------------------------------------------------------------------
-- 1) Criação idempotente da Silver (mantendo DOUBLE)
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS siape_remuneracao_silver (
  cpf                          STRING,
  nome                         STRING,
  siape_fonte                  STRING,
  salario_base                 DOUBLE,
  abate_teto                   DOUBLE,
  grat_natalina                DOUBLE,
  adicional_tempo_servico      DOUBLE,
  adicional_localidade         DOUBLE,
  horas_extras                 DOUBLE,
  insalubridade                DOUBLE,
  periculosidade               DOUBLE,
  pensao_civil                 DOUBLE,
  pensao_militar               DOUBLE,
  fundo_saude                  DOUBLE,
  taxa_ocupacao_imovel         DOUBLE,
  remuneracao_pos_deducoes     DOUBLE,
  total_verbas_inden           DOUBLE,
  source_file                  STRING,
  reference_dt                 DATE
)
USING DELTA
PARTITIONED BY (reference_dt);

------------------------------------------------------------------------------
-- 2) Registro no schema_registry (1ª execução)
------------------------------------------------------------------------------
INSERT INTO schema_registry (table_name, description, created_at)
SELECT
  'siape_remuneracao_silver',
  'Silver: remuneração SIAPE limpa e arredondada a 2 casas (DOUBLE)',
  current_timestamp()
WHERE NOT EXISTS (
  SELECT 1
    FROM schema_registry
   WHERE table_name = 'siape_remuneracao_silver'
);

------------------------------------------------------------------------------
-- 3) Limpeza idempotente
------------------------------------------------------------------------------
TRUNCATE TABLE siape_remuneracao_silver;

------------------------------------------------------------------------------
-- 4) Popula o Silver usando ROUND(...,2) sobre DOUBLE
------------------------------------------------------------------------------
INSERT INTO siape_remuneracao_silver
SELECT
  cpf,
  nome,
  siape_fonte,
  ROUND(remuneracao_basica_bruta_rs,       2) AS salario_base,
  ROUND(abate_teto_rs,                     2) AS abate_teto,
  ROUND(gratificacao_natalina_rs,          2) AS grat_natalina,
  ROUND(adicional_tempo_servico_rs,        2) AS adicional_tempo_servico,
  ROUND(adicional_localidade_rs,           2) AS adicional_localidade,
  ROUND(horas_extras_rs,                   2) AS horas_extras,
  ROUND(insalubridade_rs,                  2) AS insalubridade,
  ROUND(periculosidade_rs,                 2) AS periculosidade,
  ROUND(pensao_civil_rs,                   2) AS pensao_civil,
  ROUND(pensao_militar_rs,                 2) AS pensao_militar,
  ROUND(fundo_saude_rs,                    2) AS fundo_saude,
  ROUND(taxa_ocupacao_imovel_funcional_rs, 2) AS taxa_ocupacao_imovel,
  ROUND(remuneracao_pos_deducoes_rs,       2) AS remuneracao_pos_deducoes,
  ROUND(total_verbas_inden_rs,             2) AS total_verbas_inden,
  source_file,
  reference_dt
FROM siape_remuneracao_raw
WHERE cpf IS NOT NULL;
