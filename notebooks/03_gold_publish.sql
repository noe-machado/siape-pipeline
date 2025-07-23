-- Databricks notebook source
-- Notebook 03_gold_publish

USE public_informations;

-- 0) Desliga o modo ANSI para permitir truncamento em CAST→DECIMAL
SET spark.sql.ansi.enabled = false;

-- 1) Remove a velha (se existir)
DROP TABLE IF EXISTS siape_remuneracao_gold;

-- 2) Cria a Gold com DECIMAL(18,2)
CREATE TABLE siape_remuneracao_gold (
  cpf             STRING,
  nome            STRING,
  reference_dt    DATE,
  salario_base    DECIMAL(18,2),
  total_verbas    DECIMAL(18,2)
)
USING DELTA
PARTITIONED BY (reference_dt);

-- 3) Registra no schema_registry (só na 1ª vez)
INSERT INTO schema_registry (table_name, description, created_at)
SELECT
  'siape_remuneracao_gold',
  'Gold: agregação da remuneração por CPF e mês, com duas casas decimais',
  current_timestamp()
WHERE NOT EXISTS (
  SELECT 1
    FROM schema_registry
   WHERE table_name = 'siape_remuneracao_gold'
);

-- 4) Popula o Gold, agora o CAST vai funcionar sem estourar
INSERT INTO siape_remuneracao_gold
SELECT
  cpf,
  FIRST(nome, TRUE)                                                        AS nome,
  reference_dt,
  CAST( ROUND( SUM( COALESCE(salario_base,    0.0) ), 2)  AS DECIMAL(18,2)) AS salario_base,
  CAST( ROUND( SUM( COALESCE(total_verbas_inden, 0.0) ), 2)  AS DECIMAL(18,2)) AS total_verbas
FROM siape_remuneracao_silver
WHERE cpf IS NOT NULL
GROUP BY
  cpf,
  reference_dt;
