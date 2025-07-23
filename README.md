# SIAPE Pipeline Mensal

## 1. Descrição
Este projeto implementa um pipeline de ingestão, transformação e agregação dos dados de remuneração do SIAPE, atendendo aos requisitos de:
- Histórico de 3 meses (M-5 a M-2)  
- Posição mais atual (M-2)  
- Consumo a nível de CPF e “time travel” por data de referência  
- Identificação da fonte (servidor, aposentado, pensionista)  
- Automação incremental mensal  

A solução roda em Databricks, usa Delta Lake para armazenamento e Spark para processamento distribuído.

---

## 2. Pré-requisitos
- Conta Databricks com workspace ou repositório Git configurado  
- Cluster (Serverless ou All-Purpose) rodando Databricks Runtime 13.x ou superior  
- Bibliotecas Python disponíveis no cluster: `pandas`, `requests`, `python-dateutil`  
- Permissões para criar schemas, tabelas e volumes em Unity Catalog (schema `public_informations`)

---

## 3. Estrutura do Repositório
```
/siape_pipeline
├── notebooks
│ ├── 00_config.py                # setup de schema, widgets e variáveis
│ ├── 01_bronze_ingest.py         # ingestão incremental dos ZIPs para Bronze
│ ├── 02_silver_transform.sql     # limpeza e cast para Silver
│ ├── 03_gold_publish.sql         # agregação final para Gold
│ ├── 04_stats_enhanced.py        # estatísticas e qualidade do Gold
│ └── run_pipeline.py             # roda toda a pipeline
├── src
│ └── ingestion.py                # função compartilhada
├── jobs
│ └── siape_monthly_job.json      # definição de Job no Databricks
└── README.md # este arquivo
```

---

## 4. Quando usar o "siape_monthly_job" e o "run_pipeline"

- Deve-se usar o **"run_pipeline.py"** para desenvolvimento local ou demos rápidas, em que rodando apenas um arquivo será executada a pipeline end-to-end.

- Deve-se usar o **"run_pipeline.json"** para rodar o serviço em ambiente de produção. O JSON possibilita manter o job com cada etapa como uma task separada. Isso garante que:
a - Tarefas individuais (config, bronze_ingest, silver_transform, gold_publish, statistics) aparecem separadamente no UI de Jobs → Runs, cada uma com seu próprio log, status e métricas de duração.
b - Se algo falhar, é possível saber exatamente em qual etapa.

---

## 5. Deploy do Projeto

**5.1. Importar o repositório**  
- 1. Clone ou importe este repositório para o seu workspace Databricks.  
- 2. Assegure que a pasta `/siape_pipeline` esteja visível em **Repos**.

**5.2. Pré-requisitos**  
Ter o Databricks CLI instalado e configurado com databricks configure --token.  
Definir as variáveis de ambiente:
```
export DATABRICKS_HOST=https://<seu-workspace>
export DATABRICKS_TOKEN=<seu-token>
```

**5.3. Deploy inicial (criação do Job)**  
 Execute uma única vez, após commitar o JSON:
```
databricks jobs create --json-file jobs/siape_monthly_job.json
Isso retorna um job_id. Anote-o ou armazene em um Secret/variável de CI (SIAPE_JOB_ID).
```

**5.4. Atualizações futuras (reset do Job)**  
Sempre que alterar qualquer notebook ou a configuração do JSON, faça:
```
databricks jobs reset --job-id $SIAPE_JOB_ID --json-file jobs/siape_monthly_job.json
Isso mantém o mesmo job_id mas injeta suas mudanças no Job existente.
```

**5.5. Execução manual**  
Para disparar o pipeline “on demand” (por exemplo, em testes) use:
```
databricks jobs run-now --job-id $SIAPE_JOB_ID \
  --notebook-params '{"year":"2025","month":"05","names":"Servidores_SIAPE,Pensionistas_SIAPE,Aposentados_SIAPE"}'
```
Pode passar manualmente year, month e names conforme desejar.

**5.6. Agendamento automático**  
No próprio JSON já está definido:
```
  "schedule": {
    "quartz_cron_expression": "0 0 6 1 * ? *",
    "timezone_id": "America/Sao_Paulo"
  }
```
Isso faz com que, todo dia 1 às 06:00 (fuso São Paulo), o pipeline rode com os parâmetros atuais dos widgets (year, month, names).

**5.7. Monitoramento e Logs**  
No UI do Databricks, acesse Jobs & Pipelines → SIAPE pipeline mensal → Runs
Em cada execução você verá o status de cada task (config, bronze_ingest …) e os logs detalhados.

---

## 6. Rodar rapidamente para testes e demonstrações

**6.1. Importar o repositório**
1. Clone ou importe este repositório para o seu workspace Databricks.  
2. Assegure que a pasta `/siape_pipeline` esteja visível em **Repos**.

**6.2. Executar o run_pipeline.py**
1. Selecione o cluster e abra `notebooks/run_pipeline`.  
2. Clique em **Run All**.  
3. Valide:
```
SHOW DATABASES LIKE 'public_informations';
SELECT * FROM public_informations.schema_registry;
```

---
## 7. Incrementalidade e Governança
- **Time travel**: cada partição `reference_dt` armazena dados de cada mês M-2 e histórico.  
- **schema_registry**: rastreia criação de tabelas e versões.  
- **mergeSchema=true**: permite evolução controlada do schema Raw sem interrupções.  
- **source_file** e **siape_fonte**: garantem rastreabilidade de origem.

---

## 8. Referências
- Portal da Transparência - Dados Abertos: Planilhas  
  https://portaldatransparencia.gov.br/download-de-dados/servidores
- Portal da Transparência – Dicionário de Dados SIAPE Remuneração  
  https://portaldatransparencia.gov.br/dicionario-de-dados/servidores-remuneracao  
- Documentação Databricks  
  https://docs.databricks.com/  
- Delta Lake Guide  
  https://docs.databricks.com/delta/index.html  

## 9. Propriedade
Este projeto foi desenvolvido pelo **Eng. de Dados Noé Machado**.  
Data: 23 de jullho de 2025.  
Local: São Paulo - SP, Brasil.