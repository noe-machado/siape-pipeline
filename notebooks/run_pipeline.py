# run_pipeline.py

timeout = 1800  # 1800 segundos (30 minutos)

print(f"[INFO] Executando 00_config.py")
dbutils.notebook.run("./00_config.py", timeout, {})
print(f"[INFO] Executando 01_bronze_ingest.py")
dbutils.notebook.run("./01_bronze_ingest.py", timeout, {})
print(f"[INFO] Executando 02_silver_transform.sql")
dbutils.notebook.run("./02_silver_transform.sql", timeout, {})
print(f"[INFO] Executando 03_gold_publish.sql")
dbutils.notebook.run("./03_gold_publish.sql", timeout, {})
print(f"[INFO] Executando 04_stats_enhanced.py")
dbutils.notebook.run("./04_stats_enhanced.py",   timeout, {})

print(f"A execução da pipeline foi concluída com sucesso!")
