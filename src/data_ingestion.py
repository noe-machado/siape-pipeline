from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import lit, to_date, when, current_timestamp
import zipfile, io, requests, pandas as pd, unicodedata, re

# Base URL e path template importáveis
BASE_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores"


def download_and_extract_csv(
    year: str,
    month: str,
    dataset: str,
    schema: StructType,
    encoding: str = "latin1",
    sep: str = ";"
) -> DataFrame:
    """
    Baixa o ZIP do SIAPE, extrai o CSV em memória,
    sanitiza cabeçalhos, normaliza valores e retorna
    um Spark DataFrame tipado conforme schema, com metadata.
    """
    spark = SparkSession.builder.getOrCreate()

    # STEP 1: construir URL
    url = f"{BASE_URL}/{year}{month}_{dataset}.zip"
    print(f"[INFO] Downloading: {url}")

    # STEP 2: download
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    # STEP 3: extrair CSV
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        # encontra o arquivo de remuneração
        csv_file = next(
            (f for f in z.namelist()
             if "remuneracao" in unicodedata.normalize('NFKD', f)
                                   .encode('ascii','ignore')
                                   .decode().lower()),
            None
        )
        if not csv_file:
            raise FileNotFoundError(f"Sem arquivo de remuneração em {url}")
        with z.open(csv_file) as f:
            pdf = pd.read_csv(f, sep=sep, encoding=encoding, low_memory=False)

    # STEP 4: sanitizar cabeçalhos
    def _sanitize(col: str) -> str:
        tmp = col.replace("R$", "RS").replace("U$", "US")
        no_acc = unicodedata.normalize("NFKD", tmp).encode("ascii","ignore").decode()
        cleaned = re.sub(r"[^0-9A-Za-z]+", "_", no_acc).strip("_")
        return cleaned.lower()

    pdf.rename(columns={c: _sanitize(c) for c in pdf.columns}, inplace=True)

    # STEP 5: normalizar valores
    for field in schema:
        col_name = field.name
        dt = field.dataType
        if col_name not in pdf.columns:
            continue
        if isinstance(dt, (IntegerType, DoubleType)):
            s = (pdf[col_name].astype(str)
                     .str.replace(r"[^\d\.\-,]", "", regex=True)
                     .str.replace(r"\.(?=\d{3}(?:[.,]|$))", "", regex=True)
                     .str.replace(",", "."))
            pdf[col_name] = pd.to_numeric(s, errors="coerce")
        elif isinstance(dt, StringType):
            pdf[col_name] = pdf[col_name].fillna("").astype(str)
        elif isinstance(dt, DateType):
            pdf[col_name] = pd.to_datetime(pdf[col_name], errors="coerce")

    # STEP 6: substituir NaN/NA por None
    pdf = pdf.where(pd.notnull(pdf), None)

    # STEP 7: converter para lista de dicts e coerir inteiros
    raw = pdf.to_dict(orient="records")
    records = []
    for r in raw:
        rec = {}
        for field in schema:
            val = r.get(field.name)
            if isinstance(field.dataType, IntegerType):
                try:
                    rec[field.name] = int(val) if val is not None else None
                except:
                    rec[field.name] = None
            else:
                rec[field.name] = val
        records.append(rec)

    # STEP 8: Spark DataFrame
    sdf = spark.createDataFrame(records, schema=schema)

    # STEP 9: metadata
    sdf = (sdf
           .withColumn("siape_fonte",
               when(lit(dataset)=="Servidores_SIAPE","servidor")
              .when(lit(dataset)=="Pensionistas_SIAPE","pensionista")
              .when(lit(dataset)=="Aposentados_SIAPE","aposentado")
              .otherwise(lit(dataset)))
           .withColumn("source_file", lit(f"{year}{month}_{dataset}.zip"))
           .withColumn("data_ingestao", current_timestamp()))

    # STEP 10: reference_dt
    ref = f"{year}-{month.zfill(2)}"
    sdf = sdf.withColumn("reference_dt", to_date(lit(ref)))

    cnt = sdf.count()
    print(f"[INFO] {dataset}: {cnt} registros para {ref}")
    return sdf