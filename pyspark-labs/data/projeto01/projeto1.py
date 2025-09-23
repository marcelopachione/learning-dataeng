# Projeto 1 --> Pipeline PySpark para extrair, transformar e carregar arquivos json para o banco de dados (SQL Lite)

# Imports
import os
from pyspark.sql import SparkSession, types
from pyspark.sql.functions import col, regexp_replace

# Inicia Sessao
spark = SparkSession.builder \
    .appName("Projeto1") \
    .getOrCreate()

schema = types.StructType([
    types.StructField("nome", types.StringType(), True),
    types.StructField("idade", types.IntegerType(), True),
    types.StructField("email", types.StringType(), True),
    types.StructField("salario", types.IntegerType(), True),
    types.StructField("cidade", types.StringType(), True)
])

# Carrega json com schema definido
df = spark.read.schema(schema).json("data/projeto01/usuarios.json")

# Drop da coluna email (filtro a nivel de coluna)
df_drop_col_email = df.drop("email")

# Filtra os dados (filtro a nivel linha)

df = df_drop_col_email.filter(
    (col("idade") > 35) &
    (col("cidade") == "Sao Paulo") &
    (col("salario") < 7000)
)

# Verfica o schema e os dados
df.printSchema()
df.show()

# Verifica se o DataFrame está vazio ou não
if df.rdd.isEmpty():
    print("Nenhum dado encontrado no arquivo Json. Verifique o formato do arquivo.")
else:
    # Limpa os dados removendo '@' (se existir) da coluna nome
    df_clean = df.withColumn("Nome", regexp_replace(col("nome"), "@", ""))

    # Define o path da base sqlite
    sqlite_db_path = os.path.abspath("data/projeto01/usuarios.db")

    # Define a ROL de conexao JDBC
    sqlite_uri = "jdbc:sqlite://" + sqlite_db_path

    # Define o driver JDBC
    properties = {"driver": "org.sqlite.JDBC"}

    # Verifica se a tabelas usuarios existe e define o modo de grava
    try:
        spark.read.jdbc(url=sqlite_uri, table="usuarios", properties=properties)
        write_mode = "append"
    except:
        # Se a tabela nao existir, o modo de gravacao sera 'overwirte'
        write_mode = "overwrite"

    # Grava os dados no banco SQLite
    df_clean.write.jdbc(url=sqlite_uri, table="usuarios", mode=write_mode, properties=properties)

    print(f"Dados gravados com sucesso no banco de dado SQLite em 'usuarios.db' usnado o mode '{write_mode}'")

####

# docker exec pyspark-master \
#     spark-submit \
#     --jars ./apps/jars/sqlite-jdbc-3.44.1.0.jar \
#     --deploy-mode client ./data/projeto01/projeto1.py