from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
                    .appName('Product Analysis') \
                    .config('spark.ui.port', '4041') \
                    .getOrCreate()


path_file = './app/data/pricerunner_aggregate.csv'

df = spark.read.csv(path_file, header=True, inferSchema=True)


# exclusão de linhas nulas
df_clean = df.na.drop()

# Lista de nomes de colunas conforme o esquema com espaços
col_names = df_clean.schema.names

# Renomear coluans para remover espaços iniciais
for col_name in col_names:
    new_col_name = col_name.strip()
    df_clean = df_clean.withColumnRenamed(col_name, new_col_name)


# Verificando o novo esquma
df_clean.printSchema()

# Converte string para inteiro
df_clean = df_clean.withColumn('Merchant ID', df_clean['Merchant ID'].cast('integer'))


df_clean.printSchema()