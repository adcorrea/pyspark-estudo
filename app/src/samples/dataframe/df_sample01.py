from pyspark.sql import SparkSession


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


# Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Exemplo") \
    .getOrCreate()

# Definir o schema
schema = StructType([
    StructField("Id", IntegerType(), True),
    StructField("Nome", StringType(), True)
])

# Dados a serem usados
dados = [(1, 'Pedro'),(2, 'Maria')]

# Criar o DataFrame
df1 = spark.createDataFrame(data=dados, schema=schema)

# Mostrar o DataFrame
df1.show()
