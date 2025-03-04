from pyspark.sql import SparkSession


# Iniciar a sessão Spark
spark = SparkSession.builder \
    .appName("Exemplo") \
    .getOrCreate()

# Definir o schema
schema = ('Id INT, Nome STRING')

# Dados a serem usados
dados = [(1, 'Pedro'),(2, 'Maria')]

# Criar o DataFrame
df1 = spark.createDataFrame(data=dados, schema=schema)

# Mostrar o DataFrame
df1.show()
