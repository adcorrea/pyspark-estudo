from pyspark.sql import SparkSession

# Cria uma Sessão Spark para uso de DataFrame
spark = SparkSession.builder \
            .appName('ProductDataAnalysis') \
            .config('spark.ui.port', '4041') \
            .getOrCreate()

file_path = './app/data/pricerunner_aggregate.csv'

# Le um arquivo CSV e converte em DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Mostra as primeiras 20 linhas do conjunto de dados
df.show()

# Mostra o schema do DataFrame
df.printSchema()

# Mostra um resumo estatístico
df.describe().show()