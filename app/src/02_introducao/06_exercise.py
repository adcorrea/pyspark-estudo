from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import sum

spark = SparkSession.builder.appName('app Exercise') \
                            .config('spark.ui.port', '4041') \
                            .getOrCreate()


schema = StructType([
    StructField("Product ID", IntegerType(), True),
    StructField("Vendas", StringType(), True),
    StructField("Data Venda", StringType(), True)
])


dados = [(1, 1, '20/12/2023'),(2, 2, '21/12/2023'),(1, 5, '24/12/2023'), (2, 1, '26/12/2023')]

df = spark.createDataFrame(data=dados, schema=schema)

df.show()

dados_sumario = df.groupBy('Product ID').agg(sum('Vendas').alias('Vendas Totais'))

dados_sumario.show()

