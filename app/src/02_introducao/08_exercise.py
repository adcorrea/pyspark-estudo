from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType

spark = SparkSession.builder.appName('Union App') \
                            .config('spark.ui.port', '4041') \
                            .getOrCreate()


schema_produto = StructType([StructField('Product ID', IntegerType(), False),
                             StructField('Description', StringType(), False),
                             StructField('Price',FloatType(), True)])

schema_estoque = StructType([StructField('Product ID', IntegerType(), False),
                             StructField('Quantidade', IntegerType(), True)])

produtos = [(1, 'IPhone', 10000.0), (2, 'Notebook Dell', 7500.0)]

estoque =  [(1, 10), (2, 3)]

df_produtos = spark.createDataFrame(data=produtos, schema=schema_produto)
df_estoque = spark.createDataFrame(data=estoque, schema=schema_estoque)

df_produtos.show()
df_estoque.show()

df_combinado = df_produtos.join(df_estoque, df_produtos['Product ID'] == df_estoque['Product ID'])
df_combinado.show()

