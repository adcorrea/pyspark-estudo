from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, FloatType, IntegerType
from pyspark.sql.functions import col


spark = SparkSession.builder.appName('Imposto :p L') \
                            .config('spark.ui.port', '4041') \
                            .getOrCreate()


schema = StructType([
    StructField('Product ID', IntegerType(), True),
    StructField('Price', FloatType(), True)
])

dados = [(1, 10.10), (2, 15.0), (3, 10.0)]

df = spark.createDataFrame(data=dados, schema=schema)

df.show()


ajuste = df.withColumn('IVA', col('Price')*0.2 )

ajuste.show()