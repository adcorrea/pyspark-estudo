from pyspark.sql.types import *
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("Exemplo") \
    .getOrCreate()


arqschema = "id INT, nome STRING, status STRING, Cidade STRING, vendas INT, data STRING"

despachantes = spark.read.csv('./app/data/despachantes.csv'
                              , header=False
                              ,schema=arqschema)

despachantes.show()


desp_autoschema = spark.read.load('./app/data/despachantes.csv'
                              , header=False
                              ,format='csv'
                              ,sep=','
                              ,inferSchema=True)

desp_autoschema.show()