import os
from pyspark.sql import SparkSession


from pyspark.sql import SparkSession
from pyspark.sql import functions as Func


os.environ['PYSPARK_PYTHON'] = './venv/scripts/python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = './venv/scripts/python.exe'

spark = SparkSession.builder \
    .appName("Exemplo") \
    .getOrCreate()

arqschema = "id INT, nome STRING, status STRING, Cidade STRING, vendas INT, data STRING"

despachantes = spark.read.csv('./app/data/despachantes.csv'
                              , header=False
                              ,schema=arqschema)


despachantes.select('id', 'nome', 'vendas').where(Func.col('vendas') > 20 ).show()
