from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, FloatType, StringType, IntegerType

def main():

    spark = SparkSession.builder.appName('spark') \
                                .config('spark.ui.port', '4041') \
                                .getOrCreate()
    
    schema = StructType([StructField('id', IntegerType(), False)
                          ,StructField('produto', StringType(), False)
                          ,StructField('quantidade', IntegerType(), True)
                          ,StructField('valor', FloatType(), True)])
    
    produtos = [(1, 'iPhone', 15, 15000.00)
                ,(2, 'iPad', 3, 3500.00)
                ,(3, 'iWatch', 4, 5800.00)
                ,(4, 'iBook', 10, 9000.00)]
    
    vendas_df = spark.createDataFrame(data=produtos, schema=schema)


    # Calculando o Total de vendas e salvando o resultado
    total_vendas = vendas_df.groupBy('produto').sum('quantidade').cache()
    total_vendas.show()

    total_vendas.write.csv('./app/src/04_acoes/output/total_vendas')


if __name__ == '__main__':
    main()
    
    