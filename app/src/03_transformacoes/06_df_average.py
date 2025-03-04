from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
from pyspark.sql.functions import avg


def main():
    spark = SparkSession.builder \
                        .appName('Product Analysis') \
                        .config('spark.ui.port','4041') \
                        .getOrCreate()

    # cria o schema do dataframe
    schema = StructType([
        StructField('Id', IntegerType(), False),
        StructField('Product Name', StringType(), False),
        StructField('Category Label', StringType(), False),
        StructField('Price', FloatType(), True)
    ])

    # cria a lista dos dados
    products = [(1, 'IPhone', 'Mobile Phone', 3000.00),
                (2, 'Samsung Galaxy','Mobile Phone', 1000.00),
                (3, 'Xiaomi','Mobile Phone', 500.40),
                (4, 'Dell Core Duo', 'Notebook', 2500.30),
                (5, 'IPad X', 'Notebook', 1500.30),
                ]

    # cria o dataframe
    df = spark.createDataFrame(data=products, schema=schema)

    df.show()

    # calcula o preço médio dos produtos por Category Label
    df_average_price = df.groupBy('Category Label').agg(avg('Price').alias('Average Price per Category'))
    df_average_price.show()

if __name__ == '__main__':
    main()


