from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType



def main():
    spark = SparkSession.builder \
                        .appName('Product Analysis') \
                        .config('spark.ui.port','4041') \
                        .getOrCreate()

    # cria o schema do dataframe
    schema = StructType([
        StructField('Product ID', IntegerType(), False),
        StructField('Product Name', StringType(), False),
        StructField('Category Label', StringType(), False),
        StructField('Price', FloatType(), True),
        StructField('Merchant ID', IntegerType(), False)
    ])

    # cria a lista dos dados
    products = [(1, 'IPhone', 'Mobile Phone', 3000.00, 1),
                (2, 'Samsung Galaxy','Mobile Phone', 1000.00, 2),
                (3, 'Xiaomi','Mobile Phone', 500.40, 3),
                (4, 'Dell Core Duo', 'Notebook', 2500.30, 4),
                (5, 'IPad X', 'Notebook', 1500.30, 1),
                ]
    
    # cria o Data Frame de produtos
    df = spark.createDataFrame(data=products, schema=schema)
    df.show()

    # cria o schema para dados de forncedores
    schema_merchant = StructType([
        StructField('Merchant ID', IntegerType(), False),
        StructField('Merchant Name', StringType(), False)
    ])

    # cria a lista de Fornecedores
    merchants = [
        (1, 'Apple Inc'),
        (2, 'Samsung'),
        (3, 'China Town'),
        (4, 'Dell')
    ]

    # cria o Data Frame de fornecedores
    df_merchant = spark.createDataFrame(data=merchants, schema=schema_merchant)
    df_merchant.show()

    # junta a lista de Fornecedores e Produtos
    df_joined = df.join(df_merchant, df['Merchant ID'] == df_merchant['Merchant ID']).orderBy(df['Product ID'])
    df_joined.show()

    # Ordena os preços decrescente
    df_price_desc = df_joined.orderBy(df_joined['Price'].desc())
    df_price_desc.show()


if __name__ == '__main__':
    main()