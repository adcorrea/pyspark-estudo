from pyspark.sql import SparkSession
from pyspark.sql.functions import count


def main():
    spark = SparkSession.builder \
                    .appName('Product Analysis') \
                    .config('spark.ui.port','4041') \
                    .getOrCreate()

    path_file = './app/data/pricerunner_aggregate.csv'
    df = spark.read.csv(path_file, header=True, inferSchema=True)

    df_clean = df.na.drop()

    columns = df_clean.schema.names

    for col in columns:
        new_col = col.strip()
        df_clean = df_clean.withColumnRenamed(col, new_col)

    # filtrar os produtos da category Mobile Phones
    df_filtered = df_clean.select('Product Title', 'Category Label').where(df_clean['Category Label'] == 'Mobile Phones')

    df_filtered.show()

    # agrupa os produtos por category e contas os produtos em cada uma delas
    df_aggrgate = df_clean.groupBy('Category Label').agg(count('Product ID').alias('Total Product in Category'))

    df_aggrgate.show()

if __name__ == '__main__':
    main()

