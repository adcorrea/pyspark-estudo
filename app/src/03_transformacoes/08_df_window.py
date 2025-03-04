from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


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

    # Especificação da janela para classificação dentro de cada categoria com base no Título do prouto
    window_spec = Window.partitionBy('Category Label').orderBy('Product Title')

    # adicionando uma coluna Rank para classificar os produtos dentro de cada categoria
    df_with_rank = df_clean.withColumn('Rank', rank().over(window_spec))

    df_with_rank.show()


if __name__ == '__main__':
    main()