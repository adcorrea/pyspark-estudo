from pyspark.sql import SparkSession

# contar o numero de produtos em cada partição
def process_partition(iterator) -> any:     
    # Suponha que queremos processar os dados de alguma maneira
    # Aqui, apenas contamos os registros em cada partição
    count = 0
    for _ in iterator:
        count += 1

    yield count

def main() -> None:
    spark = SparkSession.builder \
                        .appName('Data Product') \
                        .config('spark.ui.port','4041') \
                        .getOrCreate()


    sc = spark.sparkContext

    file_path = './app/data/pricerunner_aggregate.csv'

    rdd = sc.textFile(file_path)

    # aplicando a função em cada partição do rdd criado a partir do CSV    
    partitions_counts = rdd.mapPartitions(process_partition).collect()

    print(partitions_counts) # retorno o numero de registros em cada partição

if __name__ == '__main__':
    main()