from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
                    .appName('ProductAnalysis') \
                    .config('spark.ui.port', '4041') \
                    .getOrCreate()


path_file = './app/data/pricerunner_aggregate.csv'

df = spark.read.csv(path_file
                    , header = True
                    , inferSchema = True)

#df.show()

#Mostrando o schema da tabela
df.printSchema()

# verificando valores Nulos
df.select([col(c).isNull().alias(c) for c in df.columns]).show()


# exclusão de linhas nulas
df_clean = df.na.drop()

print(f'linhas originais: {df.count()}')
print(f'linhas atualizadas: {df_clean.count()}')

