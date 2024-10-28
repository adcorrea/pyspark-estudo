# pyspark-estudo


# Instalação do Spark 3.5.3 (Docker)
docker pull spark

Inicialização do spark
docker run -it -p 4040:4040 spark:latest /opt/spark/bin/pyspark


# Configuração do ambiente

PYTHON 3.9

Spark 3.5.2

Java JDK 17

Configurar as variaveis de ambientes:

SPARK_HOME=[DIRETORIO_SISTEMA]\[DIRETORIO_SPARK]

HADOOP_HOME=[DIRETORIO_SISTEMA]\[DIRETORIO_SPARK]

PYSPARK_PYTHON=[DIRETORIO_INSTALACAO_PYTHON]\python.exe

PYSPARK_DRIVER_PYTHON=[DIRETORIO_INSTALACAO_PYTHON]\python.exe

JAVA_HOME=[DIRETORIO_INSTALACAO_JAVA]

PATH

    %SPARK_HOME%\bin

    %SPARK_HOME%\sbin
    
