
#Instalação do Spark 3.5.3 (Docker)
    docker pull spark

    #Inicialização do spark
    docker run -it -p 4040:4040 spark:latest /opt/spark/bin/pyspark


#VERSÕES

    PYTHON 3.9
    Spark 3.5.2
    Java JDK 17
    Hadoop 3.0.0

#HADOOP

    BAIXAR O HADOOP DO REPOSITÓRIO: https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin
    LINK DE ORIENTAÇÕES DE CONFIGURAÇÃO: https://stackoverflow.com/questions/68509434/i-can-read-from-local-file-in-py-spark-but-i-cant-write-data-frame-in-local-fil/74482843#74482843
    Deve-se seguir essas orientações, ou comandos de persist do pyspark começaram a dar erro de UnsatisfiedLinkError

#CONFIGURAÇÃO
    Configurar as variaveis de ambientes:

        SPARK_HOME=[DIRETORIO_SISTEMA]\[DIRETORIO_SPARK]
        HADOOP_HOME=[DIRETORIO_SISTEMA]\[DIRETORIO_HADDOP]
        PYSPARK_PYTHON=[DIRETORIO_INSTALACAO_PYTHON]\python.exe
        PYSPARK_DRIVER_PYTHON=[DIRETORIO_INSTALACAO_PYTHON]\python.exe
        JAVA_HOME=[DIRETORIO_INSTALACAO_JAVA]

        PATH
            %SPARK_HOME%\bin
            %SPARK_HOME%\sbin
            %HADOOP_HOME%\bin


