import config
from app.utils.sparkutils import inicializa_spark, retorna_df_bolsa_familia


def main():
    spark = inicializa_spark()

    df = retorna_df_bolsa_familia(spark)

    # filtrando o registros onde o CPF segue um padrão de 11 digitos
    df_filter = df.filter(df['cpf_favorecido'].rlike('^\d{11}$'))
    df_filter.show()

    # Filtrando registros que nome contenha Antonio
    df_filter = df.filter(df['nome_favorecido'].rlike('Antonio'))
    df_filter.show()

if __name__ == '__main__':
    main()