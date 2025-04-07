from io import StringIO
import shutil
import pandas as pd
import pyarrow.parquet as pq
import os
from datetime import datetime


def converte_arquivo_para_utf_8(nome_arquivo):
    try:
        with open(nome_arquivo, mode="r", encoding="utf-8") as f:
            arquivo = f.read()
        return arquivo
    except UnicodeDecodeError:
        pass

    with open(nome_arquivo, mode="rb") as f:
        arquivo_bin = f.read()
        try:
            arquivo = arquivo_bin.decode('iso-8859-15')
        except UnicodeDecodeError as e:
            print('Erro ao decodificar o arquivo', exc_info=e)
            raise ValueError('Erro ao decodificar o arquivo')

    arquivo_utf8 = arquivo.encode('utf-8')

    with open(nome_arquivo, mode="wb") as f:
        f.write(arquivo_utf8)

    return arquivo_utf8.decode('utf-8')


def read_arquivo_para_pandas(nome_arquivo, dtype='str'):
    conteudo_arquivo = converte_arquivo_para_utf_8(nome_arquivo)
    df = pd.read_csv(
        StringIO(conteudo_arquivo),
        delimiter=";",
        encoding="utf-8",
        dtype=dtype,
    )
    df = df.fillna('')
    df = df.astype(str)
    df = df.map(lambda x: x.strip())

    return df


def cria_arquivo_csv_e_parquet(nome_arquivo):
    diretorio_saida = 'output'
    if os.path.exists(diretorio_saida):
        shutil.rmtree(diretorio_saida)

    parcelas_df = read_arquivo_para_pandas(nome_arquivo)
    parcelas_df_aumentado = pd.concat([parcelas_df] * 50, ignore_index=True)

    del parcelas_df

    os.makedirs('output', exist_ok=True)

    parcelas_df_aumentado.to_csv(
        'output/parcelas.csv', index=False, sep=';'
    )
    parcelas_df_aumentado.to_parquet(
        'output/parcelas.parquet', index=False, engine='pyarrow'
    )


def read_arquivo_parquet(nome_arquivo):
    chunk_size = 100_000

    parquet_arquivo = pq.ParquetFile(nome_arquivo)
    total_linhas = parquet_arquivo.metadata.num_rows

    for batch in parquet_arquivo.iter_batches(batch_size=chunk_size):
        start = datetime.now()

        df = batch.to_pandas()
        for _ in df.itertuples(index=False):
            pass

        print(f'Faltam {total_linhas - len(df)} linhas')
        total_linhas -= len(df)

        del df

        end = datetime.now()
        print(
            f'Tempo para cada loop: {(end - start).total_seconds()} segundos'
        )


if __name__ == "__main__":
    cria_arquivo_csv_e_parquet('parcelas.csv')
    read_arquivo_parquet('output/parcelas.parquet')
