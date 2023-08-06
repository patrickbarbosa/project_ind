import mysql.connector
import pandas as pd
import numpy as np

#Criar conexão

def conectar(db):
    config = {
        'user': 'patrick',
        'password': 'root',
        'host': '127.0.0.1',
        'database': db
    }
    try:
            conexao = mysql.connector.connect(**config)
            print('Conexão estabelecida com sucesso.')
            return conexao
    except mysql.connector.Error as erro:
            print(f'Erro ao conectar ao MySQL: {erro}')
            return None
        

def executar_consulta(conexao, consulta):
    try:
        cursor = conexao.cursor()
        cursor.execute(consulta)
        resultado = cursor.fetchall()
        colunas = cursor.column_names
        cursor.close()
        df = pd.DataFrame(resultado, columns=colunas)
        conexao.commit()
        return df
    except mysql.connector.Error as erro:
        print(f"Erro ao executar consulta SQL: {erro}")
        return None


def get_column_types(dataframe):
    column_types = []

    for column_name, dtype in dataframe.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            column_types.append('INT')
        elif pd.api.types.is_float_dtype(dtype):
            column_types.append('DOUBLE')
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            column_types.append('DATETIME')
        else:
            max_length = dataframe[column_name].apply(lambda x: len(str(x))).max()
            if max_length <= 255:
                column_types.append('VARCHAR(255)')
            elif max_length <= 65535:
                column_types.append('TEXT')
            else:
                column_types.append('LONGTEXT')
    
    return column_types



# def inserir_dados(conexao, tabela, dataframe):
#     cursor = conexao.cursor()

#     # Verifica se a tabela já existe
#     cursor.execute(f"SHOW TABLES LIKE '{tabela}'")
#     tabela_existe = cursor.fetchone()

#     if tabela_existe:
#         print(f"A tabela '{tabela}' já existe.")
#     else:
#         # Caso a tabela não exista, cria a tabela
#         colunas = dataframe.columns
#         tipos_de_dados = get_column_types(dataframe)
#         colunas_def = [f"`{col}` {tipo}" for col, tipo in zip(colunas, tipos_de_dados)]
#         sql = f"CREATE TABLE {tabela} ({', '.join(colunas_def)})"
#         cursor.execute(sql)
#         print(f"Tabela '{tabela}' criada com sucesso.")

#         placeholders = ', '.join(['%s'] * len(colunas))
#         sql_insert = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"

#         for index, row in dataframe.iterrows():
#             converted_row = []
#             for value, tipo in zip(row, tipos_de_dados):
#                 if pd.api.types.is_integer_dtype(tipo):
#                     converted_row.append(int(value))
#                 elif pd.api.types.is_float_dtype(tipo):
#                     converted_row.append(float(value))
#                 elif pd.api.types.is_datetime64_any_dtype(tipo):
#                     converted_row.append(value)
#                 else:
#                     converted_row.append(str(value))
#             cursor.execute(sql_insert, tuple(converted_row))

            

#         conexao.commit()
#         print(f"Dados inseridos na tabela '{tabela}'.")

#         cursor.close()




def inserir_dados(conexao, tabela, dataframe):
    cursor = conexao.cursor()

    # Verifica se a tabela já existe
    cursor.execute(f"SHOW TABLES LIKE '{tabela}'")
    tabela_existe = cursor.fetchone()

    if tabela_existe:
        print(f"A tabela '{tabela}' já existe.")
    else:
        # Caso a tabela não exista, cria a tabela
        colunas = dataframe.columns
        tipos_de_dados = get_column_types(dataframe)
        colunas_def = [f"`{col}` {tipo}" for col, tipo in zip(colunas, tipos_de_dados)]
        sql = f"CREATE TABLE {tabela} ({', '.join(colunas_def)})"
        cursor.execute(sql)
        print(f"Tabela '{tabela}' criada com sucesso.")

        placeholders = ', '.join(['%s'] * len(colunas))
        sql_insert = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"

        for index, row in dataframe.iterrows():
            converted_row = []
            for value, tipo in zip(row, tipos_de_dados):
                if pd.api.types.is_integer_dtype(tipo):
                    converted_row.append(int(value))
                elif pd.api.types.is_float_dtype(tipo):
                    converted_row.append(float(value))
                elif pd.api.types.is_datetime64_any_dtype(tipo):
                    converted_row.append(value)
                else:
                    if pd.isna(value):  # Substitui NaN por NULL ao inserir na tabela
                        converted_row.append(None)
                    else:
                        converted_row.append(str(value))
            cursor.execute(sql_insert, tuple(converted_row))

        conexao.commit()
        print(f"Dados inseridos na tabela '{tabela}'.")

    cursor.close()

