# -*- coding: utf-8 -*-
"""
Created on Wed Aug 16 16:32:24 2023

@author: 2018459
"""


#%% Bibliotecas
import pandas as pd
import numpy as np
import keyring
import cx_Oracle
import os
import glob
import math


#%% Leitura dos arquivos e consolidação
def leitura_arquivo(arquivo):
    global df
    df = pd.read_csv(os.path.join(pasta,arquivo),sep=';',decimal=',',encoding='ANSI',dtype={'NumCPFCNPJ':'str'})


#%% Tratamento dos dados
def tratamento_dados():
    global df
    df = df.astype('str')
    for coluna in df.columns:
        df[coluna] = df[coluna].replace('nan',None)
        
    # Remove os espaços em branco no começo e no final
    df[df.columns] = df.apply(lambda x: x.str.strip())
    
        
    # Define as colunas como 'int'  
    df['NumNivelTensao'] = df['NumNivelTensao'].astype('int')
    df['NumUnidadeConsumidora'] = df['NumUnidadeConsumidora'].astype('int')
    df['NumUnidadeConsumidora'] = df['NumUnidadeConsumidora'].astype('int')


#%% Inserir dados no banco de dados
def carga_banco(df,tabela_oracle,aplicacao_usuario,aplicacao_senha,aplicacao_dsn,usuario):
    #Criar a lista para inserção no banco SQL com os dados da base editada
    dados_list = df.values.tolist()

    # Fazemos o insert em blocos, pois temos muitas linhas
    start_pos = 0
    batch_size = 50000

    #Definir conexão com o banco de dados     
    try:
        connection = cx_Oracle.connect(user = keyring.get_password(aplicacao_usuario, usuario),
                                       password = keyring.get_password(aplicacao_senha,usuario),
                                       dsn= keyring.get_password(aplicacao_dsn, usuario),
                                       encoding="UTF-8")
    
    #Se der erro na conexão com o banco, irá aparecer a mensagem abaixo
    except Exception as err:
        print('Erro na Conexao:', err)    
    
    #Se estiver tudo certo na conexão, irá aparecer a mensagem abaixo
    else:
        print('Conexao com o Banco de Dados efetuada com sucesso. Versao da conexao: ' + connection.version)
        
        #O cursor abaixo irá executar o insert de cada uma das linhas da base editada no Banco de Dados Oracle
        try:
            cursor = connection.cursor()
            cursor.execute('''TRUNCATE TABLE ''' + tabela_oracle) #Limpa os dados da tabela
            sql = '''INSERT INTO ''' + tabela_oracle +''' VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)''' #Deve ser igual ao número de colunas da tabela do banco de dados
            i = 0
            while start_pos < len(dados_list):
                data = dados_list[start_pos:start_pos + batch_size]
                start_pos += batch_size
                cursor.executemany(sql, data) 
                connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
                i += 1
                print(i)
            
        except Exception as err:
            print('Erro no Insert:', err)
        else:
            print('Carga executada com sucesso!')
            connection.commit() #Caso seja executado com sucesso, esse comando salva a base de dados
        finally:
            cursor.close()
            connection.close()
            

#%% Rodamos as funções
# Caminho referencia
pasta = r"W:\Inteligência Regulatória Analítica - IRA\2. Projetos\2024\Interrupções de Energia Elétrica\Dados"

# Anos
anos = ['2017','2018','2019','2020','2021','2022','2023']

#Definir as variáveis para conexão no banco de dados
aplicacao_usuario = "USER_IRA"
aplicacao_senha = "BD_IRA"
aplicacao_dsn = "DSN"
usuario = "IRA"

for ano in anos:
    arquivo = rf'interrupcoes-energia-eletrica-{ano}.csv'
    tabela_oracle = f'INTERRUPCOES_ANEEL_{ano}'
    
    leitura_arquivo(arquivo)
    print(f'Leu o arquivo: {arquivo}')
    
    tratamento_dados()
    print('Tratou os dados!')
    
    carga_banco(df,tabela_oracle,aplicacao_usuario,aplicacao_senha,aplicacao_dsn,usuario)





