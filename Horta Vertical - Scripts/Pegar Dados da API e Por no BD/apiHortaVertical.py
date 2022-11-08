# coding: utf-8

# In[43]:


import pandas as pd
import datetime
import pymssql
import pytz


dataNow = datetime.datetime.now()

chave = "QFYXMOTRJ6KXCXO1" #chave de acesso aos dados do ThingSpeak

url = "https://api.thingspeak.com/channels/1242885/feeds.json?api_key="+chave+"&results="
#url = "https://api.thingspeak.com/channels/685459/feeds.json?api_key="+chave+"&results=" #url ThingSpeak Usei esse de modelo

leitura = pd.read_json(url, typ = "series") #leitura da serie de dados da url 
leitura = pd.DataFrame(leitura[1]) #tranformação serie de dados em dataframe
leitura = leitura.drop("entry_id", axis = 1) #exclusão coluna indesejada
leitura.columns = ['Data','v1'] #renomeação das colunas
leitura = leitura.sort_values(by=['Data'], axis = 0, ascending = False) #ordenação dos dados pela data
leitura['Data'] = pd.to_datetime(leitura['Data'], infer_datetime_format = True) #formatação da coluna data para datetime

leitura['Data'] = leitura['Data'].dt.tz_localize('UTC') #formatação da data para UTC
#leitura['Data'] = leitura['Data'].dt.tz_convert('America/Sao_Paulo') #formatação da data para UTC-3
leitura['Data'] = leitura['Data'].dt.tz_convert('America/Cuiaba')
leitura['Data'] = leitura['Data'].dt.strftime('%Y-%m-%d %H:%M:%S') #formatação para o modo ano-mes-dia hora-minuto-segundo
leitura['Data'] = pd.to_datetime(leitura['Data'], infer_datetime_format = True) #formatação da coluna data para datetime

leitura = leitura.reset_index(drop=True) #reset do index

leitura.head()

print(leitura)


# In[40]:


#Conectando ao SQL SERVER
#connection = pymssql.connect(server='PISERVER\SQLEXPRESS', user='PISERVER\Administrator', password='Smart2017!', database='clima')
connection = pymssql.connect(server='172.16.230.10\SQLEXPRESS', user='smartcampus', password='Smart2017!', database='clima')
consulta = connection.cursor()

#Consultando a tabela yamatec
consulta = connection.cursor() 
consulta.execute('SELECT [data],[valvula] FROM [clima].[dbo].[horta_vertical] ORDER BY data DESC')

#Atribuindo os valores consultados a um vetor
row = consulta.fetchone() 

if row is None:
    for x in range(len(leitura)):

        consulta.execute("INSERT INTO [clima].[dbo].[horta_vertical] VALUES ('%s','%s')"%(leitura.loc[x,'Data'],leitura.loc[x,'v1']))
        connection.commit()

dataBanco = row[0]

if dataBanco < leitura.loc[0,'Data']:
        
    dadoNovo = (leitura['Data'] > dataBanco) & (leitura['Data'] <= leitura.loc[0,'Data']) #definição range dos dados novos, o resultado
    # é um vetor de verdadeiro e falso.
    dadoNovo = leitura.loc[dadoNovo]
    #quando passado o vetor de verdadeiro e falso para uma matriz, o resultado são os valores da matriz apenas para as linhas com verdadeiro 
    dadoNovo = dadoNovo.sort_values(by=['Data'], axis = 0, ascending = False) #ordenação dos dados pela data
    dadoNovo = dadoNovo.reset_index(drop=True) #reset do index
        
    for x in range(len(dadoNovo)):
        consulta.execute("INSERT INTO [clima].[dbo].[horta_vertical] VALUES ('%s','%s')"%(leitura.loc[x,'Data'],leitura.loc[x,'v1']))
        connection.commit()
        
connection.close()
