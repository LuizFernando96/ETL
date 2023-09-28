import requests
import pandas as pd

#Baixando a tabela SalesOrderHeaders
url = 'https://demodata.grapecity.com/adventureworks/api/v1/salesOrders?PageSize=500'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\SalesOrderFact.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\SalesOrderFact.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = ['comment' , 'revisionNumber' , 'isOrderedOnline' ,'modifiedDate' , 'shipMethod' , 'salesPersonId' , 'customerId']

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('SalesOrderFact.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela ShipMethods
url = 'https://demodata.grapecity.com/adventureworks/api/v1/shippingMethods'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\ShipMethods.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\ShipMethods.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = 'modifiedDate'

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('ShipMethods.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela SalesTerritory
url = 'https://demodata.grapecity.com/adventureworks/api/v1/salesTerritories'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\SalesTerritory.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\SalesTerritory.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = ['modifiedDate' , 'costYtd', 'costLastYear']

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('SalesTerritory.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela Customers
url = 'https://demodata.grapecity.com/adventureworks/api/v1/customers?PageSize=500'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\Customers.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\Customers.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = ['customerId' , 'storeId' ,'modifiedDate', 'personId']

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('Customers.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela CreditCard 
url = 'https://demodata.grapecity.com/adventureworks/api/v1/creditCards?PageSize=500'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\CreditCard.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\CreditCard.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = 'modifiedDate'

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('CreditCard.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela Address
url = 'https://demodata.grapecity.com/adventureworks/api/v1/addresses?PageSize=500'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\Address.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\Address.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = ['state' , 'country' ,'modifiedDate']

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('Address.csv', index=False)  # index=False evita a escrita do índice no arquivo

#Baixando a tabela CurrencyRate
url = 'https://demodata.grapecity.com/adventureworks/api/v1/currencyRates?PageSize=500'

requisicao = requests.get(url)

informacao = requisicao.json()

df = pd.DataFrame(informacao)

caminho_arquivo = 'E:\Luiz\ETL\JamesVirtual2\CurrencyRate.csv'

df.to_csv(caminho_arquivo, index=False)

#Carregando arquivo no dataframe
df = pd.read_csv('E:\Luiz\ETL\JamesVirtual2\CurrencyRate.csv')

#Especificando coluna a ser excluída
coluna_para_excluir = 'modifiedDate'

#Removendo coluna
df = df.drop(coluna_para_excluir, axis=1)

#Salvando
df.to_csv('CurrencyRate.csv', index=False)  # index=False evita a escrita do índice no arquivo