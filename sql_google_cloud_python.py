import sqlalchemy
import pymysql

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

#Nos conectamos a la API de Google Sheets

name_sheet = 'your google sheets'

scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
#,'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"
creds = ServiceAccountCredentials.from_json_keyfile_name("yourdata.json", scope)
client = gspread.authorize(creds)
sheet = client.open(name_sheet).sheet1
data = sheet.get_all_records()
m = sheet.get_all_values()

columnas = m[0]
c = columnas

df = pd.DataFrame(m[1:], columns=c)

#todos los valores son obtenidos como Strings, debemos transformarlos a sus tipos correspondientes.

df.date = df['date'].astype('datetime64[ns]')

col = df.columns[2:10]

for cols in col:
    df[str(cols)] = df[str(cols)].astype('float64')
    

for cols in col:
    df[str(cols)] = df[str(cols)].astype('int')
    

#Hacemos un rensamble de la información por año y día. Generando una suma de los valores.
table_resample = df.copy()
table_resample.index = table_resample.date
del table_resample['date']

table_resample_by_year = table_resample.resample('Y').sum()

table_resample_by_moth = table_resample.resample('M').sum()


#Usamos la función de group para agrupar información por fuente.
table_groups = df.copy()

table_groups_by_source_mean = df.groupby(['source']).mean()

table_groups_by_source_sum = df.groupby(['source']).sum()

#Filtramos para crear dataframes con información solo de una fuente particular
table_source_google = table_groups[(table_groups.source == 'google') & (table_groups.revenue > 1)]
table_source_facebook = table_groups[(table_groups.source == 'facebook') & (table_groups.revenue > 1)]


#Nos conectamos a nuestra base de datos en Google Cloud MySQL
engine_str = 'mysql+pymysql://root:password@IPv24/demo_database'
engine = sqlalchemy.create_engine(engine_str, echo=False, encoding='utf-8')
connection = engine.connect()


#Creamos nuestras tablas e insertamos valores
query_dataframe = """ CREATE TABLE `demo_database`.`dataframe_full2` (
  `revenue` VARCHAR(100) NOT NULL,
  `users` VARCHAR(100) NOT NULL,
  `transaction` VARCHAR(100) NOT NULL,
  `investment` VARCHAR(100) NOT NULL,
  `date` DATETIME NOT NULL,
  `source` VARCHAR(10) NOT NULL); """
    
#Nuestros valores de ventas y transacciones son tan grandes que debemos guardarlos como VARCHAR
insert_query = """INSERT INTO demo_database.dataframe_full2 (revenue, users, transaction, investment, date, source)
VALUES(%s, %s, %s, %s, %s, %s)"""

connection.execute(query_dataframe)

for i in range(len(df)):
    connection.execute(insert_query, (str(df.revenue[i]),
                                      str(df.users[i]),
                                      str(df.transactions[i]),
                                      str(df.cost[i]),
                                      str(df.date[i]),
                                      str(df.source[i])))

query_dataframe_month = """ CREATE TABLE `demo_database`.`table_resample_by_moth` (
  `revenue` VARCHAR(100) NOT NULL,
  `users` VARCHAR(100) NOT NULL,
  `transaction` VARCHAR(100) NOT NULL,
  `investment` VARCHAR(100) NOT NULL,
  `date` DATETIME NOT NULL); """    

connection.execute(query_dataframe_month)

insert_query = """INSERT INTO demo_database.table_resample_by_moth (revenue, users, transaction, investment, date)
VALUES(%s, %s, %s, %s, %s)"""

for i in range(len(table_resample_by_moth)):
    connection.execute(insert_query, (str(table_resample_by_moth.revenue[i]),
                                      str(table_resample_by_moth.users[i]),
                                      str(table_resample_by_moth.transactions[i]),
                                      str(table_resample_by_moth.cost[i]),
                                      str(table_resample_by_moth.index[i])))

#Podemos repetir este proceso para todo los nuevos dataframes que generamos
#volvemos a conectarnos usando pymysql para verificar que la base de datos fuera creada satisfactoriamente.
 
db_connection = pymysql.connect('IP', 'root', 'password', 'demo_database')

table_from_google_cloud_sql = pd.read_sql('SELECT * FROM table_resample_by_moth', con = db_connection)
print(table_from_google_cloud_sql.head(5))
