import sqlalchemy
import pymysql
import pandas as pd

db_connection = pymysql.connect('localhost', 'root', 
    'password', 'my_first_ddbb')

df = pd.read_sql('SELECT * FROM names', con = db_connection)

a = -1

df = df.append({'first_name': 'Juanito', 'second_name': 'Rodriguez', 'id_names': a},
                ignore_index = True)
df = df.append({'first_name': 'Juanito', 'second_name': 'perez', 'id_names': a},
          ignore_index = True)

engine_str = 'mysql+pymysql://root:password@localhost/my_first_ddbb'
engine = sqlalchemy.create_engine(engine_str, echo=False, encoding='utf-8')
connection = engine.connect()

query = """INSERT INTO my_first_ddbb.names (first_name, second_name)
VALUES(%s, %s)"""

x = float('nan')

for i in range(len(df)):
    if df.id_names[i] == -1:
        connection.execute(query, (df.first_name[i], df.second_name[i]))
        
delete = """DELETE FROM my_first_ddbb.names WHERE first_name = %s"""
connection.execute(delete, (df.first_name[0]))

connection.close()
