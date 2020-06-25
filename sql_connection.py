import sqlalchemy
import pymysql
import pandas as pd

engine_str = 'mysql+pymysql://root:password@localhost/my_first_ddbb'
engine = sqlalchemy.create_engine(engine_str, echo=False, encoding='utf-8')
connection = engine.connect()


table = """ CREATE TABLE `my_first_ddbb`.`Python_table` (
      `id_names` INT UNSIGNED NOT NULL AUTO_INCREMENT,
      `first_name` VARCHAR(100) NOT NULL,
      `second_name` VARCHAR(45) NULL,
      `update_date` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (`id_names`),
      UNIQUE INDEX `idnames_UNIQUE` (`id_names` ASC) VISIBLE)
  COMMENT = 'Nombres de las personas'; """


add_values = """ INSERT INTO my_first_ddbb.Python_table (first_name, second_name) VALUES
     ('Andrea' , 'Camaro'),
     ('Sofia', 'Lopez'),
     ('Javier' , 'Rojas'),
     ('Carlos' , 'Trujillo');"""


update_table = """ 
                  UPDATE my_first_ddbb.Python_table
                  SET first_name = 'Xavier', second_name = 'Rojas Pineda'
                  WHERE second_name = 'Rojas'; """

#Ejecutamos
connection.execute(table)
connection.execute(add_values)
connection.execute(update_table)
#terminamos de ejecutar

db_connection = pymysql.connect('localhost', 'root', 
    'password', 'my_first_ddbb')

df = pd.read_sql('SELECT * FROM names', con = db_connection)

a = -1

df = df.append({'first_name': 'Juanito', 'second_name': 'Alimaña', 'id_names': a},
                ignore_index = True)
df = df.append({'first_name': 'Juanito', 'second_name': 'perez', 'id_names': a},
          ignore_index = True)


query = """INSERT INTO my_first_ddbb.Python_table (first_name, second_name)
VALUES(%s, %s)"""


x = float('nan')
for i in range(len(df)):
    if df.id_names[i] == -1:
        connection.execute(query, (df.first_name[i], df.second_name[i]))
        
delete = """DELETE FROM my_first_ddbb.Python_table WHERE first_name = %s"""
connection.execute(delete, (df.first_name[0]))

connection.close()
