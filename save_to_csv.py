from utils import *
import csv
# Conectar a la base de datos PostgreSQL
conn = conectar_db()
cursor = conn.cursor()

table_name = input("Ingresa el nombre de la tabla:")
csv_file_path = input("Ingresa la ruta de la carpeta, tal como se la proporciona el file explorer de windows:")
csv_file_path = csv_file_path.replace("\\", "/")
csv_file_path = csv_file_path + "/" + table_name + ".csv"
query = f"COPY {table_name} TO STDOUT WITH CSV HEADER"

with open(csv_file_path, 'w') as f:
    cursor.copy_expert(query, f)


cursor.close()
conn.close()

print(f'Datos exportados a {csv_file_path}')
