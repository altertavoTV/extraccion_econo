from binance import Client
import time
import datetime
from utils import *
# Tus claves API
from privados import api_key, api_secret
#los datos de la base de datos van en utils conectar_db() a excepcion de la table name
moneda = input("nombre de la moneda sin usdt, solo la moneda, ej doge,ada:")
symbol = moneda.upper() + "USDT"
table_name = moneda.lower()+"minutes" #para base de datos, si no existe la crea

interval = '1m'
cant_registros = 1000 #registros consultados a binance por iteracion

client = Client(api_key, api_secret)
start_time = 0  # Tiempo de inicio en milisegundos desde la época Unix


log_interval = int((30*24*60)/cant_registros) #cada cuanto va a mostrar por que fecha va

current_time = int(time.time() * 1000)
print_time_readable("Current time:",current_time)


conn = conectar_db()
crear_tabla(conn, table_name)
contador = 0

while True:
    klines = get_klines(client, symbol, interval, start_time, limit=cant_registros)
    if not klines:
        print_time_readable("no hay registros con el tiempo: ", start_time)
        break

    agregar_velas(conn, klines, table_name) #cambiar a guardar directamente en csv
    start_time = klines[-1][0] + 1  # Actualizar el tiempo de inicio para el siguiente lote de datos
    if start_time >= current_time:
        print("start time mayor a current time ")
        print_time_readable("start time:", start_time)
        print_time_readable("current time:", current_time)
        break
    if(contador % log_interval == 0):#logs
        print_time_readable("iteracion: %s, fecha:" %contador, klines[0][0])
    contador = contador + 1
print("FIN")
conn.close()