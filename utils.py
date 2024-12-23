import psycopg2
import datetime

def conectar_db():
    conn = psycopg2.connect(database = "pruebabinance", 
                        user = "postgres", 
                        host= 'localhost',
                        password = "daisolana78",
                        port = 5432)
    return conn
    
def crear_tabla(conn, table_name):
    cursor = conn.cursor()
    # Crear la tabla con open_time como clave primaria
    query = f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        open_time TIMESTAMP NOT NULL PRIMARY KEY,
        open_price DECIMAL(18, 8) NOT NULL,
        high_price DECIMAL(18, 8) NOT NULL,
        low_price DECIMAL(18, 8) NOT NULL,
        close_price DECIMAL(18, 8) NOT NULL,
        volume DECIMAL(18, 8) NOT NULL,
        close_time TIMESTAMP NOT NULL,
        quote_asset_volume DECIMAL(18, 8) NOT NULL,
        number_of_trades INT NOT NULL,
        taker_buy_base_asset_volume DECIMAL(18, 8) NOT NULL,
        taker_buy_quote_asset_volume DECIMAL(18, 8) NOT NULL
    )
    '''
    cursor.execute(query)
    conn.commit()
    cursor.close()
    return True


def agregar_velas(conn, klines, table_name):
    cursor = conn.cursor()

    # Convertir open_time y close_time a TIMESTAMP y seleccionar los primeros 11 elementos
    klines_modificados = []
    for kline in klines:
        open_time = datetime.datetime.fromtimestamp(kline[0] / 1000.0)
        close_time = datetime.datetime.fromtimestamp(kline[6] / 1000.0)
        kline_modificado = [
            open_time,
            kline[1],
            kline[2],
            kline[3],
            kline[4],
            kline[5],
            close_time,
            kline[7],
            kline[8],
            kline[9],
            kline[10]
        ]
        klines_modificados.append(kline_modificado)

    # Insertar datos en la tabla
    for kline in klines_modificados:
        query = f'''
    INSERT INTO {table_name} (open_time, open_price, high_price, low_price, close_price, volume, close_time, quote_asset_volume, number_of_trades, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (open_time) DO NOTHING
    '''
        cursor.execute(query, kline)

    # Guardar los cambios y cerrar la conexión
    conn.commit()
    cursor.close()
    return True


def get_klines(client, symbol, interval, start_time, end_time=None, limit=1500):
    return client.get_klines(symbol=symbol, interval=interval, startTime=start_time, endTime=end_time, limit=limit)

def print_time_readable(msj, open_time):
    open_time_readable = datetime.datetime.fromtimestamp(open_time / 1000.0) 
    print(msj + str(open_time_readable))


 