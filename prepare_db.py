# Файл: prepare_db.py
import sqlite3
import csv
import os

# Път до GTFS файловете
BASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/"
DB_FILE = os.path.join(BASE_PATH, "gtfs.db")

def create_database():
    """Създава SQLite базата данни и таблиците."""
    # Изтриваме старата база данни, ако съществува
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    print("Създаване на таблици...")

    # Създаваме таблици, които отразяват структурата на GTFS файловете
    cursor.execute("""
        CREATE TABLE stops (
            stop_id TEXT PRIMARY KEY,
            stop_code TEXT,
            stop_name TEXT,
            stop_lat REAL,
            stop_lon REAL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE routes (
            route_id TEXT PRIMARY KEY,
            route_short_name TEXT,
            route_long_name TEXT,
            route_type INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE trips (
            route_id TEXT,
            service_id TEXT,
            trip_id TEXT PRIMARY KEY,
            trip_headsign TEXT,
            direction_id INTEGER,
            shape_id TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE stop_times (
            trip_id TEXT,
            arrival_time TEXT,
            departure_time TEXT,
            stop_id TEXT,
            stop_sequence INTEGER,
            PRIMARY KEY (trip_id, stop_sequence)
        )
    """)
    
    cursor.execute("""
        CREATE TABLE shapes (
            shape_id TEXT,
            shape_pt_lat REAL,
            shape_pt_lon REAL,
            shape_pt_sequence INTEGER
        )
    """)
    
    # ... добави и други таблици, ако са нужни (напр. calendar_dates) ...

    conn.commit()
    conn.close()
    print("Таблиците са създадени.")

def import_data():
    """Импортира данните от CSV файловете в базата данни."""
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    def import_file(filename, table_name, columns):
        print(f"Импортиране на {filename}...")
        with open(os.path.join(BASE_PATH, filename), 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            # Създаваме SQL заявка от типа INSERT INTO table (...) VALUES (?, ?, ...)
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
            
            # Подготвяме данните за bulk insert
            data_to_insert = []
            for row in reader:
                data_to_insert.append(tuple(row.get(col) for col in columns))

            cursor.executemany(sql, data_to_insert)
        conn.commit()
        print(f"{filename} е импортиран успешно.")

    import_file('stops.txt', 'stops', ['stop_id', 'stop_code', 'stop_name', 'stop_lat', 'stop_lon'])
    import_file('routes.txt', 'routes', ['route_id', 'route_short_name', 'route_long_name', 'route_type'])
    import_file('trips.txt', 'trips', ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id', 'shape_id'])
    import_file('stop_times.txt', 'stop_times', ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence'])
    import_file('shapes.txt', 'shapes', ['shape_id', 'shape_pt_lat', 'shape_pt_lon', 'shape_pt_sequence'])

    conn.close()

def create_indexes():
    """Създава индекси за по-бързи заявки."""
    print("Създаване на индекси...")
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute("CREATE INDEX idx_stop_times_trip_id ON stop_times (trip_id);")
    cursor.execute("CREATE INDEX idx_stop_times_stop_id ON stop_times (stop_id);")
    cursor.execute("CREATE INDEX idx_trips_route_id ON trips (route_id);")
    cursor.execute("CREATE INDEX idx_shapes_shape_id ON shapes (shape_id);")
    
    conn.commit()
    conn.close()
    print("Индексите са създадени.")


if __name__ == "__main__":
    create_database()
    import_data()
    create_indexes()
    print("\nБазата данни 'gtfs.db' е създадена успешно!")