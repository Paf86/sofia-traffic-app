import requests
from flask import Flask, jsonify, request
from google.transit import gtfs_realtime_pb2
import csv
from datetime import datetime, timedelta
import pytz
from flask_cors import CORS
import time
import sys
import threading
import os
import zipfile

app = Flask(__name__)
CORS(app)

# --- НОВА СЕКЦИЯ: ЛОКАЛНО РАЗАРХИВИРАНЕ НА ДАННИ ---
# Път до ZIP файла, който е в хранилището
ZIP_FILE_PATH = "gtfs_data.zip"

# Път, където ще разархивираме данните. /tmp е стандартна временна директория в Render.
DATA_DIR = "/tmp/gtfs_unzipped"
# Казваме на останалата част от скрипта да търси файловете тук!
BASE_PATH = DATA_DIR + "/"

def setup_local_data():
    """
    Разархивира статичните GTFS данни от локалния ZIP файл.
    Тази функция се изпълнява при всяко стартиране на сървъра.
    """
    print("Проверка и подготовка на статичните данни...")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        print(f"Разархивиране на '{ZIP_FILE_PATH}' в '{DATA_DIR}'...")
        with zipfile.ZipFile(ZIP_FILE_PATH, 'r') as zf:
            zf.extractall(DATA_DIR)
        print("Данните са разархивирани успешно.")
        return True
    except FileNotFoundError:
        print(f"КРИТИЧНА ГРЕШКА: ZIP файлът '{ZIP_FILE_PATH}' не е намерен!", file=sys.stderr)
        return False
    except zipfile.BadZipFile:
        print(f"КРИТИЧНА ГРЕШКА: Файлът '{ZIP_FILE_PATH}' не е валиден ZIP архив.", file=sys.stderr)
        return False

# --- Секция за кеширане (остава същата) ---
CACHE_DURATION_SECONDS = 15
cache_lock = threading.Lock()
trip_updates_feed_cache = None
vehicle_positions_feed_cache = None
last_cache_update_timestamp = 0

# Глобални статични данни (остават същите)
routes_data, trips_data, stops_data, active_services = {}, {}, {}, set()
schedule_by_trip, shapes_data, trip_stops_sequence = {}, {}, {}
stop_service_info, stop_to_trips_map = {}, {}

# --- ИЗПЪЛНЕНИЕ ПРИ СТАРТИРАНЕ НА СЪРВЪРА ---
# Първо разархивираме, после зареждаме данните!
if setup_local_data():
    load_static_data() # Тази ваша функция вече ще използва новия BASE_PATH
else:
    print("Приложението не може да стартира поради проблем с разархивирането.", file=sys.stderr)
    sys.exit(1) # Спираме приложението, ако данните ги няма

# ... ОСТАНАЛАТА ЧАСТ ОТ ВАШИЯ КОД (refresh_realtime_cache_if_needed, load_static_data, @app.route и т.н.) ...
# НЕ ПРОМЕНЯЙТЕ НИЩО ДРУГО! Функцията load_static_data() автоматично ще използва новия BASE_PATH.
