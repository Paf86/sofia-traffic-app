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

# 1. ИНИЦИАЛИЗАЦИЯ НА ПРИЛОЖЕНИЕТО
app = Flask(__name__)
CORS(app)


# 2. КОНФИГУРАЦИЯ И ГЛОБАЛНИ ПРОМЕНЛИВИ
# --- Секция за разархивиране ---
# Името на ZIP файла, който се намира във вашето GitHub хранилище
ZIP_FILE_PATH = "gtfs-static.zip"
# Път, където ще разархивираме данните. /tmp е стандартна временна директория в Render.
DATA_DIR = "/tmp/gtfs_unzipped"
# Казваме на останалата част от скрипта да търси файловете тук!
BASE_PATH = DATA_DIR + "/"

# --- Секция за кеширане ---
CACHE_DURATION_SECONDS = 15
cache_lock = threading.Lock()
trip_updates_feed_cache = None
vehicle_positions_feed_cache = None
last_cache_update_timestamp = 0

# --- Глобални променливи за данните (дефинираме ги празни) ---
routes_data, trips_data, stops_data, active_services = {}, {}, {}, set()
schedule_by_trip, shapes_data, trip_stops_sequence = {}, {}, {}
stop_service_info, stop_to_trips_map = {}, {}


# 3. ДЕФИНИЦИИ НА ВСИЧКИ ФУНКЦИИ
# Всички функции трябва да бъдат дефинирани тук, преди да ги използваме.

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

def load_static_data():
    """
    Зарежда всички разархивирани статични файлове в паметта.
    """
    global routes_data, trips_data, stops_data, active_services, shapes_data, schedule_by_trip, trip_stops_sequence, stop_service_info, stop_to_trips_map
    try:
        print("Зареждане на статични данни в паметта...")
        with open(f'{BASE_PATH}routes.txt', mode='r', encoding='utf-8-sig') as f: routes_data = {row['route_id']: row for row in csv.DictReader(f)}
        with open(f'{BASE_PATH}trips.txt', mode='r', encoding='utf-8-sig') as f: trips_data = {row['trip_id']: row for row in csv.DictReader(f)}
        used_stop_ids = set()
        with open(f'{BASE_PATH}stop_times.txt', mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                trip_id = row['trip_id']; stop_id = row['stop_id']; used_stop_ids.add(stop_id)
                if trip_id not in schedule_by_trip: schedule_by_trip[trip_id] = {}
                schedule_by_trip[trip_id][stop_id] = row['arrival_time']
                if trip_id not in trip_stops_sequence: trip_stops_sequence[trip_id] = []
                trip_stops_sequence[trip_id].append({'stop_id': stop_id, 'stop_sequence': int(row['stop_sequence'])})
                if stop_id not in stop_to_trips_map: stop_to_trips_map[stop_id] = []
                stop_to_trips_map[stop_id].append(trip_id)
                trip_info = trips_data.get(trip_id)
                if trip_info:
                    route_info = routes_data.get(trip_info['route_id'])
                    if route_info:
                        if stop_id not in stop_service_info: stop_service_info[stop_id] = {'types': set()}
                        route_type = route_info.get('route_type'); type_map = {'0': 'TRAM', '3': 'BUS', '11': 'TROLLEY'}; transport_type = type_map.get(route_type); route_name = route_info.get('route_short_name', '')
                        if route_name.startswith('N'): transport_type = 'NIGHT'
                        if transport_type: stop_service_info[stop_id]['types'].add(transport_type)
        for trip_id in trip_stops_sequence: trip_stops_sequence[trip_id].sort(key=lambda x: x['stop_sequence'])
        with open(f'{BASE_PATH}stops.txt', mode='r', encoding='utf-8-sig') as f: stops_data = {row['stop_id']: row for row in csv.DictReader(f) if row['stop_id'] in used_stop_ids}
        with open(f'{BASE_PATH}shapes.txt', mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                shape_id = row['shape_id']
                if shape_id not in shapes_data: shapes_data[shape_id] = []
                shapes_data[shape_id].append([float(row['shape_pt_lat']), float(row['shape_pt_lon'])])
        sofia_tz = pytz.timezone('Europe/Sofia'); today_str = datetime.now(sofia_tz).strftime('%Y%m%d')
        with open(f'{BASE_PATH}calendar_dates.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row['date'] == today_str and row['exception_type'] == '1': active_services.add(row['service_id'])
        print("Статичните данни са заредени успешно.")
    except FileNotFoundError as e:
        print(f"КРИТИЧНА ГРЕШКА: Файлът {e.filename} не е намерен след разархивиране.", file=sys.stderr)
        return False
    return True

def refresh_realtime_cache_if_needed():
    """Проверява дали кешът е стар и ако е, го опреснява."""
    global trip_updates_feed_cache, vehicle_positions_feed_cache, last_cache_update_timestamp
    with cache_lock:
        if time.time() - last_cache_update_timestamp > CACHE_DURATION_SECONDS:
            print("Кешът е стар. Опресняване...", file=sys.stderr)
            try:
                proxy_url_updates = "https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed=trip-updates"
                response_updates = requests.get(proxy_url_updates, timeout=15)
                if response_updates.status_code == 200:
                    feed_updates = gtfs_realtime_pb2.FeedMessage()
                    feed_updates.ParseFromString(response_updates.content)
                    trip_updates_feed_cache = feed_updates
                else:
                    print(f"Грешка при Trip Updates: статус {response_updates.status_code}", file=sys.stderr)

                proxy_url_positions = "https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed=vehicle-positions"
                response_positions = requests.get(proxy_url_positions, timeout=15)
                if response_positions.status_code == 200:
                    feed_positions = gtfs_realtime_pb2.FeedMessage()
                    feed_positions.ParseFromString(response_positions.content)
                    vehicle_positions_feed_cache = feed_positions
                else:
                    print(f"Грешка при Vehicle Positions: статус {response_positions.status_code}", file=sys.stderr)

                last_cache_update_timestamp = time.time()
                print("Кешът е опреснен успешно.", file=sys.stderr)
            except requests.RequestException as e:
                print(f"КРИТИЧНА ГРЕШКА при мрежова заявка към прокси: {e}", file=sys.stderr)

def parse_gtfs_time(time_str, date_obj):
    try:
        parts = time_str.split(':'); h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
        days_offset = h // 24; h = h % 24
        time_delta = timedelta(days=days_offset, hours=h, minutes=m, seconds=s)
        return datetime(date_obj.year, date_obj.month, date_obj.day) + time_delta
    except (ValueError, IndexError): return None


# 4. КОД, КОЙТО СЕ ИЗПЪЛНЯВА САМО ВЕДНЪЖ ПРИ СТАРТИРАНЕ
# След като всички функции са дефинирани, можем да ги извикаме.
if setup_local_data():
    load_static_data()
else:
    print("Приложението не може да стартира поради проблем с разархивирането на данните.", file=sys.stderr)
    sys.exit(1)


# 5. API ЕНДПОЙНТИ (ROUTES)
# Този код се изпълнява при всяка заявка от потребител.

@app.route('/api/all_stops')
def get_all_stops():
    enriched_stops = []
    for stop_id, stop_data in stops_data.items():
        info = stop_service_info.get(stop_id)
        if info:
            stop_copy = stop_data.copy()
            stop_copy['service_types'] = sorted(list(info['types']))
            enriched_stops.append(stop_copy)
    return jsonify(enriched_stops)

@app.route('/api/vehicles_for_stop/<stop_id>')
def get_vehicles_for_stop(stop_id):
    try:
        refresh_realtime_cache_if_needed()
        sofia_tz = pytz.timezone('Europe/Sofia')
        now_dt = datetime.now(sofia_tz)
        now_ts = int(time.time())
        arrival_predictions = {}
        if trip_updates_feed_cache:
            for entity in trip_updates_feed_cache.entity:
                if entity.HasField('trip_update'):
                    update = entity.trip_update
                    trip_id = update.trip.trip_id
                    if trip_id not in arrival_predictions: arrival_predictions[trip_id] = {}
                    for stu in update.stop_time_update:
                        if stu.HasField('arrival') and stu.arrival.time > 0:
                            arrival_predictions[trip_id][stu.stop_id] = stu.arrival.time
        stop_info = stops_data.get(stop_id)
        if not stop_info: return jsonify({"error": "Stop not found"}), 404
        stop_code = stop_info.get('stop_code')
        physical_stop_ids = {s_id for s_id, s_data in stops_data.items() if s_data.get('stop_code') == stop_code and stop_code}
        physical_stop_ids.add(stop_id)
        all_arrivals = []
        trip_ids_for_stop = set()
        for s_id in physical_stop_ids:
            trip_ids_for_stop.update(stop_to_trips_map.get(s_id, []))
        for trip_id in trip_ids_for_stop:
            trip_info = trips_data.get(trip_id)
            if not trip_info or trip_info.get('service_id') not in active_services: continue
            trip_schedule_for_stops = schedule_by_trip.get(trip_id, {})
            relevant_stop_id_in_schedule = next((s_id for s_id in physical_stop_ids if s_id in trip_schedule_for_stops), None)
            if not relevant_stop_id_in_schedule: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            eta_minutes, prediction_source = -1, None
            scheduled_time_str = trip_schedule_for_stops.get(relevant_stop_id_in_schedule, "")
            predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(relevant_stop_id_in_schedule)
            if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60:
                eta_minutes = max(0, round((predicted_arrival_ts - now_ts) / 60))
                prediction_source = "official"
            else:
                scheduled_dt_naive = parse_gtfs_time(scheduled_time_str, now_dt)
                if scheduled_dt_naive:
                    scheduled_dt_aware = sofia_tz.localize(scheduled_dt_naive)
                    if now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                        eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60))
                        prediction_source = "schedule"
            if prediction_source:
                all_arrivals.append({
                    "trip_id": trip_id, "route_name": route_info.get('route_short_name', 'Н/А'),
                    "route_type": route_info.get('route_type', ''), "destination": trip_info.get('trip_headsign', 'Н/И'),
                    "eta_minutes": eta_minutes, "prediction_source": prediction_source
                })
        all_arrivals.sort(key=lambda x: x['eta_minutes'])
        return jsonify(all_arrivals)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_vehicles_for_stop: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/all_detailed_arrivals_snapshot')
def get_all_detailed_arrivals_snapshot():
    try:
        refresh_realtime_cache_if_needed()
        sofia_tz = pytz.timezone('Europe/Sofia')
        now_dt = datetime.now(sofia_tz)
        now_ts = int(time.time())
        arrival_predictions = {}
        if trip_updates_feed_cache:
            for entity in trip_updates_feed_cache.entity:
                if entity.HasField('trip_update'):
                    update = entity.trip_update
                    trip_id = update.trip.trip_id
                    if trip_id not in arrival_predictions: arrival_predictions[trip_id] = {}
                    for stu in update.stop_time_update:
                        if stu.HasField('arrival') and stu.arrival.time > 0:
                            arrival_predictions[trip_id][stu.stop_id] = stu.arrival.time
        final_results = {stop_id: [] for stop_id in stops_data.keys()}
        for trip_id, trip_info in trips_data.items():
            if trip_info.get('service_id') not in active_services: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            for stop_id_in_schedule, scheduled_time_str in schedule_by_trip.get(trip_id, {}).items():
                eta_minutes, prediction_source = -1, None
                predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(stop_id_in_schedule)
                if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60:
                    eta_minutes = max(0, round((predicted_arrival_ts - now_ts) / 60))
                    prediction_source = "official"
                else:
                    scheduled_dt_naive = parse_gtfs_time(scheduled_time_str, now_dt)
                    if scheduled_dt_naive:
                        scheduled_dt_aware = sofia_tz.localize(scheduled_dt_naive)
                        if now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                            eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60))
                            prediction_source = "schedule"
                if prediction_source and stop_id_in_schedule in final_results:
                    final_results[stop_id_in_schedule].append({
                        "trip_id": trip_id, "route_name": route_info.get('route_short_name', 'Н/А'),
                        "route_type": route_info.get('route_type', ''), "destination": trip_info.get('trip_headsign', 'Н/И'),
                        "eta_minutes": eta_minutes, "prediction_source": prediction_source
                    })
        for stop_id in final_results: final_results[stop_id].sort(key=lambda x: x['eta_minutes'])
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_all_detailed_arrivals_snapshot: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/vehicles_for_routes/<route_names_str>')
def get_vehicles_for_routes(route_names_str):
    try:
        refresh_realtime_cache_if_needed()
        requested_routes = set(route_names_str.split(','))
        if not vehicle_positions_feed_cache: return jsonify([])
        vehicles_on_routes = []
        for entity in vehicle_positions_feed_cache.entity:
            if entity.HasField('vehicle'):
                vehicle = entity.vehicle; trip_id = vehicle.trip.trip_id
                trip_info = trips_data.get(trip_id)
                if not trip_info: continue
                route_info = routes_data.get(trip_info['route_id'])
                if route_info and route_info.get('route_short_name') in requested_routes:
                    vehicles_on_routes.append({
                        "latitude": vehicle.position.latitude, "longitude": vehicle.position.longitude,
                        "trip_id": trip_id, "route_name": route_info.get('route_short_name'),
                        "route_type": route_info.get('route_type', ''), "destination": trip_info.get('trip_headsign', 'Н/И')
                    })
        return jsonify(vehicles_on_routes)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_vehicles_for_routes: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/shape/<trip_id>')
def get_shape_for_trip(trip_id):
    trip_info = trips_data.get(trip_id)
    if not trip_info: return jsonify({"error": "Trip not found"}), 404
    shape_id = trip_info.get('shape_id')
    shape_points = shapes_data.get(shape_id, [])
    return jsonify(shape_points)

@app.route('/api/stops_for_trip/<trip_id>')
def get_stops_for_trip(trip_id):
    if trip_id not in trip_stops_sequence: return jsonify({"error": "Trip not found"}), 404
    stops_list = [stops_data.get(s['stop_id']) for s in trip_stops_sequence[trip_id] if stops_data.get(s['stop_id'])]
    return jsonify(stops_list)

@app.route('/api/bulk_arrivals_for_stops', methods=['POST'])
def get_bulk_arrivals_for_stops():
    try:
        refresh_realtime_cache_if_needed()
        stop_codes_to_query = set(request.json.get('stop_codes', []))
        if not stop_codes_to_query: return jsonify({})
        sofia_tz = pytz.timezone('Europe/Sofia'); now_dt = datetime.now(sofia_tz); now_ts = int(time.time())
        arrival_predictions = {}
        if trip_updates_feed_cache:
            for entity in trip_updates_feed_cache.entity:
                if entity.HasField('trip_update'):
                    update = entity.trip_update; trip_id = update.trip.trip_id
                    if trip_id not in arrival_predictions: arrival_predictions[trip_id] = {}
                    for stu in update.stop_time_update:
                        if stu.HasField('arrival') and stu.arrival.time > 0: arrival_predictions[trip_id][stu.stop_id] = stu.arrival.time
        stop_code_to_ids_map = {}
        for s_id, s_data in stops_data.items():
            code = s_data.get('stop_code')
            if code in stop_codes_to_query:
                if code not in stop_code_to_ids_map: stop_code_to_ids_map[code] = []
                stop_code_to_ids_map[code].append(s_id)
        bulk_results = {}
        trips_to_check = set()
        for code in stop_codes_to_query:
            for stop_id in stop_code_to_ids_map.get(code, []):
                trips_to_check.update(stop_to_trips_map.get(stop_id, []))
        for trip_id in trips_to_check:
            trip_info = trips_data.get(trip_id)
            if not trip_info or trip_info.get('service_id') not in active_services: continue
            for stop_id_in_schedule, scheduled_time_str in schedule_by_trip.get(trip_id, {}).items():
                stop_details = stops_data.get(stop_id_in_schedule)
                if not stop_details: continue
                stop_code = stop_details.get('stop_code')
                if stop_code in stop_codes_to_query:
                    predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(stop_id_in_schedule)
                    is_upcoming = False
                    if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60: is_upcoming = True
                    else:
                        scheduled_dt_naive = parse_gtfs_time(scheduled_time_str, now_dt)
                        if scheduled_dt_naive and now_dt < sofia_tz.localize(scheduled_dt_naive) < now_dt + timedelta(hours=2): is_upcoming = True
                    if is_upcoming:
                        if stop_code not in bulk_results: bulk_results[stop_code] = {'arrivals': set()}
                        route_info = routes_data.get(trip_info['route_id'])
                        route_type_str = route_info.get('route_type', ''); route_name = route_info.get('route_short_name', '')
                        transport_type = 'BUS'
                        if route_name.startswith('N'): transport_type = 'NIGHT'
                        elif route_type_str == '0': transport_type = 'TRAM'
                        elif route_type_str == '11': transport_type = 'TROLLEY'
                        bulk_results[stop_code]['arrivals'].add(transport_type)
        final_results = {code: {'arrivals': list(data['arrivals'])} for code, data in bulk_results.items()}
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_arrivals_for_stops: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/bulk_detailed_arrivals', methods=['POST'])
def get_bulk_detailed_arrivals():
    try:
        refresh_realtime_cache_if_needed()
        stop_codes_to_query = set(request.json.get('stop_codes', []))
        if not stop_codes_to_query: return jsonify({})
        sofia_tz = pytz.timezone('Europe/Sofia'); now_dt = datetime.now(sofia_tz); now_ts = int(time.time())
        arrival_predictions = {}
        if trip_updates_feed_cache:
            for entity in trip_updates_feed_cache.entity:
                if entity.HasField('trip_update'):
                    update = entity.trip_update; trip_id = update.trip.trip_id
                    if trip_id not in arrival_predictions: arrival_predictions[trip_id] = {}
                    for stu in update.stop_time_update:
                        if stu.HasField('arrival') and stu.arrival.time > 0: arrival_predictions[trip_id][stu.stop_id] = stu.arrival.time
        final_results = {code: [] for code in stop_codes_to_query}
        relevant_stop_ids = set()
        stop_id_to_code_map = {}
        for s_id, s_data in stops_data.items():
            code = s_data.get('stop_code')
            if code in stop_codes_to_query: relevant_stop_ids.add(s_id); stop_id_to_code_map[s_id] = code
        trips_to_check = set()
        for stop_id in relevant_stop_ids: trips_to_check.update(stop_to_trips_map.get(stop_id, []))
        for trip_id in trips_to_check:
            trip_info = trips_data.get(trip_id)
            if not trip_info or trip_info.get('service_id') not in active_services: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            for stop_id_in_schedule, scheduled_time_str in schedule_by_trip.get(trip_id, {}).items():
                if stop_id_in_schedule in relevant_stop_ids:
                    eta_minutes, prediction_source = -1, None
                    predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(stop_id_in_schedule)
                    if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60:
                        eta_minutes = max(0, round((predicted_arrival_ts - now_ts) / 60)); prediction_source = "official"
                    else:
                        scheduled_dt_naive = parse_gtfs_time(scheduled_time_str, now_dt)
                        if scheduled_dt_naive:
                            scheduled_dt_aware = sofia_tz.localize(scheduled_dt_naive)
                            if now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                                eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60)); prediction_source = "schedule"
                    if prediction_source:
                        arrival_object = { "trip_id": trip_id, "route_name": route_info.get('route_short_name', 'Н/А'), "route_type": route_info.get('route_type', ''), "destination": trip_info.get('trip_headsign', 'Н/И'), "eta_minutes": eta_minutes, "prediction_source": prediction_source }
                        stop_code = stop_id_to_code_map[stop_id_in_schedule]
                        final_results[stop_code].append(arrival_object)
        for code in final_results: final_results[code].sort(key=lambda x: x['eta_minutes'])
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_detailed_arrivals: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500
