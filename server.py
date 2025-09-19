import requests
from flask import Flask, jsonify
import csv
from datetime import datetime, timedelta
import pytz
from flask_cors import CORS
from bs4 import BeautifulSoup
import os # <-- Добавяме нов import за работа с файловата система

# --- КЛЮЧОВА ПРОМЯНА: Правим пътя относителен, за да работи навсякъде ---
# Взимаме директорията, в която се намира самият server.py файл
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Пътят до файловете вече е относителен спрямо скрипта
BASE_PATH = BASE_DIR + "/"

app = Flask(__name__)
CORS(app)

# Глобални структури за данни
routes_data, trips_data, stops_data, arrivals_by_stop = {}, {}, {}, {}
active_services, shapes_data, schedule_by_trip = set(), {}, {}

def get_seconds_from_midnight(time_str):
    try:
        if 'мин' in time_str:
            now = datetime.now(pytz.timezone('Europe/Sofia'))
            minutes = int(time_str.split()[0])
            arrival_time = now.hour * 3600 + now.minute * 60 + now.second + (minutes * 60)
            return arrival_time
        elif 'Пристига' in time_str:
            return (datetime.now(pytz.timezone('Europe/Sofia')).hour * 3600 + 
                    datetime.now(pytz.timezone('Europe/Sofia')).minute * 60 + 
                    datetime.now(pytz.timezone('Europe/Sofia')).second)
        parts = list(map(int, time_str.split(':')))
        h, m, s = parts[0], parts[1], parts[2] if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError, IndexError):
        return 99999

def fetch_from_sofia_traffic(stop_code):
    if not stop_code:
        print("[ДЕБЪГ] fetch_from_sofia_traffic извикан без stop_code.")
        return []
    
    url = "https://www.sofiatraffic.bg/bg/schedules/stops-info"
    payload = {'stop_code_q': stop_code}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        print(f"[ДЕБЪГ] Изпращам POST заявка към {url} със stop_code: {stop_code}")
        response = requests.post(url, data=payload, headers=headers, timeout=15)
        print(f"[ДЕБЪГ] Получих отговор със статус код: {response.status_code}")

        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        arrivals = []
        
        arrival_rows = soup.find_all('div', class_='arrival-row')
        print(f"[ДЕБЪГ] Намерени редове с пристигания: {len(arrival_rows)}") # Ключов дебъг ред
        
        for row in arrival_rows:
            line_element = row.find('span', class_='line_number')
            time_element = row.find('div', class_='time').find('span')

            if line_element and time_element:
                line_name = line_element.text.strip()
                arrival_time_text = time_element.text.strip()
                
                now = datetime.now(pytz.timezone('Europe/Sofia'))
                timing_str = now.strftime('%H:%M:%S')
                
                if 'мин' in arrival_time_text:
                    try:
                        minutes_to_add = int(arrival_time_text.split()[0])
                        arrival_dt = now + timedelta(minutes=minutes_to_add)
                        timing_str = arrival_dt.strftime('%H:%M:%S')
                    except (ValueError, IndexError):
                        pass
                
                arrivals.append({
                    "lineName": line_name,
                    "timing": timing_str
                })
        
        print(f"[ДЕБЪГ] Успешно извлечени пристигания: {arrivals}")
        return arrivals

    except requests.RequestException as e:
        print(f"[ДЕБЪГ] КРИТИЧНА ГРЕШКА при връзка: {e}")
        return []
    except Exception as e:
        print(f"[ДЕБЪГ] ГРЕШКА при парсване на HTML: {e}")
        return []

def load_static_data():
    """Зарежда всички статични GTFS данни от файлове при стартиране."""
    global routes_data, trips_data, stops_data, arrivals_by_stop, active_services, shapes_data, schedule_by_trip
    try:
        print(f"[ДЕБЪГ] Зареждам статични файлове от: {BASE_PATH}")
        with open(f'{BASE_PATH}routes.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f): routes_data[row['route_id']] = row
        # ... (останалата част от функцията е същата)
        with open(f'{BASE_PATH}trips.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f): trips_data[row['trip_id']] = row
        active_stop_ids = set()
        with open(f'{BASE_PATH}stop_times.txt', mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stop_id, trip_id = row['stop_id'], row['trip_id']
                active_stop_ids.add(stop_id)
                if stop_id not in arrivals_by_stop: arrivals_by_stop[stop_id] = []
                arrivals_by_stop[stop_id].append(row)
                if trip_id not in schedule_by_trip: schedule_by_trip[trip_id] = []
                schedule_by_trip[trip_id].append(row)
        temp_stops_data = {}
        with open(f'{BASE_PATH}stops.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row['stop_id'] in active_stop_ids: temp_stops_data[row['stop_id']] = row
        stops_data = temp_stops_data
        today_str = datetime.now(pytz.timezone('Europe/Sofia')).strftime('%Y%m%d')
        with open(f'{BASE_PATH}calendar_dates.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row['date'] == today_str and row['exception_type'] == '1': active_services.add(row['service_id'])
        for trip_id in schedule_by_trip:
            schedule_by_trip[trip_id].sort(key=lambda x: int(x['stop_sequence']))
        with open(f'{BASE_PATH}shapes.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                shape_id = row['shape_id']
                if shape_id not in shapes_data: shapes_data[shape_id] = []
                shapes_data[shape_id].append((float(row['shape_pt_lat']), float(row['shape_pt_lon'])))
        print(f"[ДЕБЪГ] Всички статични данни са заредени. Брой спирки: {len(stops_data)}")
    except FileNotFoundError as e:
        print(f"[ДЕБЪГ] КРИТИЧНА ГРЕШКА: Файл не е намерен! -> {e}")
    except Exception as e:
        print(f"[ДЕБЪГ] КРИТИЧНА ГРЕШКА при зареждане на статични данни: {e}")

# ... (всички @app.route функции остават същите) ...
@app.route('/api/arrivals/<stop_id>')
def get_live_arrivals(stop_id):
    stop_info = stops_data.get(stop_id)
    stop_code = stop_info.get('stop_code') if stop_info else None
    api_arrivals = fetch_from_sofia_traffic(stop_code)
    if not api_arrivals:
        return jsonify({"arrivals": []})
    all_arrivals = []
    for arrival in api_arrivals:
        line_name = arrival.get("lineName")
        arrival_time_str = arrival.get("timing")
        if not line_name or not arrival_time_str:
            continue
        found_trip_info = None
        stop_ids_for_code = [s_id for s_id, s_data in stops_data.items() if s_data.get('stop_code') == stop_code]
        for s_id in stop_ids_for_code:
            if s_id in arrivals_by_stop:
                for scheduled in arrivals_by_stop[s_id]:
                    trip = trips_data.get(scheduled['trip_id'])
                    route = routes_data.get(trip.get('route_id')) if trip else None
                    if route and route.get('route_short_name') == line_name and trip.get('service_id') in active_services:
                        found_trip_info = {"trip_id": scheduled['trip_id'], "route_short_name": line_name, "destination": trip.get('trip_headsign', 'Н/И'), "arrival_time": arrival_time_str.split(',')[0], "is_realtime": True, "route_type": route.get('route_type', '3'), "route_id": route.get('route_id'), "direction_id": trip.get('direction_id', 'N/A')}
                        break
            if found_trip_info:
                break
        if found_trip_info:
            all_arrivals.append(found_trip_info)
    now = datetime.now(pytz.timezone('Europe/Sofia'))
    seconds_since_midnight = now.hour * 3600 + now.minute * 60 + now.second
    upcoming = [a for a in all_arrivals if get_seconds_from_midnight(a['arrival_time']) >= seconds_since_midnight]
    unique_arrivals = list({f"{v['route_short_name']}_{v['arrival_time']}": v for v in upcoming}.values())
    unique_arrivals.sort(key=lambda x: get_seconds_from_midnight(x['arrival_time']))
    return jsonify({"arrivals": unique_arrivals})
@app.route('/api/stops_for_trip/<trip_id>')
def get_stops_for_trip(trip_id):
    full_schedule = schedule_by_trip.get(trip_id, [])
    stops_with_details = [{"stop_id": st.get('stop_id'), "stop_name": stops_data.get(st.get('stop_id'), {}).get("stop_name"), "stop_lat": stops_data.get(st.get('stop_id'), {}).get("stop_lat"), "stop_lon": stops_data.get(st.get('stop_id'), {}).get("stop_lon")} for st in full_schedule if st.get('stop_id') in stops_data]
    return jsonify(stops_with_details)
@app.route('/api/routes_for_stop/<stop_id>')
def get_routes_for_stop(stop_id):
    if stop_id not in arrivals_by_stop:
        return jsonify([])
    trip_ids = {arrival['trip_id'] for arrival in arrivals_by_stop[stop_id]}
    route_ids = set()
    for trip_id in trip_ids:
        trip_info = trips_data.get(trip_id)
        if trip_info and trip_info.get('service_id') in active_services:
            route_ids.add(trip_info['route_id'])
    routes_for_this_stop = []
    for route_id in route_ids:
        route_info = routes_data.get(route_id)
        if route_info:
            routes_for_this_stop.append({"route_id": route_info.get('route_id'), "route_short_name": route_info.get('route_short_name')})
    return jsonify(routes_for_this_stop)
@app.route('/api/all_stops')
def get_all_stops():
    return jsonify(list(stops_data.values()))
@app.route('/api/shape/<trip_id>')
def get_shape_for_trip(trip_id):
    trip_info = trips_data.get(trip_id)
    if not trip_info or 'shape_id' not in trip_info:
        return jsonify({"error": "Маршрут не е намерен"}), 404
    shape_points = shapes_data.get(trip_info['shape_id'])
    if not shape_points:
        return jsonify({"error": "Геометрия не е намерена"}), 404
    return jsonify(shape_points)

load_static_data()
