import zipfile
import os

import requests
from flask import Flask, jsonify
from google.transit import gtfs_realtime_pb2
import csv
from datetime import datetime
import pytz
from flask_cors import CORS
import time

# Път до вашите GTFS файлове на PythonAnywhere
BASE_PATH = ""

app = Flask(__name__)
CORS(app)

# Глобални структури за данни и кеш
routes_data, trips_data, stops_data, arrivals_by_stop, active_services, shapes_data = {}, {}, {}, {}, set(), {}
schedule_by_trip = {}
realtime_data_cache = {}
last_realtime_fetch = 0

def get_seconds_from_midnight(time_str):
    try:
        h, m, s = map(int, time_str.split(':'))
        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        return 0

def fetch_and_parse_realtime_data():
    """
    Изтегля, парсва и кешира данните в реално време, заобикаляйки ограниченията на PythonAnywhere.
    """
    global realtime_data_cache, last_realtime_fetch
    
    if time.time() - last_realtime_fetch < 20:
        return realtime_data_cache

    try:
        # Оригинален URL
        target_url = "https://gtfs.sofiattraffic.bg/api/v1/trip-updates"
        # Прокси URL, за да заобиколим ограничението "403 Forbidden"
        proxy_url = f"https://corsproxy.io/?{requests.utils.quote(target_url)}"
        
        # Използваме proxy_url за заявката
        response = requests.get(proxy_url, timeout=15) # Увеличаваме таймаута заради проксито
        
        if response.status_code == 200:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            processed_updates = {}
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    trip_id = entity.trip_update.trip.trip_id
                    stop_updates = {}
                    for update in entity.trip_update.stop_time_update:
                        stop_sequence = update.stop_sequence
                        delay = update.arrival.delay if update.arrival.HasField('delay') else 0
                        arrival_time_unix = update.arrival.time if update.arrival.HasField('time') else 0
                        stop_updates[stop_sequence] = {'delay': delay, 'time': arrival_time_unix}
                    processed_updates[trip_id] = stop_updates

            realtime_data_cache = processed_updates
            last_realtime_fetch = time.time()
            print(f"Успешно заредени {len(processed_updates)} актуализации в реално време (през прокси).")
            return processed_updates
        else:
            print(f"ГРЕШКА при изтегляне през прокси. Статус код: {response.status_code}")
            return {} # Връщаме празен речник при грешка

    except requests.RequestException as e:
        print(f"КРИТИЧНА ГРЕШКА при изтегляне на данни в реално време: {e}")
        return {} # Връщаме празен речник при грешка

def load_static_data():
    global routes_data, trips_data, stops_data, arrivals_by_stop, active_services, shapes_data, schedule_by_trip
    # (Тази функция остава без промяна)
    try:
        with open(f'{BASE_PATH}routes.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f): routes_data[row['route_id']] = row
        print(f"Заредени {len(routes_data)} маршрута.")

        with open(f'{BASE_PATH}trips.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f): trips_data[row['trip_id']] = row
        print(f"Заредени {len(trips_data)} курса.")

        active_stop_ids = set()
        with open(f'{BASE_PATH}stop_times.txt', mode='r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                stop_id, trip_id = row['stop_id'], row['trip_id']
                active_stop_ids.add(stop_id)
                if stop_id not in arrivals_by_stop: arrivals_by_stop[stop_id] = []
                arrivals_by_stop[stop_id].append(row)
                if trip_id not in schedule_by_trip: schedule_by_trip[trip_id] = []
                schedule_by_trip[trip_id].append({
                    "stop_id": stop_id,
                    "arrival_time": row["arrival_time"],
                    "stop_sequence": int(row["stop_sequence"])
                })
        print(f"Индексирането на разписанията приключи.")

        temp_stops_data = {}
        with open(f'{BASE_PATH}stops.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row['stop_id'] in active_stop_ids:
                    temp_stops_data[row['stop_id']] = row
        stops_data = temp_stops_data
        print(f"Заредени {len(stops_data)} АКТИВНИ спирки.")

        today_str = datetime.now(pytz.timezone('Europe/Sofia')).strftime('%Y%m%d')
        with open(f'{BASE_PATH}calendar_dates.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                if row['date'] == today_str and row['exception_type'] == '1':
                    active_services.add(row['service_id'])
        print(f"Намерени {len(active_services)} активни услуги за днес.")

        for trip_id in schedule_by_trip:
            schedule_by_trip[trip_id].sort(key=lambda x: x['stop_sequence'])

        with open(f'{BASE_PATH}shapes.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                shape_id = row['shape_id']
                if shape_id not in shapes_data: shapes_data[shape_id] = []
                shapes_data[shape_id].append((float(row['shape_pt_lat']), float(row['shape_pt_lon'])))
        print(f"Заредени {len(shapes_data)} геометрии.")
    except FileNotFoundError as e:
        print(f"КРИТИЧНА ГРЕШКА: Файлът {e.filename} не е намерен.")
        return False
    return True

@app.route('/api/stops_for_trip/<trip_id>')
def get_stops_for_trip(trip_id):
    # (Тази функция остава без промяна)
    full_schedule = schedule_by_trip.get(trip_id)
    if not full_schedule: return jsonify({"error": "Графикът за този курс не е намерен"}), 404
    stops_with_details = []
    for stop_time in full_schedule:
        stop_id, stop_details = stop_time['stop_id'], stops_data.get(stop_id)
        if stop_details:
            stops_with_details.append({ "stop_id": stop_id, "stop_name": stop_details.get("stop_name"), "stop_lat": stop_details.get("stop_lat"), "stop_lon": stop_details.get("stop_lon"), "stop_sequence": stop_time["stop_sequence"] })
    return jsonify(stops_with_details)

@app.route('/api/all_stops')
def get_all_stops():
    # (Тази функция остава без промяна)
    return jsonify([sdata for sdata in stops_data.values() if sdata.get('location_type', '0') in ['0', '1', '']])

@app.route('/api/arrivals/<stop_id>')
def get_live_arrivals(stop_id):
    # (Тази функция е с добавена дебъг информация)
    try:
        live_updates = fetch_and_parse_realtime_data()
        debug_info = { "total_realtime_updates_fetched": len(live_updates) }

        sofia_tz = pytz.timezone('Europe/Sofia')
        now = datetime.now(sofia_tz)
        seconds_since_midnight = now.hour * 3600 + now.minute * 60 + now.second
        
        stop_info = stops_data.get(stop_id)
        if not stop_info: return jsonify({"arrivals": [], "debug_info": debug_info})

        stop_code = stop_info.get('stop_code')
        search_ids = set([stop_id])
        if stop_code:
            for s_id, s_data in stops_data.items():
                if s_data.get('stop_code') == stop_code: search_ids.add(s_id)
        
        all_arrivals = []
        for search_id in search_ids:
            if search_id in arrivals_by_stop:
                for arrival_info in arrivals_by_stop[search_id]:
                    trip_id, trip_info = arrival_info['trip_id'], trips_data.get(arrival_info['trip_id'])
                    if not (trip_info and trip_info.get('service_id') in active_services): continue

                    route_info = routes_data.get(trip_info.get('route_id'), {})
                    is_realtime, delay_seconds, final_arrival_time_str = False, 0, arrival_info['arrival_time']
                    
                    if trip_id in live_updates:
                        current_stop_sequence = int(arrival_info['stop_sequence'])
                        if current_stop_sequence in live_updates[trip_id]:
                            update = live_updates[trip_id][current_stop_sequence]
                            is_realtime, delay_seconds = True, update['delay']
                            if update['time'] > 0:
                                final_arrival_time_str = datetime.fromtimestamp(update['time'], sofia_tz).strftime('%H:%M:%S')
                            else:
                                h, rem = divmod(get_seconds_from_midnight(arrival_info['arrival_time']) + delay_seconds, 3600)
                                m, s = divmod(rem, 60)
                                final_arrival_time_str = f"{int(h):02}:{int(m):02}:{int(s):02}"

                    all_arrivals.append({ "trip_id": trip_id, "route_short_name": route_info.get('route_short_name', 'N/A'), "destination": trip_info.get('trip_headsign', 'Н/И'), "scheduled_time": arrival_info['arrival_time'], "arrival_time": final_arrival_time_str, "route_type": route_info.get('route_type', '3'), "route_id": trip_info.get('route_id'), "direction_id": trip_info.get('direction_id', 'N/A'), "is_realtime": is_realtime, "delay": delay_seconds })

        upcoming = [a for a in all_arrivals if get_seconds_from_midnight(a['arrival_time']) > seconds_since_midnight]
        
        if upcoming:
            upcoming.sort(key=lambda x: get_seconds_from_midnight(x['arrival_time']))
            return jsonify({ "arrivals": upcoming[:30], "debug_info": debug_info })
            
        all_day_lines = {a['route_short_name']: {**a, "arrival_time": "Извън работно време", "is_schedule": True} for a in all_arrivals if a['route_short_name'] not in {}}
        sorted_lines = sorted(all_day_lines.values(), key=lambda x: int(x['route_short_name']) if x['route_short_name'].isdigit() else 999)
        return jsonify({ "arrivals": sorted_lines, "debug_info": debug_info })

    except Exception as e:
        print(f"Error in get_live_arrivals: {e}")
        return jsonify({ "error": str(e), "arrivals": [], "debug_info": {"total_realtime_updates_fetched": -1} }), 500

@app.route('/api/shape/<trip_id>')
def get_shape_for_trip(trip_id):
    # (Тази функция остава без промяна)
    trip_info = trips_data.get(trip_id)
    if not trip_info or 'shape_id' not in trip_info: return jsonify({"error": "Маршрут не е намерен"}), 404
    shape_points = shapes_data.get(trip_info['shape_id'])
    if not shape_points: return jsonify({"error": "Геометрия не е намерена"}), 404
    return jsonify(shape_points)


load_static_data()
