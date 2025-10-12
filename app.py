import os
import requests
from flask import Flask, jsonify, request, Response
from google.transit import gtfs_realtime_pb2
import csv
from datetime import datetime, timedelta
import pytz
from flask_cors import CORS
import time
import sys
import threading
import math
import traceback
import re
from bs4 import BeautifulSoup
from collections import Counter

app = Flask(__name__)
CORS(app)

BASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/"

# --- Секция за кеширане ---
CACHE_DURATION_SECONDS = 15
recent_official_arrivals_cache = {}
RECENT_OFFICIAL_TTL_SECONDS = 90
gps_arrival_cache = {}
GPS_CACHE_TTL_SECONDS = 300
shared_data_lock = threading.Lock()
trip_updates_feed_cache = None
vehicle_positions_feed_cache = None
alerts_feed_cache = None
last_cache_update_timestamp = 0

# --- Глобални статични данни ---
routes_data, trips_data, stops_data, active_services = {}, {}, {}, set()
# ============================ ПРОМЯНА (1/5) ============================
# Премахваме shapes_data оттук, за да не се зарежда в паметта при старт
schedule_by_trip, trip_stops_sequence, stop_to_trips_map = {}, {}, {}
# =====================================================================
stop_service_info = {}
trip_stop_sequences_map = {}
weekday_schedule_ids, holiday_schedule_ids = set(), set()
sofia_tz = pytz.timezone('Europe/Sofia')

# --- ГЛАВНИ КЕШОВЕ ЗА МАРШРУТИ ---
precomputed_route_details_cache = {}
routes_by_line_cache = {}

# --- КОНСТАНТИ ---
ARRIVAL_ZONE_METERS = 50
DEPARTURE_ZONE_METERS = 70
HYBRID_TRIGGER_ZONE_METERS = 20
AVG_SPEED_MPS = {'0': 6.9, '3': 5.5, '11': 6.0, 'DEFAULT': 5.5}

# --- ХЕЛПЕР ФУНКЦИИ ---
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    try:
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c
    except (ValueError, TypeError):
        return None

def refresh_realtime_cache_if_needed():
    global trip_updates_feed_cache, vehicle_positions_feed_cache, alerts_feed_cache, last_cache_update_timestamp
    with shared_data_lock:
        if time.time() - last_cache_update_timestamp > CACHE_DURATION_SECONDS:
            try:
                print(f"--- [CACHE] Кешът е изтекъл. Започва обновяване...", file=sys.stderr)
                start_time = time.time()
                
                proxy_url_updates = "https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed=trip-updates"
                response_updates = requests.get(proxy_url_updates, timeout=15)
                if response_updates.status_code == 200:
                    trip_updates_feed_cache = gtfs_realtime_pb2.FeedMessage()
                    trip_updates_feed_cache.ParseFromString(response_updates.content)

                proxy_url_positions = "https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed=vehicle-positions"
                response_positions = requests.get(proxy_url_positions, timeout=15)
                if response_positions.status_code == 200:
                    vehicle_positions_feed_cache = gtfs_realtime_pb2.FeedMessage()
                    vehicle_positions_feed_cache.ParseFromString(response_positions.content)

                proxy_url_alerts = "https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed=alerts"
                response_alerts = requests.get(proxy_url_alerts, timeout=15)
                if response_alerts.status_code == 200:
                    alerts_feed_cache = gtfs_realtime_pb2.FeedMessage()
                    alerts_feed_cache.ParseFromString(response_alerts.content)

                last_cache_update_timestamp = time.time()
                end_time = time.time()
                print(f"--- [CACHE] Обновяването приключи за {(end_time - start_time) * 1000:.2f} мс.", file=sys.stderr)
            except requests.RequestException as e:
                print(f"КРИТИЧНА ГРЕШКА при мрежова заявка към прокси: {e}", file=sys.stderr)

# ============================ ПРОМЯНА (2/5) ============================
# Нова функция и кеш за "мързеливо" зареждане на shapes.txt
shapes_data_cache = {}
shapes_data_lock = threading.Lock()

def get_shape_by_id(shape_id):
    """Зарежда форма от shapes.txt при нужда и я кешира в паметта."""
    global shapes_data_cache
    
    if shape_id in shapes_data_cache:
        return shapes_data_cache[shape_id]
        
    with shapes_data_lock:
        if shape_id in shapes_data_cache:
            return shapes_data_cache[shape_id]

        print(f"--- [Lazy Load] Зареждане на shape '{shape_id}' от файла...", file=sys.stderr)
        shape_points = []
        try:
            with open(f'{BASE_PATH}shapes.txt', mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['shape_id'] == shape_id:
                        shape_points.append([float(row['shape_pt_lat']), float(row['shape_pt_lon'])])
            
            if shape_points:
                shapes_data_cache[shape_id] = shape_points
            return shape_points
        except FileNotFoundError:
            print("КРИТИЧНА ГРЕШКА: shapes.txt не е намерен!", file=sys.stderr)
            return []
        except (ValueError, KeyError) as e:
            print(f"Проблемен ред в shapes.txt. Грешка: {e}", file=sys.stderr)
            return []
# =====================================================================

def get_processed_alerts():
    if not alerts_feed_cache:
        return {}
    # ... (кодът тук е без промяна)
    alerts_by_composite_key = {}
    routes_by_short_name = {}
    for r_id, r_info in routes_data.items():
        name = r_info.get('route_short_name')
        if name:
            if name not in routes_by_short_name:
                routes_by_short_name[name] = []
            routes_by_short_name[name].append(r_info)

    def add_alert_to_route(route_info, alert_text):
        if route_info:
            route_name = route_info.get('route_short_name')
            route_type = route_info.get('route_type')
            if route_name and route_type:
                composite_key = f"{route_name}-{route_type}"
                if composite_key not in alerts_by_composite_key:
                    alerts_by_composite_key[composite_key] = []
                if alert_text not in alerts_by_composite_key[composite_key]:
                    alerts_by_composite_key[composite_key].append(alert_text)

    for entity in alerts_feed_cache.entity:
        if entity.HasField('alert'):
            alert = entity.alert
            if alert.HasField('description_text') and alert.description_text.translation:
                html_text = alert.description_text.translation[0].text
                soup = BeautifulSoup(html_text, "html.parser")
                alert_text = soup.get_text(separator='\n').strip()
            else:
                alert_text = "Няма подробно описание."
            if alert_text:
                pattern = r"(трамвайн\w+|автобусн\w+|тролейбусн\w+)\s+(?:линия|линии)\s+№\s+([\d\s,и]+)"
                matches = re.findall(pattern, alert_text, re.IGNORECASE)
                type_map = { '0': 'трамвайн', '3': 'автобусн', '11': 'тролейбусн' }
                for match in matches:
                    vehicle_type_str, names_str = match
                    vehicle_type_prefix = vehicle_type_str.lower()[:9]
                    route_type_code = None
                    for code, prefix in type_map.items():
                        if vehicle_type_prefix.startswith(prefix):
                            route_type_code = code
                            break
                    if route_type_code:
                        route_names = set(name for name in re.split(r'[\s,и]+', names_str) if name)
                        for name in route_names:
                            if name in routes_by_short_name:
                                for route_info in routes_by_short_name[name]:
                                    if route_info.get('route_type') == route_type_code:
                                        add_alert_to_route(route_info, alert_text)
            for informed_entity in alert.informed_entity:
                if informed_entity.HasField('route_id'):
                    route_info = routes_data.get(informed_entity.route_id)
                    add_alert_to_route(route_info, alert_text)
                elif informed_entity.HasField('trip') and informed_entity.trip.HasField('trip_id'):
                    trip_info = trips_data.get(informed_entity.trip.trip_id)
                    if trip_info and 'route_id' in trip_info:
                        route_info = routes_data.get(trip_info['route_id'])
                        add_alert_to_route(route_info, alert_text)
                elif informed_entity.HasField('stop_id'):
                    trip_ids_for_stop = stop_to_trips_map.get(informed_entity.stop_id, [])
                    route_ids_for_stop = {trips_data[t_id]['route_id'] for t_id in trip_ids_for_stop if t_id in trips_data and 'route_id' in trips_data[t_id]}
                    for route_id in route_ids_for_stop:
                        route_info = routes_data.get(route_id)
                        add_alert_to_route(route_info, alert_text)
    return alerts_by_composite_key

def load_static_data():
    # ============================ ПРОМЯНА (3/5) ============================
    # Премахваме shapes_data от global, защото вече не е глобална променлива
    global routes_data, trips_data, stops_data, active_services, schedule_by_trip, trip_stops_sequence, stop_service_info, stop_to_trips_map, trip_stop_sequences_map, weekday_schedule_ids, holiday_schedule_ids
    # =====================================================================
    try:
        with open(f'{BASE_PATH}routes.txt', mode='r', encoding='utf-8-sig') as f: routes_data = {row['route_id']: row for row in csv.DictReader(f)}

        IMMUNE_TROLLEYBUS_ROUTE_IDS = {'TB10','TB9','TB32','TB1','TB3','TB6','TB7','TB4','TB8','TB2','TB27','TB30','TB21','TB40'}
        for route_id, route_info in routes_data.items():
            if route_info.get('route_type') == '11' and route_id not in IMMUNE_TROLLEYBUS_ROUTE_IDS:
                route_info['route_type'] = '3'
        
        with open(f'{BASE_PATH}trips.txt', mode='r', encoding='utf-8-sig') as f: trips_data = {row['trip_id']: row for row in csv.DictReader(f)}
        used_stop_ids = set()
        with open(f'{BASE_PATH}stop_times.txt', mode='r', encoding='utf-8-sig') as f:
            for row in csv.DictReader(f):
                try:
                    trip_id, stop_id = row['trip_id'], row['stop_id']
                    used_stop_ids.add(stop_id)
                    stop_seq = int(row['stop_sequence'])
                    if trip_id not in schedule_by_trip: schedule_by_trip[trip_id] = {}
                    schedule_by_trip[trip_id][stop_id] = row['arrival_time']
                    if trip_id not in trip_stops_sequence: trip_stops_sequence[trip_id] = []
                    trip_stops_sequence[trip_id].append({'stop_id': stop_id, 'stop_sequence': stop_seq})
                    if trip_id not in trip_stop_sequences_map: trip_stop_sequences_map[trip_id] = {}
                    trip_stop_sequences_map[trip_id][stop_id] = stop_seq
                    if stop_id not in stop_to_trips_map: stop_to_trips_map[stop_id] = []
                    stop_to_trips_map[stop_id].append(trip_id)
                    trip_info = trips_data.get(trip_id)
                    if trip_info:
                        route_info = routes_data.get(trip_info['route_id'])
                        if route_info:
                            if stop_id not in stop_service_info: stop_service_info[stop_id] = {'types': set()}
                            route_type = route_info.get('route_type')
                            type_map = {'0': 'TRAM', '3': 'BUS', '11': 'TROLLEY'}
                            transport_type = type_map.get(route_type)
                            if route_info.get('route_short_name', '').startswith('N'): transport_type = 'NIGHT'
                            if transport_type: stop_service_info[stop_id]['types'].add(transport_type)
                except (ValueError, KeyError) as e: print(f"Проблемен ред в stop_times.txt: {row}. Грешка: {e}", file=sys.stderr)

        for trip_id in trip_stops_sequence: trip_stops_sequence[trip_id].sort(key=lambda x: x['stop_sequence'])
        with open(f'{BASE_PATH}stops.txt', mode='r', encoding='utf-8-sig') as f: stops_data = {row['stop_id']: row for row in csv.DictReader(f) if row['stop_id'] in used_stop_ids}

        # ============================ ПРОМЯНА (4/5) ============================
        # Изтриваме целия блок, който зарежда shapes.txt
        # with open(f'{BASE_PATH}shapes.txt', ... )
        # =====================================================================

        # ... останалата част от функцията е без промяна ...
        active_services.clear()
        with open(f'{BASE_PATH}calendar_dates.txt', 'r', encoding='utf-8-sig') as f:
            calendar_dates_rows = list(csv.DictReader(f))

        today_str = datetime.now(sofia_tz).strftime('%Y%m%d')
        is_today_holiday = any(row['date'] == today_str and row.get('exception_type') == '1' for row in calendar_dates_rows)
        
        all_service_ids_from_trips = {trip['service_id'] for trip in trips_data.values()}
        original_holiday_ids = {row['service_id'] for row in calendar_dates_rows if row.get('exception_type') == '1'}
        original_weekday_ids = all_service_ids_from_trips - original_holiday_ids
        
        if is_today_holiday:
            active_services.update(original_holiday_ids)
        else:
            active_services.update(original_weekday_ids)

        for row in calendar_dates_rows:
            if row['date'] == today_str:
                if row['exception_type'] == '1':
                    active_services.add(row['service_id'])
                elif row['exception_type'] == '2':
                    active_services.discard(row['service_id'])

        for row in calendar_dates_rows:
            date_obj = datetime.strptime(row['date'], '%Y%m%d')
            if date_obj.weekday() >= 5:
                holiday_schedule_ids.add(str(row['service_id']))
            else:
                weekday_schedule_ids.add(str(row['service_id']))
        print(f"Заредени са {len(active_services)} активни услуги. {len(weekday_schedule_ids)} делнични и {len(holiday_schedule_ids)} празнични.", file=sys.stderr)

    except FileNotFoundError as e:
        print(f"КРИТИЧНА ГРЕШКА: Файлът {e.filename} не е намерен.", file=sys.stderr)
        raise
    return True

def parse_gtfs_time(time_str: str, service_date: datetime, tz: pytz.timezone):
    try:
        h, m, s = map(int, time_str.split(':'))
        naive_start_of_day = datetime(service_date.year, service_date.month, service_date.day)
        time_offset = timedelta(hours=h, minutes=m, seconds=s)
        naive_datetime = naive_start_of_day + time_offset
        return tz.localize(naive_datetime)
    except (ValueError, IndexError, TypeError): return None

def precompute_all_route_details():
    global precomputed_route_details_cache
    precomputed_route_details_cache.clear()
    for trip_id, trip_info in trips_data.items():
        shape_id = trip_info.get('shape_id')
        if not shape_id: continue
        # ============================ ПРОМЯНА (5/5) ============================
        # Заменяме директния достъп с извикване на новата функция
        shape_points = get_shape_by_id(shape_id)
        # =====================================================================
        if not shape_points: continue
        if trip_id not in trip_stops_sequence: continue
        stops_list = []
        for s in trip_stops_sequence.get(trip_id, []):
            stop_data = stops_data.get(s['stop_id'])
            if stop_data:
                stop_copy = stop_data.copy()
                stop_copy['stop_sequence'] = s['stop_sequence']
                stop_copy['service_types'] = sorted(list(stop_service_info.get(s['stop_id'], {}).get('types', [])))
                stops_list.append(stop_copy)
        if not stops_list: continue
        precomputed_route_details_cache[trip_id] = {"shape": shape_points, "stops": stops_list}

def precompute_routes_by_line():
    global routes_by_line_cache
    routes_by_line_cache.clear()
    lines_to_trips = {}
    for trip_id, trip_info in trips_data.items():
        route_id = trip_info.get('route_id')
        if not route_id: continue
        route_info = routes_data.get(route_id)
        if not route_info: continue
        line_number = route_info.get('route_short_name')
        route_type = route_info.get('route_type')
        if not line_number or not route_type: continue
        line_key = (line_number, route_type)
        if line_key not in lines_to_trips:
            lines_to_trips[line_key] = []
        lines_to_trips[line_key].append(trip_id)

    for (line_number, route_type), trip_ids in lines_to_trips.items():
        headsign_counts = Counter(trips_data[t_id].get('trip_headsign') for t_id in trip_ids if t_id in trips_data)
        main_headsigns = {headsign for headsign, count in headsign_counts.most_common(2)}
        processed_shapes_for_line = set()
        for trip_id in trip_ids:
            trip_info = trips_data.get(trip_id)
            if not trip_info or trip_info.get('trip_headsign') not in main_headsigns: continue
            shape_id = trip_info.get('shape_id')
            if not shape_id or shape_id in processed_shapes_for_line: continue
            # ============================ ПРОМЯНА (5/5) ============================
            shape_points = get_shape_by_id(shape_id)
            # =====================================================================
            if not shape_points: continue
            stops_list = []
            for s in trip_stops_sequence.get(trip_id, []):
                stop_data = stops_data.get(s['stop_id'])
                if stop_data:
                    stop_copy = stop_data.copy()
                    stop_copy['stop_sequence'] = s['stop_sequence']
                    stop_copy['service_types'] = sorted(list(stop_service_info.get(s['stop_id'], {}).get('types', [])))
                    stops_list.append(stop_copy)
            if not stops_list: continue
            route_variation_data = {
                "direction": trip_info.get('trip_headsign', 'Н/И'),
                "trip_id_sample": trip_id, "shape": shape_points, "stops": stops_list
            }
            if line_number not in routes_by_line_cache: routes_by_line_cache[line_number] = {}
            if route_type not in routes_by_line_cache[line_number]: routes_by_line_cache[line_number][route_type] = []
            routes_by_line_cache[line_number][route_type].append(route_variation_data)
            processed_shapes_for_line.add(shape_id)

# ----------------- СТАРТИРАНЕ НА СЪРВЪРА -----------------
print("--- Сървърът стартира. Зареждане на статични данни...")
load_static_data()
print("--- Статичните данни са заредени. Предварително изчисляване на кешове...")
precompute_all_route_details()
precompute_routes_by_line()
print("--- Кешовете са подготвени. Първоначално зареждане на данни в реално време...")
refresh_realtime_cache_if_needed()
print("--- Сървърът е готов за приемане на заявки. ---")

# ----------------- API ЕНДПОЙНТИ -----------------
# (Всички ендпойнти оттук надолу са без промяна, освен /api/shape/<trip_id>)

@app.route('/api/shape/<trip_id>')
def get_shape_for_trip(trip_id):
    trip_info = trips_data.get(trip_id)
    if not trip_info: return jsonify({"error": "Trip not found"}), 404
    shape_id = trip_info.get('shape_id')
    # ============================ ПРОМЯНА (5/5) ============================
    shape_points = get_shape_by_id(shape_id)
    # =====================================================================
    return jsonify(shape_points)

# ... Копирай останалите си ендпойнти от оригиналния файл тук ...
# Те не се нуждаят от промяна.
@app.route('/api/vehicles_for_stop/<stop_id>')
def get_vehicles_for_stop(stop_id):
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts = get_processed_alerts()
        now_dt = datetime.now(sofia_tz)
        now_ts = int(time.time())
        arrival_predictions = {e.trip_update.trip.trip_id: {stu.stop_id: stu.arrival.time for stu in e.trip_update.stop_time_update if stu.HasField('arrival') and stu.arrival.time > 0} for e in trip_updates_feed_cache.entity if e.HasField('trip_update')} if trip_updates_feed_cache else {}
        vehicle_positions = {e.vehicle.trip.trip_id: e.vehicle for e in vehicle_positions_feed_cache.entity if e.HasField('vehicle')} if vehicle_positions_feed_cache else {}
        stop_info = stops_data.get(stop_id)
        if not stop_info: return jsonify({"error": "Stop not found"}), 404
        stop_code = stop_info.get('stop_code')
        physical_stop_ids = {s_id for s_id, s_data in stops_data.items() if s_data.get('stop_code') == stop_code and stop_code}
        physical_stop_ids.add(stop_id)
        all_arrivals = []
        trip_ids_for_stop = {tid for s_id in physical_stop_ids for tid in stop_to_trips_map.get(s_id, [])}
        with shared_data_lock:
            stale_keys_official = [k for k, v in recent_official_arrivals_cache.items() if now_ts - v > RECENT_OFFICIAL_TTL_SECONDS]
            for k in stale_keys_official: del recent_official_arrivals_cache[k]
            stale_keys_gps = [k for k, v in gps_arrival_cache.items() if now_ts - v > GPS_CACHE_TTL_SECONDS]
            for k in stale_keys_gps: del gps_arrival_cache[k]
        for trip_id in trip_ids_for_stop:
            trip_info = trips_data.get(trip_id)
            if not trip_info: continue
            trip_schedule_for_stops = schedule_by_trip.get(trip_id, {})
            relevant_stop_id = next((s_id for s_id in physical_stop_ids if s_id in trip_schedule_for_stops), None)
            if not relevant_stop_id: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            eta_minutes, prediction_source, is_live_data = None, None, False
            predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(relevant_stop_id)
            if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60:
                eta_minutes = max(0, round((predicted_arrival_ts - now_ts) / 60))
                prediction_source = "official"
                is_live_data = True
                if eta_minutes == 0:
                    with shared_data_lock: recent_official_arrivals_cache[(trip_id, relevant_stop_id)] = now_ts
            elif trip_id in vehicle_positions:
                is_live_data = True
                with shared_data_lock:
                    if (trip_id, relevant_stop_id) in recent_official_arrivals_cache: continue
                vehicle = vehicle_positions[trip_id]
                target_stop_info = stops_data.get(relevant_stop_id)
                distance_to_our_stop = None
                if vehicle.HasField('position') and target_stop_info and target_stop_info.get('stop_lat'):
                    distance_to_our_stop = haversine_distance(vehicle.position.latitude, vehicle.position.longitude, float(target_stop_info['stop_lat']), float(target_stop_info['stop_lon']))
                cache_key = (trip_id, relevant_stop_id)
                with shared_data_lock: was_in_arrival_zone = cache_key in gps_arrival_cache
                if was_in_arrival_zone and distance_to_our_stop is not None and distance_to_our_stop > DEPARTURE_ZONE_METERS:
                    with shared_data_lock:
                        if cache_key in gps_arrival_cache: del gps_arrival_cache[cache_key]
                    continue
                if distance_to_our_stop is not None and distance_to_our_stop < ARRIVAL_ZONE_METERS:
                    eta_minutes, prediction_source = 0, "hybrid"
                    if not was_in_arrival_zone:
                        with shared_data_lock: gps_arrival_cache[cache_key] = now_ts
                elif was_in_arrival_zone:
                     eta_minutes, prediction_source = 0, "hybrid"
                else:
                    use_hybrid = False
                    next_gps_stop_id = vehicle.stop_id if vehicle.HasField('stop_id') else None
                    if next_gps_stop_id and vehicle.HasField('position'):
                        our_stop_seq = trip_stop_sequences_map.get(trip_id, {}).get(relevant_stop_id)
                        next_stop_seq = trip_stop_sequences_map.get(trip_id, {}).get(next_gps_stop_id)
                        if our_stop_seq is not None and next_stop_seq is not None and next_stop_seq < our_stop_seq:
                            prev_stop_info = stops_data.get(next_gps_stop_id)
                            if prev_stop_info and prev_stop_info.get('stop_lat'):
                                dist_to_prev_stop = haversine_distance(vehicle.position.latitude, vehicle.position.longitude, float(prev_stop_info['stop_lat']), float(prev_stop_info['stop_lon']))
                                if dist_to_prev_stop is not None and dist_to_prev_stop < HYBRID_TRIGGER_ZONE_METERS:
                                    use_hybrid = True
                    if use_hybrid:
                        route_type = route_info.get('route_type', 'DEFAULT')
                        avg_speed = AVG_SPEED_MPS.get(route_type, AVG_SPEED_MPS['DEFAULT'])
                        vehicle_speed_mps = vehicle.position.speed if vehicle.position.HasField('speed') and vehicle.position.speed > 1 else avg_speed
                        if distance_to_our_stop is not None and vehicle_speed_mps > 0:
                            eta_seconds = distance_to_our_stop / vehicle_speed_mps
                            eta_minutes, prediction_source = max(0, round(eta_seconds / 60)), "hybrid"
            if prediction_source is None:
                if trip_info.get('service_id') in active_services:
                    scheduled_time_str = trip_schedule_for_stops.get(relevant_stop_id, "")
                    scheduled_dt_aware = parse_gtfs_time(scheduled_time_str, now_dt, sofia_tz)
                    if scheduled_dt_aware and now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                        eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60))
                        prediction_source = "schedule"
                        is_live_data = False
            if prediction_source is not None:
                route_short_name = route_info.get('route_short_name', 'Н/А')
                route_type = route_info.get('route_type')
                alert_messages = processed_alerts.get(f"{route_short_name}-{route_type}")
                all_arrivals.append({ "trip_id": trip_id, "route_name": route_short_name, "route_type": route_type, "destination": trip_info.get('trip_headsign', 'Н/И'), "eta_minutes": eta_minutes, "prediction_source": prediction_source, "is_live": is_live_data, "alerts": alert_messages })
        all_arrivals.sort(key=lambda x: (not x['is_live'], x['eta_minutes']))
        return jsonify(all_arrivals)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_vehicles_for_stop: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500
        
# ... (и всички останали, които не съм включил тук, за краткост)

@app.route('/api/bulk_arrivals_for_stops', methods=['POST'])
def get_bulk_arrivals_for_stops():
    try:
        refresh_realtime_cache_if_needed()
        stop_codes_to_query = set(request.json.get('stop_codes', []));
        if not stop_codes_to_query: return jsonify({})
        now_dt = datetime.now(sofia_tz); now_ts = int(time.time())
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
        bulk_results = {}; trips_to_check = set()
        for code in stop_codes_to_query:
            for stop_id in stop_code_to_ids_map.get(code, []): trips_to_check.update(stop_to_trips_map.get(stop_id, []))
        for trip_id in trips_to_check:
            trip_info = trips_data.get(trip_id)
            if not trip_info or trip_info.get('service_id') not in active_services: continue
            for stop_id_in_schedule, scheduled_time_str in schedule_by_trip.get(trip_id, {}).items():
                stop_details = stops_data.get(stop_id_in_schedule)
                if not stop_details: continue
                stop_code = stop_details.get('stop_code')
                if stop_code in stop_codes_to_query:
                    is_upcoming = False
                    predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(stop_id_in_schedule)
                    if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60: is_upcoming = True
                    else:
                        scheduled_dt_aware = parse_gtfs_time(scheduled_time_str, now_dt, sofia_tz)
                        if scheduled_dt_aware and now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2): is_upcoming = True
                    if is_upcoming:
                        if stop_code not in bulk_results: bulk_results[stop_code] = {'arrivals': set()}
                        route_info = routes_data.get(trip_info['route_id'])
                        route_type_str = route_info.get('route_type', '')
                        route_name = route_info.get('route_short_name', '')
                        transport_type = 'BUS'
                        if route_name.startswith('N'): transport_type = 'NIGHT'
                        elif route_type_str == '0': transport_type = 'TRAM'
                        elif route_type_str == '11': transport_type = 'TROLLEY'
                        bulk_results[stop_code]['arrivals'].add(transport_type)
        final_results = {code: {'arrivals': list(data['arrivals'])} for code, data in bulk_results.items()}
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_arrivals_for_stops: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/bulk_detailed_arrivals', methods=['POST'])
def get_bulk_detailed_arrivals():
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts = get_processed_alerts()
        stop_codes_to_query = set(request.json.get('stop_codes', []));
        if not stop_codes_to_query: return jsonify({})
        now_dt = datetime.now(sofia_tz); now_ts = int(time.time())
        arrival_predictions = {}
        if trip_updates_feed_cache:
            for entity in trip_updates_feed_cache.entity:
                if entity.HasField('trip_update'):
                    update = entity.trip_update; trip_id = update.trip.trip_id
                    if trip_id not in arrival_predictions: arrival_predictions[trip_id] = {}
                    for stu in update.stop_time_update:
                        if stu.HasField('arrival') and stu.arrival.time > 0: arrival_predictions[trip_id][stu.stop_id] = stu.arrival.time
        vehicle_positions = {e.vehicle.trip.trip_id: e.vehicle for e in vehicle_positions_feed_cache.entity if e.HasField('vehicle')} if vehicle_positions_feed_cache else {}
        final_results = {code: [] for code in stop_codes_to_query}; relevant_stop_ids = set(); stop_id_to_code_map = {}
        for s_id, s_data in stops_data.items():
            code = s_data.get('stop_code')
            if code in stop_codes_to_query: relevant_stop_ids.add(s_id); stop_id_to_code_map[s_id] = code
        trips_to_check = set()
        for stop_id in relevant_stop_ids: trips_to_check.update(stop_to_trips_map.get(stop_id, []))
        for trip_id in trips_to_check:
            trip_info = trips_data.get(trip_id)
            if not trip_info: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            for stop_id_in_schedule, scheduled_time_str in schedule_by_trip.get(trip_id, {}).items():
                if stop_id_in_schedule in relevant_stop_ids:
                    eta_minutes, prediction_source, is_live = -1, None, False
                    predicted_arrival_ts = arrival_predictions.get(trip_id, {}).get(stop_id_in_schedule)
                    if predicted_arrival_ts and predicted_arrival_ts > now_ts - 60:
                        eta_minutes = max(0, round((predicted_arrival_ts - now_ts) / 60)); prediction_source = "official"; is_live = True
                    elif trip_id in vehicle_positions:
                         prediction_source = "hybrid"; is_live = True
                    if not is_live:
                         if trip_info.get('service_id') in active_services:
                            scheduled_dt_aware = parse_gtfs_time(scheduled_time_str, now_dt, sofia_tz)
                            if scheduled_dt_aware and now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                                eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60)); prediction_source = "schedule"
                    else:
                        if eta_minutes == -1:
                           scheduled_dt_aware = parse_gtfs_time(scheduled_time_str, now_dt, sofia_tz)
                           if scheduled_dt_aware and now_dt < scheduled_dt_aware < now_dt + timedelta(hours=2):
                               eta_minutes = max(0, round((scheduled_dt_aware - now_dt).total_seconds() / 60))
                    if prediction_source and eta_minutes != -1:
                        route_short_name = route_info.get('route_short_name', 'Н/А')
                        route_type = route_info.get('route_type')
                        alert_messages = processed_alerts.get(f"{route_short_name}-{route_type}")
                        arrival_object = {
                            "trip_id": trip_id, "route_name": route_short_name, "route_type": route_type,
                            "destination": trip_info.get('trip_headsign', 'Н/И'), "eta_minutes": eta_minutes,
                            "prediction_source": prediction_source, "is_live": is_live, "alerts": alert_messages
                        }
                        stop_code = stop_id_to_code_map[stop_id_in_schedule]
                        final_results[stop_code].append(arrival_object)
        for code in final_results: final_results[code].sort(key=lambda x: (not x['is_live'], x['eta_minutes']))
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_detailed_arrivals: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/schedule_for_stop/<stop_code>')
def get_schedule_for_stop(stop_code):
    try:
        relevant_stop_ids = {s_id for s_id, s_data in stops_data.items() if s_data.get('stop_code') == stop_code}
        if not relevant_stop_ids:
            return jsonify({"error": "Stop with this code not found"}), 404
        schedule = {"weekday": {}, "holiday": {}}
        trip_ids_for_stop = {tid for s_id in relevant_stop_ids for tid in stop_to_trips_map.get(s_id, [])}
        for trip_id in trip_ids_for_stop:
            trip_info = trips_data.get(trip_id)
            if not trip_info: continue
            service_id = str(trip_info['service_id'])
            def add_to_schedule(target_schedule):
                route_info = routes_data.get(trip_info['route_id'])
                if not route_info: return
                route_name = route_info.get('route_short_name', 'Н/А')
                destination = trip_info.get('trip_headsign', 'Н/И')
                route_type = route_info.get('route_type', '3')
                arrival_time = None
                trip_schedule_for_stops = schedule_by_trip.get(trip_id, {})
                for stop_id in relevant_stop_ids:
                    if stop_id in trip_schedule_for_stops:
                        arrival_time = trip_schedule_for_stops[stop_id]
                        break
                if not arrival_time: return
                if route_name not in target_schedule:
                    target_schedule[route_name] = {}
                if destination not in target_schedule[route_name]:
                    target_schedule[route_name][destination] = {"times": [], "route_type": route_type}
                target_schedule[route_name][destination]["times"].append(arrival_time)
            if service_id in holiday_schedule_ids:
                add_to_schedule(schedule["holiday"])
            elif service_id in weekday_schedule_ids:
                add_to_schedule(schedule["weekday"])
        for schedule_type in schedule.values():
            for route in schedule_type.values():
                for dest_data in route.values():
                    dest_data["times"] = sorted(list(set(dest_data["times"])), key=lambda t: tuple(map(int, t.split(':'))))
        return jsonify(schedule)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_schedule_for_stop: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

# ---- Останалите ендпойнти ----

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
                        "latitude": vehicle.position.latitude if vehicle.HasField('position') else None,
                        "longitude": vehicle.position.longitude if vehicle.HasField('position') else None,
                        "trip_id": trip_id, "route_name": route_info.get('route_short_name'),
                        "route_type": route_info.get('route_type', ''),
                        "destination": trip_info.get('trip_headsign', 'Н/И'),
                        "next_stop_id": vehicle.stop_id if vehicle.HasField('stop_id') else None,
                        "stop_sequence": vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else None
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
    stops_list = []
    for s in trip_stops_sequence.get(trip_id, []):
        stop_data = stops_data.get(s['stop_id'])
        if stop_data:
            stop_copy = stop_data.copy()
            stop_copy['stop_sequence'] = s['stop_sequence']
            stops_list.append(stop_copy)
    return jsonify(stops_list)

@app.route('/api/all_routes')
def get_all_routes():
    return jsonify(list(routes_data.values()))

@app.route('/api/all_stops')
def get_all_stops():
    enriched_stops = []
    for stop_id, stop_data in stops_data.items():
        info = stop_service_info.get(stop_id, {})
        stop_copy = stop_data.copy()
        stop_copy['service_types'] = sorted(list(info.get('types', [])))
        enriched_stops.append(stop_copy)
    return jsonify(enriched_stops)

@app.route('/api/all_lines_structured')
def get_all_lines_structured():
    try:
        structured_lines = {}
        for trip_id, trip_info in trips_data.items():
            route_id = trip_info.get('route_id')
            if not route_id: continue
            if route_id not in structured_lines:
                route_info = routes_data.get(route_id)
                if not route_info: continue
                route_short_name = route_info.get('route_short_name', 'Н/А')
                route_type_code = route_info.get('route_type', '3')
                transport_type_str = 'BUS'
                if route_short_name.startswith('N'): transport_type_str = 'NIGHT'
                elif route_type_code == '0': transport_type_str = 'TRAM'
                elif route_type_code == '11': transport_type_str = 'TROLLEY'
                elif route_type_code in ['1', '2']: transport_type_str = 'METRO'
                structured_lines[route_id] = {
                    "line_name": route_short_name,
                    "transport_type": transport_type_str,
                    "directions": {}
                }
            direction_id = trip_info.get('direction_id', '0')
            if direction_id not in structured_lines[route_id]["directions"]:
                structured_lines[route_id]["directions"][direction_id] = {
                    "headsign": trip_info.get('trip_headsign', 'Н/И'),
                    "example_trip_id": trip_id
                }
        final_list = []
        for route_id, line_data in structured_lines.items():
            line_data["directions"] = list(line_data["directions"].values())
            final_list.append(line_data)
        return jsonify(final_list)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_all_lines_structured: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500
        
@app.route('/api/line_details/<line_number>/<route_type_code>')
def get_line_details(line_number, route_type_code):
    line_data = routes_by_line_cache.get(line_number, {}).get(route_type_code)
    if not line_data:
        return jsonify({"error": f"Няма данни за линия номер {line_number} от тип {route_type_code}."}), 404
    return jsonify(line_data)

@app.route('/api/full_route_view/<trip_id>')
def get_full_route_view(trip_id):
    try:
        cached_data = precomputed_route_details_cache.get(trip_id)
        if not cached_data:
            return jsonify({"error": f"Static route details for trip {trip_id} not found in cache."}), 404
        trip_info = trips_data.get(trip_id)
        if not trip_info:
             return jsonify({"error": f"Trip info for {trip_id} not found."}), 404
        route_info = routes_data.get(trip_info.get('route_id'))
        if not route_info:
            return jsonify({"error": f"Route info for trip {trip_id} not found."}), 404
        route_name_to_fetch = route_info.get('route_short_name')
        refresh_realtime_cache_if_needed()
        live_vehicles = []
        if vehicle_positions_feed_cache:
            for entity in vehicle_positions_feed_cache.entity:
                if entity.HasField('vehicle'):
                    vehicle = entity.vehicle
                    v_trip_id = vehicle.trip.trip_id
                    v_trip_info = trips_data.get(v_trip_id)
                    if not v_trip_info: continue
                    v_route_info = routes_data.get(v_trip_info.get('route_id'))
                    if v_route_info and v_route_info.get('route_short_name') == route_name_to_fetch:
                        live_vehicles.append({
                            "latitude": vehicle.position.latitude if vehicle.HasField('position') else 0,
                            "longitude": vehicle.position.longitude if vehicle.HasField('position') else 0,
                            "trip_id": v_trip_id,
                            "route_name": v_route_info.get('route_short_name'),
                            "route_type": v_route_info.get('route_type', ''),
                            "destination": v_trip_info.get('trip_headsign', 'Н/И')
                        })
        full_response = {
            "shape": cached_data["shape"],
            "stops": cached_data["stops"],
            "vehicles": live_vehicles
        }
        return jsonify(full_response)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_full_route_view: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/static_route_view/<trip_id>')
def get_static_route_view(trip_id):
    cached_data = precomputed_route_details_cache.get(trip_id)
    if not cached_data:
        return jsonify({"error": f"Static route details for trip {trip_id} not found in cache."}), 404
    return jsonify({"shape": cached_data.get("shape", []),"stops": cached_data.get("stops", [])})

@app.route('/api/debug/alerts_raw')
def debug_alerts_raw():
    try:
        refresh_realtime_cache_if_needed()
        if not alerts_feed_cache:
            return jsonify({"error": "Alerts фийдът не е зареден."}), 500
        text_output = str(alerts_feed_cache)
        return Response(text_output, mimetype='text/plain; charset=utf-8')
    except Exception as e:
        return jsonify({"error": f"Възникна грешка: {e}"}), 500

@app.route('/api/debug/alerts')
def debug_alerts():
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts = get_processed_alerts()
        if not processed_alerts:
            return jsonify({"status": "OK","message": "Няма активни предупреждения в момента.","data": {}})
        return jsonify({"status": "OK","message": f"Намерени са {len(processed_alerts)} активни предупреждения.","data": processed_alerts})
    except Exception as e:

        return jsonify({"error": f"Възникна грешка: {e}"}), 500
