import os
import requests
from flask import Flask, jsonify, request, Response
from google.transit import gtfs_realtime_pb2
import csv
from datetime import datetime, timedelta
import pytz
from flask_cors import CORS
import time, sys, threading, math, traceback, re
from bs4 import BeautifulSoup
from collections import Counter

app = Flask(__name__)
CORS(app)

BASE_PATH = os.path.dirname(os.path.abspath(__file__)) + "/"

# --- Глобални променливи ---
CACHE_DURATION_SECONDS = 15
recent_official_arrivals_cache, gps_arrival_cache = {}, {}
RECENT_OFFICIAL_TTL_SECONDS, GPS_CACHE_TTL_SECONDS = 90, 300
shared_data_lock = threading.Lock()
trip_updates_feed_cache, vehicle_positions_feed_cache, alerts_feed_cache = None, None, None
last_cache_update_timestamp = 0
routes_data, trips_data, stops_data, active_services = {}, {}, {}, set()
schedule_by_trip, trip_stops_sequence, stop_to_trips_map = {}, {}, {}
stop_service_info, trip_stop_sequences_map = {}, {}
weekday_schedule_ids, holiday_schedule_ids = set(), set()
sofia_tz = pytz.timezone('Europe/Sofia')
precomputed_route_details_cache, routes_by_line_cache = None, None
initialization_lock = threading.Lock()
shapes_data_cache, shapes_data_lock = {}, threading.Lock()
ARRIVAL_ZONE_METERS, DEPARTURE_ZONE_METERS, HYBRID_TRIGGER_ZONE_METERS = 50, 70, 20
AVG_SPEED_MPS = {'0': 6.9, '3': 5.5, '11': 6.0, 'DEFAULT': 5.5}

# --- Хелпър функции ---
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371000
    try:
        phi1, phi2, delta_phi, delta_lambda = map(math.radians, [lat1, lat2, lat2 - lat1, lon2 - lon1])
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    except (ValueError, TypeError): return None

def refresh_realtime_cache_if_needed():
    global trip_updates_feed_cache, vehicle_positions_feed_cache, alerts_feed_cache, last_cache_update_timestamp
    with shared_data_lock:
        if time.time() - last_cache_update_timestamp > CACHE_DURATION_SECONDS:
            try:
                print(f"--- [CACHE] Обновяване на live данни...", file=sys.stderr)
                start_time = time.time()
                for feed_name in ["trip-updates", "vehicle-positions", "alerts"]:
                    url = f"https://sofia-traffic-proxy.pavel-manahilov-box.workers.dev/?feed={feed_name}"
                    response = requests.get(url, timeout=15)
                    if response.status_code == 200:
                        feed = gtfs_realtime_pb2.FeedMessage()
                        feed.ParseFromString(response.content)
                        if feed_name == "trip-updates": trip_updates_feed_cache = feed
                        elif feed_name == "vehicle-positions": vehicle_positions_feed_cache = feed
                        else: alerts_feed_cache = feed
                last_cache_update_timestamp = time.time()
                print(f"--- [CACHE] Обновяването приключи за {(time.time() - start_time) * 1000:.2f} мс.", file=sys.stderr)
            except requests.RequestException as e: print(f"КРИТИЧНА ГРЕШКА при мрежова заявка: {e}", file=sys.stderr)

def get_shape_by_id(shape_id):
    if shape_id in shapes_data_cache: return shapes_data_cache[shape_id]
    with shapes_data_lock:
        if shape_id in shapes_data_cache: return shapes_data_cache[shape_id]
        print(f"--- [Lazy Load] Зареждане на shape '{shape_id}'...", file=sys.stderr)
        shape_points = []
        try:
            with open(f'{BASE_PATH}shapes.txt', mode='r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row['shape_id'] == shape_id:
                        shape_points.append([float(row['shape_pt_lat']), float(row['shape_pt_lon'])])
            if shape_points: shapes_data_cache[shape_id] = shape_points
            return shape_points
        except Exception as e:
            print(f"Грешка при четене на shapes.txt: {e}", file=sys.stderr)
            return []

def get_processed_alerts():
    if not alerts_feed_cache: return {}
    alerts_by_composite_key = {}
    routes_by_short_name = {}
    for r_id, r_info in routes_data.items():
        name = r_info.get('route_short_name')
        if name and name not in routes_by_short_name: routes_by_short_name[name] = []
        if name: routes_by_short_name[name].append(r_info)
    def add_alert_to_route(route_info, alert_text):
        if route_info:
            r_name, r_type = route_info.get('route_short_name'), route_info.get('route_type')
            if r_name and r_type:
                key = f"{r_name}-{r_type}"
                if key not in alerts_by_composite_key: alerts_by_composite_key[key] = []
                if alert_text not in alerts_by_composite_key[key]: alerts_by_composite_key[key].append(alert_text)
    for entity in alerts_feed_cache.entity:
        if entity.HasField('alert'):
            alert = entity.alert; alert_text = "Няма подробно описание."
            if alert.HasField('description_text') and alert.description_text.translation:
                alert_text = BeautifulSoup(alert.description_text.translation[0].text, "html.parser").get_text(separator='\n').strip()
            if alert_text:
                for v_type, names in re.findall(r"(трамвайн\w+|автобусн\w+|тролейбусн\w+)\s+.*?№\s+([\d\s,и]+)", alert_text, re.I):
                    for code, prefix in {'0':'трамвайн', '3':'автобусн', '11':'тролейбусн'}.items():
                        if v_type.lower().startswith(prefix):
                            for name in set(n for n in re.split(r'[\s,и]+', names) if n):
                                for r_info in routes_by_short_name.get(name, []):
                                    if r_info.get('route_type') == code: add_alert_to_route(r_info, alert_text)
                            break
            for ie in alert.informed_entity:
                if ie.HasField('route_id'): add_alert_to_route(routes_data.get(ie.route_id), alert_text)
                elif ie.HasField('trip') and ie.trip.HasField('trip_id'):
                    trip_info = trips_data.get(ie.trip.trip_id)
                    if trip_info and 'route_id' in trip_info: add_alert_to_route(routes_data.get(trip_info['route_id']), alert_text)
                elif ie.HasField('stop_id'):
                    for t_id in stop_to_trips_map.get(ie.stop_id, []):
                        if t_id in trips_data and 'route_id' in trips_data[t_id]: add_alert_to_route(routes_data.get(trips_data[t_id]['route_id']), alert_text)
    return alerts_by_composite_key

def load_static_data():
    global routes_data, trips_data, stops_data, active_services, schedule_by_trip, trip_stops_sequence, stop_service_info, stop_to_trips_map, trip_stop_sequences_map, weekday_schedule_ids, holiday_schedule_ids
    try:
        with open(f'{BASE_PATH}routes.txt', mode='r', encoding='utf-8-sig') as f: routes_data = {r['route_id']: r for r in csv.DictReader(f)}
        IMMUNE_TROLLEYBUS_ROUTE_IDS = {'TB10','TB9','TB32','TB1','TB3','TB6','TB7','TB4','TB8','TB2','TB27','TB30','TB21','TB40'}
        for r_id, r_info in routes_data.items():
            if r_info.get('route_type') == '11' and r_id not in IMMUNE_TROLLEYBUS_ROUTE_IDS: r_info['route_type'] = '3'
        with open(f'{BASE_PATH}trips.txt', mode='r', encoding='utf-8-sig') as f: trips_data = {r['trip_id']: r for r in csv.DictReader(f)}
        used_stop_ids = set()
        with open(f'{BASE_PATH}stop_times.txt', mode='r', encoding='utf-8-sig') as f:
            for r in csv.DictReader(f):
                try:
                    t_id, s_id = r['trip_id'], r['stop_id']
                    used_stop_ids.add(s_id)
                    s_seq = int(r['stop_sequence'])
                    if t_id not in schedule_by_trip: schedule_by_trip[t_id] = {}
                    schedule_by_trip[t_id][s_id] = r['arrival_time']
                    if t_id not in trip_stops_sequence: trip_stops_sequence[t_id] = []
                    trip_stops_sequence[t_id].append({'stop_id': s_id, 'stop_sequence': s_seq})
                    if t_id not in trip_stop_sequences_map: trip_stop_sequences_map[t_id] = {}
                    trip_stop_sequences_map[t_id][s_id] = s_seq
                    if s_id not in stop_to_trips_map: stop_to_trips_map[s_id] = []
                    stop_to_trips_map[s_id].append(t_id)
                    trip_info = trips_data.get(t_id)
                    if trip_info and s_id not in stop_service_info: stop_service_info[s_id] = {'types': set()}
                    if trip_info and 'route_id' in trip_info:
                        route_info = routes_data.get(trip_info['route_id'])
                        if route_info:
                            r_type = route_info.get('route_type')
                            transport_type = {'0':'TRAM', '3':'BUS', '11':'TROLLEY'}.get(r_type)
                            if route_info.get('route_short_name','').startswith('N'): transport_type = 'NIGHT'
                            if transport_type: stop_service_info[s_id]['types'].add(transport_type)
                except (ValueError, KeyError) as e: print(f"Проблемен ред в stop_times.txt: {r}. Грешка: {e}", file=sys.stderr)
        for t_id in trip_stops_sequence: trip_stops_sequence[t_id].sort(key=lambda x: x['stop_sequence'])
        with open(f'{BASE_PATH}stops.txt', mode='r', encoding='utf-8-sig') as f: stops_data = {r['stop_id']: r for r in csv.DictReader(f) if r['stop_id'] in used_stop_ids}
        with open(f'{BASE_PATH}calendar_dates.txt', 'r', encoding='utf-8-sig') as f: calendar_dates_rows = list(csv.DictReader(f))
        active_services.clear()
        today_str = datetime.now(sofia_tz).strftime('%Y%m%d')
        is_today_holiday = any(r['date'] == today_str and r.get('exception_type') == '1' for r in calendar_dates_rows)
        all_service_ids = {t['service_id'] for t in trips_data.values()}
        holiday_service_ids = {r['service_id'] for r in calendar_dates_rows if r.get('exception_type') == '1'}
        if is_today_holiday: active_services.update(holiday_service_ids)
        else: active_services.update(all_service_ids - holiday_service_ids)
        for r in calendar_dates_rows:
            if r['date'] == today_str:
                if r['exception_type'] == '1': active_services.add(r['service_id'])
                elif r['exception_type'] == '2': active_services.discard(r['service_id'])
        for r in calendar_dates_rows:
            (holiday_schedule_ids if datetime.strptime(r['date'], '%Y%m%d').weekday() >= 5 else weekday_schedule_ids).add(str(r['service_id']))
        print(f"Заредени са {len(active_services)} активни услуги.", file=sys.stderr)
    except FileNotFoundError as e:
        print(f"КРИТИЧНА ГРЕШКА: Файлът {e.filename} не е намерен.", file=sys.stderr)
        raise

def parse_gtfs_time(time_str, service_date, tz):
    try:
        h, m, s = map(int, time_str.split(':'))
        return tz.localize(datetime(service_date.year, service_date.month, service_date.day) + timedelta(hours=h, minutes=m, seconds=s))
    except: return None

def _build_precomputed_route_details():
    global precomputed_route_details_cache
    for t_id, t_info in trips_data.items():
        s_id = t_info.get('shape_id')
        if not s_id: continue
        shape_points = get_shape_by_id(s_id)
        if not shape_points or t_id not in trip_stops_sequence: continue
        stops_list = [dict(stops_data.get(s['stop_id']), **{'stop_sequence': s['stop_sequence'], 'service_types': sorted(list(stop_service_info.get(s['stop_id'],{}).get('types',[])))}) for s in trip_stops_sequence.get(t_id, []) if stops_data.get(s['stop_id'])]
        if stops_list: precomputed_route_details_cache[t_id] = {"shape": shape_points, "stops": stops_list}

def _build_routes_by_line():
    global routes_by_line_cache
    lines_to_trips = {}
    for t_id, t_info in trips_data.items():
        route_info = routes_data.get(t_info.get('route_id'))
        if route_info:
            line_key = (route_info.get('route_short_name'), route_info.get('route_type'))
            if all(line_key):
                if line_key not in lines_to_trips: lines_to_trips[line_key] = []
                lines_to_trips[line_key].append(t_id)
    for (line_num, r_type), t_ids in lines_to_trips.items():
        headsigns = Counter(trips_data[t_id].get('trip_headsign') for t_id in t_ids if t_id in trips_data)
        main_headsigns = {h for h, c in headsigns.most_common(2)}
        processed_shapes = set()
        for t_id in t_ids:
            t_info = trips_data.get(t_id)
            if not t_info or t_info.get('trip_headsign') not in main_headsigns: continue
            s_id = t_info.get('shape_id')
            if not s_id or s_id in processed_shapes: continue
            shape_points = get_shape_by_id(s_id)
            if not shape_points: continue
            stops_list = [dict(stops_data.get(s['stop_id']), **{'stop_sequence': s['stop_sequence'], 'service_types': sorted(list(stop_service_info.get(s['stop_id'],{}).get('types',[])))}) for s in trip_stops_sequence.get(t_id,[]) if stops_data.get(s['stop_id'])]
            if not stops_list: continue
            variation_data = {"direction":t_info.get('trip_headsign','Н/И'), "trip_id_sample":t_id, "shape":shape_points, "stops":stops_list}
            if line_num not in routes_by_line_cache: routes_by_line_cache[line_num] = {}
            if r_type not in routes_by_line_cache[line_num]: routes_by_line_cache[line_num][r_type] = []
            routes_by_line_cache[line_num][r_type].append(variation_data)
            processed_shapes.add(s_id)

def ensure_route_details_cache():
    if precomputed_route_details_cache is None:
        with initialization_lock:
            if precomputed_route_details_cache is None:
                print("--- [Lazy Init] Изграждане на precomputed_route_details_cache...")
                start_time = time.time()
                global precomputed_route_details_cache
                precomputed_route_details_cache = {}
                _build_precomputed_route_details()
                print(f"--- [Lazy Init] precomputed_route_details_cache е готов за {time.time() - start_time:.2f} сек.")

def ensure_routes_by_line_cache():
    if routes_by_line_cache is None:
        with initialization_lock:
            if routes_by_line_cache is None:
                print("--- [Lazy Init] Изграждане на routes_by_line_cache...")
                start_time = time.time()
                global routes_by_line_cache
                routes_by_line_cache = {}
                _build_routes_by_line()
                print(f"--- [Lazy Init] routes_by_line_cache е готов за {time.time() - start_time:.2f} сек.")

# ----------------- СТАРТИРАНЕ НА СЪРВЪРА -----------------
print("--- Сървърът стартира. Зареждане на основни статични данни...")
load_static_data()
print("--- Основните данни са заредени. Първоначално зареждане на данни в реално време...")
refresh_realtime_cache_if_needed()
print("--- Сървърът е готов. Тежките кешове ще се изградят при първа нужда. ---")

# ----------------- API ЕНДПОЙНТИ -----------------
@app.route('/api/vehicles_for_stop/<stop_id>')
def get_vehicles_for_stop(stop_id):
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts, now_dt, now_ts = get_processed_alerts(), datetime.now(sofia_tz), int(time.time())
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
            for k in [k for k,v in recent_official_arrivals_cache.items() if now_ts - v > RECENT_OFFICIAL_TTL_SECONDS]: del recent_official_arrivals_cache[k]
            for k in [k for k,v in gps_arrival_cache.items() if now_ts - v > GPS_CACHE_TTL_SECONDS]: del gps_arrival_cache[k]
        for t_id in trip_ids_for_stop:
            trip_info = trips_data.get(t_id)
            if not trip_info: continue
            trip_schedule = schedule_by_trip.get(t_id, {})
            rel_stop_id = next((s_id for s_id in physical_stop_ids if s_id in trip_schedule), None)
            if not rel_stop_id: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            eta_min, pred_src, is_live = None, None, False
            pred_ts = arrival_predictions.get(t_id, {}).get(rel_stop_id)
            if pred_ts and pred_ts > now_ts - 60:
                eta_min, pred_src, is_live = max(0, round((pred_ts - now_ts) / 60)), "official", True
                if eta_min == 0:
                    with shared_data_lock: recent_official_arrivals_cache[(t_id, rel_stop_id)] = now_ts
            elif t_id in vehicle_positions:
                is_live = True
                with shared_data_lock:
                    if (t_id, rel_stop_id) in recent_official_arrivals_cache: continue
                vehicle, target_stop = vehicle_positions[t_id], stops_data.get(rel_stop_id)
                dist_to_stop = haversine_distance(vehicle.position.latitude, vehicle.position.longitude, float(target_stop['stop_lat']), float(target_stop['stop_lon'])) if vehicle.HasField('position') and target_stop and target_stop.get('stop_lat') else None
                cache_key = (t_id, rel_stop_id)
                with shared_data_lock: was_in_zone = cache_key in gps_arrival_cache
                if was_in_zone and dist_to_stop is not None and dist_to_stop > DEPARTURE_ZONE_METERS:
                    with shared_data_lock:
                        if cache_key in gps_arrival_cache: del gps_arrival_cache[cache_key]
                    continue
                if (dist_to_stop is not None and dist_to_stop < ARRIVAL_ZONE_METERS) or was_in_zone:
                    eta_min, pred_src = 0, "hybrid"
                    if not was_in_zone:
                        with shared_data_lock: gps_arrival_cache[cache_key] = now_ts
                else:
                    use_hybrid = False
                    next_gps_stop = vehicle.stop_id if vehicle.HasField('stop_id') else None
                    if next_gps_stop and vehicle.HasField('position'):
                        our_seq, next_seq = trip_stop_sequences_map.get(t_id, {}).get(rel_stop_id), trip_stop_sequences_map.get(t_id, {}).get(next_gps_stop)
                        if our_seq is not None and next_seq is not None and next_seq < our_seq:
                            prev_stop = stops_data.get(next_gps_stop)
                            if prev_stop and prev_stop.get('stop_lat'):
                                dist_to_prev = haversine_distance(vehicle.position.latitude, vehicle.position.longitude, float(prev_stop['stop_lat']), float(prev_stop['stop_lon']))
                                if dist_to_prev is not None and dist_to_prev < HYBRID_TRIGGER_ZONE_METERS: use_hybrid = True
                    if use_hybrid:
                        r_type = route_info.get('route_type', 'DEFAULT')
                        avg_speed = AVG_SPEED_MPS.get(r_type, AVG_SPEED_MPS['DEFAULT'])
                        v_speed = vehicle.position.speed if vehicle.position.HasField('speed') and vehicle.position.speed > 1 else avg_speed
                        if dist_to_stop is not None and v_speed > 0:
                            eta_min, pred_src = max(0, round((dist_to_stop / v_speed) / 60)), "hybrid"
            if pred_src is None and trip_info.get('service_id') in active_services:
                sched_time_str = trip_schedule.get(rel_stop_id, "")
                sched_dt = parse_gtfs_time(sched_time_str, now_dt, sofia_tz)
                if sched_dt and now_dt < sched_dt < now_dt + timedelta(hours=2):
                    eta_min, pred_src, is_live = max(0, round((sched_dt - now_dt).total_seconds() / 60)), "schedule", False
            if pred_src is not None:
                r_name, r_type = route_info.get('route_short_name', 'Н/А'), route_info.get('route_type')
                all_arrivals.append({"trip_id": t_id, "route_name": r_name, "route_type": r_type, "destination": trip_info.get('trip_headsign', 'Н/И'), "eta_minutes": eta_min, "prediction_source": pred_src, "is_live": is_live, "alerts": processed_alerts.get(f"{r_name}-{r_type}")})
        all_arrivals.sort(key=lambda x: (not x['is_live'], x['eta_minutes']))
        return jsonify(all_arrivals)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_vehicles_for_stop: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/bulk_detailed_arrivals', methods=['POST'])
def get_bulk_detailed_arrivals():
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts, stop_codes = get_processed_alerts(), set(request.json.get('stop_codes', []))
        if not stop_codes: return jsonify({})
        now_dt, now_ts = datetime.now(sofia_tz), int(time.time())
        arrival_predictions = {e.trip_update.trip.trip_id: {stu.stop_id: stu.arrival.time for stu in e.trip_update.stop_time_update if stu.HasField('arrival') and stu.arrival.time > 0} for e in trip_updates_feed_cache.entity if e.HasField('trip_update')} if trip_updates_feed_cache else {}
        vehicle_positions = {e.vehicle.trip.trip_id: e.vehicle for e in vehicle_positions_feed_cache.entity if e.HasField('vehicle')} if vehicle_positions_feed_cache else {}
        final_results = {code: [] for code in stop_codes}; rel_stop_ids, stop_id_to_code = set(), {}
        for s_id, s_data in stops_data.items():
            code = s_data.get('stop_code')
            if code in stop_codes: rel_stop_ids.add(s_id); stop_id_to_code[s_id] = code
        trips_to_check = {tid for s_id in rel_stop_ids for tid in stop_to_trips_map.get(s_id, [])}
        for t_id in trips_to_check:
            trip_info = trips_data.get(t_id)
            if not trip_info: continue
            route_info = routes_data.get(trip_info['route_id'])
            if not route_info: continue
            for s_id, sched_time in schedule_by_trip.get(t_id, {}).items():
                if s_id in rel_stop_ids:
                    eta_min, pred_src, is_live = -1, None, False
                    pred_ts = arrival_predictions.get(t_id, {}).get(s_id)
                    if pred_ts and pred_ts > now_ts - 60:
                        eta_min, pred_src, is_live = max(0, round((pred_ts - now_ts) / 60)), "official", True
                    elif t_id in vehicle_positions: pred_src, is_live = "hybrid", True
                    if not is_live and trip_info.get('service_id') in active_services:
                        sched_dt = parse_gtfs_time(sched_time, now_dt, sofia_tz)
                        if sched_dt and now_dt < sched_dt < now_dt + timedelta(hours=2): eta_min, pred_src = max(0, round((sched_dt - now_dt).total_seconds() / 60)), "schedule"
                    elif is_live and eta_min == -1:
                        sched_dt = parse_gtfs_time(sched_time, now_dt, sofia_tz)
                        if sched_dt and now_dt < sched_dt < now_dt + timedelta(hours=2): eta_min = max(0, round((sched_dt - now_dt).total_seconds() / 60))
                    if pred_src and eta_min != -1:
                        r_name, r_type = route_info.get('route_short_name', 'Н/А'), route_info.get('route_type')
                        final_results[stop_id_to_code[s_id]].append({"trip_id": t_id, "route_name": r_name, "route_type": r_type, "destination": trip_info.get('trip_headsign', 'Н/И'), "eta_minutes": eta_min, "prediction_source": pred_src, "is_live": is_live, "alerts": processed_alerts.get(f"{r_name}-{r_type}")})
        for code in final_results: final_results[code].sort(key=lambda x: (not x['is_live'], x['eta_minutes']))
        return jsonify(final_results)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_detailed_arrivals: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/bulk_arrivals_for_stops', methods=['POST'])
def get_bulk_arrivals_for_stops():
    try:
        refresh_realtime_cache_if_needed()
        stop_codes = set(request.json.get('stop_codes', []))
        if not stop_codes: return jsonify({})
        now_dt, now_ts = datetime.now(sofia_tz), int(time.time())
        arrival_predictions = {e.trip_update.trip.trip_id: {stu.stop_id: stu.arrival.time for stu in e.trip_update.stop_time_update if stu.HasField('arrival') and stu.arrival.time > 0} for e in trip_updates_feed_cache.entity if e.HasField('trip_update')} if trip_updates_feed_cache else {}
        stop_code_to_ids = {code:[] for code in stop_codes}
        for s_id, s_data in stops_data.items():
            code = s_data.get('stop_code')
            if code in stop_codes: stop_code_to_ids[code].append(s_id)
        bulk_results = {}
        trips_to_check = {tid for code in stop_codes for s_id in stop_code_to_ids.get(code,[]) for tid in stop_to_trips_map.get(s_id,[])}
        for t_id in trips_to_check:
            trip_info = trips_data.get(t_id)
            if not trip_info or trip_info.get('service_id') not in active_services: continue
            for s_id, sched_time in schedule_by_trip.get(t_id, {}).items():
                s_details = stops_data.get(s_id)
                if not s_details: continue
                s_code = s_details.get('stop_code')
                if s_code in stop_codes:
                    is_upcoming = False
                    if arrival_predictions.get(t_id, {}).get(s_id) and arrival_predictions[t_id][s_id] > now_ts - 60: is_upcoming = True
                    else:
                        sched_dt = parse_gtfs_time(sched_time, now_dt, sofia_tz)
                        if sched_dt and now_dt < sched_dt < now_dt + timedelta(hours=2): is_upcoming = True
                    if is_upcoming:
                        if s_code not in bulk_results: bulk_results[s_code] = {'arrivals': set()}
                        r_info = routes_data.get(trip_info['route_id'])
                        r_type, r_name = r_info.get('route_type', ''), r_info.get('route_short_name', '')
                        transport_type = 'BUS'
                        if r_name.startswith('N'): transport_type = 'NIGHT'
                        elif r_type == '0': transport_type = 'TRAM'
                        elif r_type == '11': transport_type = 'TROLLEY'
                        bulk_results[s_code]['arrivals'].add(transport_type)
        return jsonify({c: {'arrivals': list(d['arrivals'])} for c, d in bulk_results.items()})
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_bulk_arrivals_for_stops: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/schedule_for_stop/<stop_code>')
def get_schedule_for_stop(stop_code):
    try:
        relevant_stop_ids = {s_id for s_id, s_data in stops_data.items() if s_data.get('stop_code') == stop_code}
        if not relevant_stop_ids: return jsonify({"error": "Stop not found"}), 404
        schedule = {"weekday": {}, "holiday": {}}
        trip_ids_for_stop = {tid for s_id in relevant_stop_ids for tid in stop_to_trips_map.get(s_id, [])}
        for t_id in trip_ids_for_stop:
            trip_info = trips_data.get(t_id)
            if not trip_info: continue
            service_id = str(trip_info['service_id'])
            def add_to_schedule(target):
                route_info = routes_data.get(trip_info['route_id'])
                if not route_info: return
                r_name, dest, r_type = route_info.get('route_short_name', 'Н/А'), trip_info.get('trip_headsign', 'Н/И'), route_info.get('route_type', '3')
                arr_time = next((schedule_by_trip.get(t_id, {}).get(s_id) for s_id in relevant_stop_ids if s_id in schedule_by_trip.get(t_id, {})), None)
                if not arr_time: return
                if r_name not in target: target[r_name] = {}
                if dest not in target[r_name]: target[r_name][dest] = {"times": [], "route_type": r_type}
                if arr_time not in target[r_name][dest]["times"]: target[r_name][dest]["times"].append(arr_time)
            if service_id in holiday_schedule_ids: add_to_schedule(schedule["holiday"])
            elif service_id in weekday_schedule_ids: add_to_schedule(schedule["weekday"])
        for sched_type in schedule.values():
            for route in sched_type.values():
                for dest_data in route.values():
                    dest_data["times"].sort(key=lambda t: tuple(map(int, t.split(':'))))
        return jsonify(schedule)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_schedule_for_stop: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/vehicles_for_routes/<route_names_str>')
def get_vehicles_for_routes(route_names_str):
    try:
        refresh_realtime_cache_if_needed()
        req_routes = set(route_names_str.split(','))
        if not vehicle_positions_feed_cache: return jsonify([])
        vehicles = []
        for entity in vehicle_positions_feed_cache.entity:
            if entity.HasField('vehicle'):
                v = entity.vehicle; t_id = v.trip.trip_id
                trip_info = trips_data.get(t_id)
                if not trip_info: continue
                route_info = routes_data.get(trip_info['route_id'])
                if route_info and route_info.get('route_short_name') in req_routes:
                    vehicles.append({"latitude": v.position.latitude if v.HasField('position') else None, "longitude": v.position.longitude if v.HasField('position') else None, "trip_id": t_id, "route_name": route_info.get('route_short_name'), "route_type": route_info.get('route_type', ''), "destination": trip_info.get('trip_headsign', 'Н/И'), "next_stop_id": v.stop_id if v.HasField('stop_id') else None, "stop_sequence": v.current_stop_sequence if v.HasField('current_stop_sequence') else None})
        return jsonify(vehicles)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_vehicles_for_routes: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/shape/<trip_id>')
def get_shape_for_trip(trip_id):
    trip_info = trips_data.get(trip_id)
    if not trip_info: return jsonify({"error": "Trip not found"}), 404
    shape_id = trip_info.get('shape_id')
    shape_points = get_shape_by_id(shape_id)
    return jsonify(shape_points)

@app.route('/api/stops_for_trip/<trip_id>')
def get_stops_for_trip(trip_id):
    ensure_route_details_cache()
    if trip_id not in trip_stops_sequence: return jsonify({"error": "Trip not found"}), 404
    stops_list = [dict(stops_data.get(s['stop_id']), **{'stop_sequence': s['stop_sequence']}) for s in trip_stops_sequence.get(trip_id, []) if stops_data.get(s['stop_id'])]
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
    ensure_routes_by_line_cache()
    try:
        final_list = []
        for line_num, types in routes_by_line_cache.items():
            for r_type, variations in types.items():
                transport_type = 'BUS'
                if line_num.startswith('N'): transport_type = 'NIGHT'
                elif r_type == '0': transport_type = 'TRAM'
                elif r_type == '11': transport_type = 'TROLLEY'
                elif r_type in ['1','2']: transport_type = 'METRO'
                directions = [{"headsign": v['direction'], "example_trip_id": v['trip_id_sample']} for v in variations]
                final_list.append({"line_name": line_num, "transport_type": transport_type, "directions": directions})
        return jsonify(final_list)
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_all_lines_structured: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/line_details/<line_number>/<route_type_code>')
def get_line_details(line_number, route_type_code):
    ensure_routes_by_line_cache()
    line_data = routes_by_line_cache.get(line_number, {}).get(route_type_code, [])
    if not line_data:
        return jsonify({"error": f"Няма данни за линия {line_number}."}), 404
    return jsonify(line_data)

@app.route('/api/full_route_view/<trip_id>')
def get_full_route_view(trip_id):
    ensure_route_details_cache()
    try:
        cached_data = precomputed_route_details_cache.get(trip_id)
        if not cached_data: return jsonify({"error": f"Static route details not found for trip {trip_id}."}), 404
        trip_info = trips_data.get(trip_id)
        if not trip_info: return jsonify({"error": f"Trip info for {trip_id} not found."}), 404
        route_info = routes_data.get(trip_info.get('route_id'))
        if not route_info: return jsonify({"error": f"Route info for trip {trip_id} not found."}), 404
        r_name_fetch = route_info.get('route_short_name')
        refresh_realtime_cache_if_needed()
        live_vehicles = []
        if vehicle_positions_feed_cache:
            for entity in vehicle_positions_feed_cache.entity:
                if entity.HasField('vehicle'):
                    v = entity.vehicle; t_id = v.trip.trip_id
                    v_trip_info = trips_data.get(t_id)
                    if not v_trip_info: continue
                    v_route_info = routes_data.get(v_trip_info.get('route_id'))
                    if v_route_info and v_route_info.get('route_short_name') == r_name_fetch:
                        live_vehicles.append({"latitude": v.position.latitude if v.HasField('position') else 0, "longitude": v.position.longitude if v.HasField('position') else 0, "trip_id": t_id, "route_name": v_route_info.get('route_short_name'), "route_type": v_route_info.get('route_type', ''), "destination": v_trip_info.get('trip_headsign', 'Н/И')})
        return jsonify({"shape": cached_data["shape"], "stops": cached_data["stops"], "vehicles": live_vehicles})
    except Exception as e:
        print(f"КРИТИЧНА ГРЕШКА в get_full_route_view: {e}", file=sys.stderr)
        return jsonify({"error": "An internal server error occurred."}), 500

@app.route('/api/static_route_view/<trip_id>')
def get_static_route_view(trip_id):
    ensure_route_details_cache()
    cached_data = precomputed_route_details_cache.get(trip_id)
    if not cached_data: return jsonify({"error": f"Static route details not found for trip {trip_id}."}), 404
    return jsonify({"shape": cached_data.get("shape", []), "stops": cached_data.get("stops", [])})

@app.route('/api/debug/alerts_raw')
def debug_alerts_raw():
    try:
        refresh_realtime_cache_if_needed()
        if not alerts_feed_cache: return jsonify({"error": "Alerts фийдът не е зареден."}), 500
        return Response(str(alerts_feed_cache), mimetype='text/plain; charset=utf-8')
    except Exception as e:
        return jsonify({"error": f"Възникна грешка: {e}"}), 500

@app.route('/api/debug/alerts')
def debug_alerts():
    try:
        refresh_realtime_cache_if_needed()
        processed_alerts = get_processed_alerts()
        if not processed_alerts: return jsonify({"status": "OK","message": "Няма активни предупреждения.","data": {}})
        return jsonify({"status": "OK","message": f"Намерени са {len(processed_alerts)} активни предупреждения.","data": processed_alerts})
    except Exception as e:
        return jsonify({"error": f"Възникна грешка: {e}"}), 500
