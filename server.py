# ===================================================================
# ==                КОД ЗА СЪРВЪРА В RENDER.COM                  ==
# ==    (Лек скрейпър за данни в реално време, без файлове)       ==
# ===================================================================

from flask import Flask, jsonify
from flask_cors import CORS
import requests
from bs4 import BeautifulSoup

app = Flask(__name__)
CORS(app) # Позволява достъп отвсякъде

def fetch_from_sofia_traffic(stop_code):
    """Извлича данни за пристигания от sofiatraffic.bg."""
    if not stop_code:
        return []
        
    url = "https://www.sofiatraffic.bg/bg/schedules/stops-info"
    payload = {'stop_code_q': stop_code}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        response = requests.post(url, data=payload, headers=headers, timeout=15)
        if response.status_code != 200:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        arrivals = []
        arrival_rows = soup.find_all('div', class_='arrival-row')
        
        for row in arrival_rows:
            line_element = row.find('span', class_='line_number')
            time_element = row.find('div', class_='time').find('span')
            if line_element and time_element:
                arrivals.append({
                    "line": line_element.text.strip(),
                    "time": time_element.text.strip()
                })
        return arrivals
    except:
        return []

@app.route('/api/arrivals/<stop_code>')
def get_live_arrivals(stop_code):
    """Единственият endpoint: взима код на спирка и връща пристигания."""
    live_arrivals = fetch_from_sofia_traffic(stop_code)
    return jsonify({"arrivals": live_arrivals})
