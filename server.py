# ===================================================================
# ==            ФИНАЛЕН ДЕБЪГ КОД ЗА СЪРВЪРА В RENDER.COM        ==
# ==         (Записва получения HTML в лога за анализ)          ==
# ===================================================================
from flask import Flask, jsonify
from flask_cors import CORS
import requests

app = Flask(__name__)
CORS(app)

@app.route('/')
def health_check():
    return jsonify({"status": "ok", "message": "Скрейпър сървърът в Render работи!"})

@app.route('/api/arrivals/<stop_code>')
def get_live_arrivals(stop_code):
    print(f"--- ЗАПОЧВА НОВА ЗАЯВКА ЗА СПИРКА {stop_code} ---")
    if not stop_code:
        return jsonify({"arrivals":[]})
        
    url = "https://www.sofiatraffic.bg/bg/schedules/stops-info"
    payload = {'stop_code_q': stop_code}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    try:
        response = requests.post(url, data=payload, headers=headers, timeout=15)
        print(f"Статус код от sofiatraffic.bg: {response.status_code}")
        
        # --- ТОВА Е КЛЮЧОВИЯТ РЕД ---
        # Записваме целия получен HTML в лога
        print("--- НАЧАЛО НА ПОЛУЧЕНИЯ HTML ---")
        print(response.text)
        print("--- КРАЙ НА ПОЛУЧЕНИЯ HTML ---")
        # -----------------------------

        # Засега просто връщаме празен резултат, целта е само да видим лога
        return jsonify({"arrivals":[]})

    except Exception as e:
        print(f"ГРЕШКА ПРИ ЗАЯВКАТА: {e}")
        return jsonify({"error": str(e)}), 500
