from flask import Flask, jsonify, render_template_string, request, redirect, session, make_response
import requests
import json
import os
from zeep import Client
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from flask_cors import CORS
from flask_session import Session
import time
import csv
from datetime import datetime
from flask import send_file

CSV_DIR = "./csv_reports"  # Папка для хранения CSV-файлов
os.makedirs(CSV_DIR, exist_ok=True)  # Создаём папку, если её нет

# 🔹 PowerBody API (SOAP)


USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
WSDL_URL = os.getenv('URL')

app = Flask(__name__)
CORS(app)

# 🔹 Shopify API настройки
SHOPIFY_CLIENT_ID = os.getenv('CLIENT_ID')
SHOPIFY_API_SECRET = os.getenv('API_SECRET')
SHOPIFY_SCOPES = "read_products,write_products,write_inventory"
APP_URL = os.getenv('APP_URL')  # ⚠️ Указать свой URL от ngrok
REDIRECT_URI = f"{APP_URL}/auth/callback"

# 🔹 Flask настройки
app = Flask(__name__)
CORS(app)
app.secret_key = os.urandom(24)
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_FILE_DIR"] = "./flask_sessions"
Session(app)

TOKENS_FILE = "tokens.json"
SETTINGS_FILE = "settings.json"

# 🔹 Планировщик задач
executors = {'default': ThreadPoolExecutor(max_workers=10)}
scheduler = BackgroundScheduler(executors=executors)
scheduler.start()


# 🔹 Функции для работы с токенами
def save_token(shop, access_token):
    tokens = {}
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            tokens = json.load(f)
    tokens[shop] = access_token
    with open(TOKENS_FILE, "w") as f:
        json.dump(tokens, f, indent=4)


def get_token(shop):
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            tokens = json.load(f)
        return tokens.get(shop)
    return None


@app.route("/")
def home():
    shop = request.args.get("shop") or request.cookies.get("shop")

    if not shop:
        print("❌ Ошибка: отсутствует параметр 'shop'. Запрос:", request.args, request.cookies)
        return "❌ Ошибка: отсутствует параметр 'shop'.", 400

    access_token = get_token(shop)
    if not access_token:
        return redirect(f"/install?shop={shop}")

    return redirect(f"/admin?shop={shop}")


@app.route("/install")
def install_app():
    shop = request.args.get("shop")
    if not shop:
        return "❌ Ошибка: укажите магазин Shopify", 400

    session["shop"] = shop
    authorization_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={SHOPIFY_CLIENT_ID}"
        f"&scope={SHOPIFY_SCOPES}"
        f"&redirect_uri={REDIRECT_URI}"
    )
    return redirect(authorization_url)


@app.route("/auth/callback")
def auth_callback():
    shop = request.args.get("shop")
    code = request.args.get("code")
    if not code or not shop:
        return "❌ Ошибка авторизации", 400

    token_url = f"https://{shop}/admin/oauth/access_token"
    data = {"client_id": SHOPIFY_CLIENT_ID, "client_secret": SHOPIFY_API_SECRET, "code": code}
    response = requests.post(token_url, json=data)

    if response.status_code == 200:
        access_token = response.json().get("access_token")
        save_token(shop, access_token)

        response = make_response(redirect(f"/admin?shop={shop}"))
        response.set_cookie("shop", shop, httponly=True, samesite="None", secure=True)

        start_sync_for_shop(shop, access_token)

        return response
    return f"❌ Ошибка авторизации: {response.text}", 400


@app.route("/admin")
def admin():
    """Встраиваемое приложение в Shopify Admin"""
    shop = request.args.get("shop") or request.cookies.get("shop")
    access_token = get_token(shop)

    if not shop or not access_token:
        return redirect(f"/install?shop={shop}")

    settings = load_settings()

    return render_template_string(f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Shopify Embedded App</title>
        <script src="https://unpkg.com/@shopify/app-bridge"></script>
        <script src="https://unpkg.com/@shopify/app-bridge-utils"></script>
        <script>
            var AppBridge = window["app-bridge"];
            var createApp = AppBridge.createApp;
            var actions = AppBridge.actions;
            var Redirect = actions.Redirect;

            var app = createApp({{
                apiKey: "{SHOPIFY_CLIENT_ID}",
                shopOrigin: "{shop}",
                forceRedirect: true
            }});
        </script>
          <style>
            body {{
                display: flex;
                justify-content: center;
                align-items: center;
                height: 100vh;
                background-color: #f4f4f4;
                margin: 0;
            }}

            .container {{
                width: 40%;
                background: white;
                padding-left: 20px;
                padding-right: 35px;
                padding-bottom: 10px;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
                text-align: center;
            }}

            .form-group {{
                margin-bottom: 15px;
            }}

            label {{
                font-weight: bold;
                display: block;
                margin-bottom: 5px;
            }}

            input {{
                width: 100%;
                padding: 8px;
                border: 1px solid #ccc;
                border-radius: 5px;
                font-size: 16px;
            }}

            button {{
                background-color: #007bff;
                color: white;
                padding: 10px 15px;
                border: none;
                border-radius: 5px;
                font-size: 16px;
                cursor: pointer;
                margin-top: 10px;
            }}

            button:hover {{
                background-color: #0056b3;
            }}

            #message {{
                margin-top: 15px;
                padding: 10px;
                border-radius: 5px;
                display: none;
                font-weight: bold;
                font-size: 16px;
            }}

            .success {{
                background-color: #d4edda;
                color: #155724;
                border: 1px solid #c3e6cb;
            }}

            .error {{
                background-color: #f8d7da;
                color: #721c24;
                border: 1px solid #f5c6cb;
            }}
        </style>
    </head>

    <body>
        <div class="container">
                <h2>Pricing settings</h2>
                <form id="settingsForm">
                    <div class="form-group">
                        <label>VAT (%):</label>
                        <input type="number" name="vat" value="{settings["vat"]}" step="0.01">
                    </div>
                    <div class="form-group">
                        <label>PayPal Fees (%):</label>
                        <input type="number" name="paypal_fees" value="{settings["paypal_fees"]}" step="0.01">
                    </div>
                    <div class="form-group">
                        <label>Second PayPal Fees (£):</label>
                        <input type="number" name="second_paypal_fees" value="{settings["second_paypal_fees"]}" step="0.01">
                    </div>
                    <div class="form-group">
                        <label>Profit Margin (%):</label>
                        <input type="number" name="profit" value="{settings["profit"]}" step="0.01">
                    </div>
                    <button type="submit">Update</button>
                    <button id="downloadCSV">Download CSV</button>
                </form>
                <p id="message"></p>
            </div>
            

        <script>
                document.addEventListener("DOMContentLoaded", function() {{
                    // Обработчик формы
                    var settingsForm = document.getElementById('settingsForm');
                    if (settingsForm) {{
                        settingsForm.addEventListener('submit', function(event) {{
                            event.preventDefault();
                            var formData = new FormData(this);
        
                            fetch('/update_settings', {{
                                method: 'POST',
                                body: formData
                            }})
                            .then(response => response.json())
                            .then(data => {{
                                let messageElement = document.getElementById('message');
                                if (messageElement) {{
                                    messageElement.innerText = data.message;
                                    messageElement.style.display = 'block';
                                    setTimeout(() => {{
                                        messageElement.style.display = 'none';
                                    }}, 3000);
                                }}
                            }})
                            .catch(error => console.error('Ошибка:', error));
                        }});
                    }} else {{
                        console.error("❌ Форма 'settingsForm' не найдена!");
                    }}
        
                    // Обработчик кнопки скачивания CSV
                    var downloadBtn = document.getElementById('downloadCSV');
                    if (downloadBtn) {{
                        downloadBtn.addEventListener('click', function() {{
                            window.location.href = '/download_csv';
                        }});
                    }} else {{
                        console.error("❌ Кнопка 'Download CSV' не найдена!");
                    }}
                }});
        </script>

        
        
    </body>
    </html>
    """)


# 🔄 Получение товаров из PowerBody API
def fetch_powerbody_products():
    print("🔄 Запрос товаров из PowerBody API...")
    try:
        client = Client(WSDL_URL)
        session = client.service.login(USERNAME, PASSWORD)
        response = client.service.call(session, "dropshipping.getProductList", [])

        if isinstance(response, str):
            try:
                response = json.loads(response)
            except json.JSONDecodeError:
                print("❌ Ошибка декодирования JSON")
                return []

        if not isinstance(response, list):
            return []

        print(f"✅ Загружено товаров: {len(response)}")
        client.service.endSession(session)
        return response

    except Exception as e:
        print(f"❌ Ошибка получения товаров: {e}")
        return []


# 🔄 Получение товаров из Shopify API
def fetch_all_shopify_products(shop, access_token):
    print("🔄 Запрос товаров из Shopify API...")
    shopify_url = f"https://{shop}/admin/api/2024-01/products.json"
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}
    params = {"fields": "id,variants", "limit": 250}
    all_products = []

    while True:
        time.sleep(0.6)
        response = requests.get(shopify_url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"❌ Ошибка Shopify API: {response.status_code} | {response.text}")
            break

        products = response.json().get("products", [])
        all_products.extend(products)
        print(f"📦 Получено товаров: {len(products)}, всего: {len(all_products)}")

        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            try:
                next_page_info = [l.split(";")[0].strip("<>") for l in link_header.split(",") if 'rel="next"' in l][0]
                params["page_info"] = next_page_info.split("page_info=")[1]
            except Exception as e:
                print(f"❌ Ошибка парсинга page_info: {e}")
                break
        else:
            break

    print(f"✅ Всего товаров в Shopify: {len(all_products)}")
    return all_products


def calculate_final_price(base_price, vat, paypal_fees, second_paypal_fees, profit):
    """Рассчитывает финальную цену по введенным данным"""
    if base_price is None or base_price == 0:
        return None

    vat_amount = base_price * (vat / 100)
    paypal_fees_amount = base_price * (paypal_fees / 100)
    profit_amount = base_price * (profit / 100)

    final_price = base_price + vat_amount + paypal_fees_amount + second_paypal_fees + profit_amount
    return round(final_price, 2)


def make_request_with_retries(url, headers, data, method="PUT", max_retries=5):
    """Функция для повторных попыток запроса в случае ошибки 429"""
    delay = 2
    for attempt in range(max_retries):
        response = requests.put(url, headers=headers, json=data) if method == "PUT" else requests.post(url,
                                                                                                       headers=headers,
                                                                                                       json=data)

        if response.status_code == 200:
            return response
        elif response.status_code == 429:
            print(f"⚠️ Error 429 (Too Many Requests). Send request again {delay} sec...")
            time.sleep(delay)
            delay *= 2
        else:
            print(f"❌ Ошибка {response.status_code} | {response.text}")
            return response
    print("🚨 Превышено количество повторных попыток запроса.")
    return response


def update_shopify_variant(shop, access_token, variant_id, inventory_item_id, new_price, new_quantity, sku):
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}

    print(f"🔄 Обновляем variant {variant_id} (SKU: {sku}): Цена {new_price}, Количество {new_quantity}")

    update_variant_url = f"https://{shop}/admin/api/2024-01/variants/{variant_id}.json"
    variant_data = {"variant": {"id": variant_id, "price": f"{new_price:.2f}"}}
    response = requests.put(update_variant_url, headers=headers, json=variant_data)

    if response.status_code == 200:
        print(f"✅ Успешно обновлена цена для variant {variant_id} (SKU: {sku}): {new_price}")
    else:
        print(f"❌ Ошибка обновления цены для variant {variant_id} (SKU: {sku}): {response.status_code} - {response.text}")

    update_inventory_url = f"https://{shop}/admin/api/2024-01/inventory_levels/set.json"
    inventory_data = {"location_id": 85726363936, "inventory_item_id": inventory_item_id, "available": new_quantity}
    response = requests.post(update_inventory_url, headers=headers, json=inventory_data)

    if response.status_code == 200:
        print(f"✅ Количество обновлено для variant {variant_id} (SKU: {sku}): {new_quantity}")
    else:
        print(f"❌ Ошибка обновления количества для variant {variant_id} (SKU: {sku}): {response.status_code} - {response.text}")


def sync_products(shop):
    """Полная синхронизация товаров с сохранением CSV-отчёта."""
    access_token = get_token(shop)
    if not access_token:
        print(f"❌ Ошибка: Токен для {shop} не найден. Пропускаем синхронизацию.")
        return

    print(f"🔄 Начинаем синхронизацию для {shop}...")

    # Загружаем настройки
    settings = load_settings()
    vat = settings["vat"]
    paypal_fees = settings["paypal_fees"]
    second_paypal_fees = settings["second_paypal_fees"]
    profit = settings["profit"]

    powerbody_products = fetch_powerbody_products()
    shopify_products = fetch_all_shopify_products(shop, access_token)

    # Создаём карту SKU → (variant_id, inventory_item_id, old_price, old_quantity)
    shopify_sku_map = {
        v.get("sku"): (v["id"], v.get("inventory_item_id"), v.get("price"), v.get("inventory_quantity"))
        for p in shopify_products for v in p["variants"] if v.get("sku")
    }

    synced_count = 0
    csv_data = []

    for pb_product in powerbody_products:
        if not isinstance(pb_product, dict):
            continue

        sku = pb_product.get("sku")
        base_price = pb_product.get("retail_price", pb_product.get("price", "0.00"))
        new_quantity = pb_product.get("qty")

        if not sku or sku not in shopify_sku_map:
            continue

        try:
            base_price = float(base_price)
        except ValueError:
            continue

        variant_id, inventory_item_id, old_price, old_quantity = shopify_sku_map[sku]

        # Рассчитываем финальную цену
        final_price = calculate_final_price(base_price, vat, paypal_fees, second_paypal_fees, profit)

        # Добавляем данные в CSV
        csv_data.append([sku, base_price, old_price, old_quantity])

        # Проверяем, нужно ли обновлять товар
        if old_price != final_price or old_quantity != new_quantity:
            print(f"🔄 Обновляем SKU {sku}: Цена API {base_price} → Shopify {final_price}, Количество: {old_quantity} → {new_quantity}")
            update_shopify_variant(shop, access_token, variant_id, inventory_item_id, final_price, new_quantity, sku)
            synced_count += 1

            # Shopify API лимит - не более 2 запросов в секунду, ставим задержку
            time.sleep(0.6)

    # Сохранение CSV-отчёта после завершения синхронизации
    csv_filename = save_to_csv(csv_data)
    print(f"✅ Синхронизация завершена! Обновлено товаров: {synced_count}")
    return csv_filename  # Возвращаем путь к CSV


@app.route('/update_settings', methods=['POST'])
def update_settings():
    """Обновление настроек"""
    settings = {
        "vat": float(request.form.get("vat", 20)),
        "paypal_fees": float(request.form.get("paypal_fees", 3)),
        "second_paypal_fees": float(request.form.get("second_paypal_fees", 0.20)),
        "profit": float(request.form.get("profit", 30))
    }
    save_settings(settings)
    return jsonify({"status": "success", "message": "✅ Settings saved!"})


def load_settings():
    if os.path.exists(SETTINGS_FILE):
        with open(SETTINGS_FILE, "r") as file:
            return json.load(file)
    else:
        default_settings = {"vat": 20.0, "paypal_fees": 3.0, "second_paypal_fees": 0.20, "profit": 30.0}
        save_settings(default_settings)
        return default_settings


def save_settings(settings):
    with open(SETTINGS_FILE, "w") as file:
        json.dump(settings, file, indent=4)

def save_to_csv(data):
    """Сохраняет данные в CSV с текущей датой и временем."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(CSV_DIR, f"sync_report_{timestamp}.csv")

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["SKU", "Price API", "Price Shopify", "Quantity"])  # Заголовки
        writer.writerows(data)  # Записываем строки

    print(f"✅ CSV файл сохранён: {filename}")
    return filename  # Возвращаем путь к файлу

def get_latest_csv():
    """Находит последний созданный CSV-файл."""
    files = sorted(os.listdir(CSV_DIR), reverse=True)
    if files:
        return os.path.join(CSV_DIR, files[0])
    return None

@app.route("/download_csv")
def download_csv():
    """Отправляет последний созданный CSV-файл для скачивания."""
    latest_file = get_latest_csv()
    if not latest_file:
        return "❌ Нет доступных CSV-файлов.", 404
    return send_file(latest_file, as_attachment=True)


# 🔄 Запуск фоновой синхронизации
def start_sync_for_shop(shop, access_token):
    job_id = f"sync_{shop}"
    existing_job = scheduler.get_job(job_id)

    if not existing_job:
        print(f"🕒 Запуск фоновой синхронизации для {shop} каждые 5 минут.")
        scheduler.add_job(sync_products, 'interval', minutes=60, args=[shop], id=job_id, replace_existing=True)


# 🔄 Запуск фоновой синхронизации при старте сервера
def schedule_sync():
    if os.path.exists(TOKENS_FILE):
        with open(TOKENS_FILE, "r") as f:
            tokens = json.load(f)
        for shop, access_token in tokens.items():
            start_sync_for_shop(shop, access_token)


schedule_sync()

if __name__ == "__main__":
    os.makedirs("./flask_sessions", exist_ok=True)
    app.run(host='0.0.0.0', port=80, debug=False)