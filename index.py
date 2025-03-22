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
import redis
import re

CSV_DIR = "./csv_reports"  # Папка для хранения CSV-файлов
os.makedirs(CSV_DIR, exist_ok=True)  # Создаём папку, если её нет

# 🔹 PowerBody API (SOAP)


USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')
WSDL_URL = os.getenv('URL')

app = Flask(__name__)
CORS(app)
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# Подключение к Redis Cloud
redis_client = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    decode_responses=True
)

# 🔹 Shopify API настройки
SHOPIFY_CLIENT_ID = os.getenv('CLIENT_ID')
SHOPIFY_API_SECRET = os.getenv('API_SECRET')
SHOPIFY_SCOPES = "read_products,write_products,write_inventory"
APP_URL = os.getenv('APP_URL')  # ⚠️ Указать свой URL от ngrok
REDIRECT_URI = f"{APP_URL}/auth/callback"

app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", os.urandom(24).hex())  # Используем .env или генерируем новый
app.config["SESSION_TYPE"] = "redis"
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_USE_SIGNER"] = True
app.config["SESSION_KEY_PREFIX"] = "session:"
app.config["SESSION_REDIS"] = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    decode_responses=True
)

# Настраиваем сессии
Session(app)

SETTINGS_FILE = "settings.json"

# 🔹 Планировщик задач
executors = {'default': ThreadPoolExecutor(max_workers=10)}
scheduler = BackgroundScheduler(executors=executors)
scheduler.start()


@app.before_request
def log_request():
    print(f"📥 Входящий запрос: {request.method} {request.url} | IP: {request.remote_addr}")


def save_token(shop, access_token):
    """Сохраняет токен магазина в Redis Cloud с TTL"""
    token_key = f"shopify_token:{shop}"
    redis_client.set(token_key, access_token, ex=2592000)  # 30 дней TTL

    stored_token = redis_client.get(token_key)
    ttl = redis_client.ttl(token_key)

    if stored_token:
        print(f"✅ Токен сохранён в Redis: {shop} → {stored_token[:8]}*** (TTL: {ttl} сек)")
    else:
        print(f"❌ Ошибка: токен НЕ сохранён в Redis!")


@app.route("/test_redis")
def test_redis():
    redis_client.set("foo", "bar")
    value = redis_client.get("foo")
    return f"Redis Cloud работает! foo = {value}"


def get_token(shop):
    """Получает токен магазина из Redis"""
    token_key = f"shopify_token:{shop}"
    token = redis_client.get(token_key)
    ttl = redis_client.ttl(token_key)  # Проверяем TTL

    if token:
        if ttl == -1:  # Если у токена нет TTL, устанавливаем его
            redis_client.expire(token_key, 2592000)  # 30 дней
            print(f"🔄 Обновлён TTL токена для {shop} (30 дней)")

        print(f"📥 Токен из Redis для {shop}: {token[:8]}*** (TTL: {ttl} сек)")
        return token
    else:
        print(f"❌ Токен не найден в Redis для {shop} (TTL: {ttl} сек)")
        return None


@app.route("/")
def home():
    shop = request.args.get("shop") or request.cookies.get("shop")
    print(f"🛒 Получен запрос на / с параметром shop: {shop}")  # Логируем запрос

    if not shop:
        print("❌ Ошибка: отсутствует параметр 'shop'. Запрос:", request.args, request.cookies)
        return "❌ Ошибка: отсутствует параметр 'shop'.", 400

    access_token = get_token(shop)
    print(f"🔑 Токен для {shop}: {access_token}")

    if not access_token:
        print(f"🔄 Перенаправление на /install?shop={shop}")
        return redirect(f"/install?shop={shop}")

    print(f"✅ Токен найден, перенаправление на /admin?shop={shop}")
    return redirect(f"/admin?shop={shop}")


@app.route("/install")
def install_app():
    shop = request.args.get("shop")
    print(f"📦 Установка приложения для: {shop}")

    if not shop:
        print("❌ Ошибка: параметр 'shop' отсутствует")
        return "❌ Ошибка: укажите магазин Shopify", 400

    if redis_client.ping():
        session["shop"] = shop
    else:
        print("⚠️ Redis не подключен. Пропускаем установку сессии.")
    authorization_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={SHOPIFY_CLIENT_ID}"
        f"&scope={SHOPIFY_SCOPES}"
        f"&redirect_uri={REDIRECT_URI}"
    )

    print(f"🔗 Перенаправление на Shopify OAuth: {authorization_url}")
    return redirect(authorization_url)


@app.route("/auth/callback")
def auth_callback():
    shop = request.args.get("shop")
    code = request.args.get("code")

    print(f"📞 Вызван `auth_callback`")
    print(f"🔍 Получен shop: {shop}")
    print(f"🔍 Получен code: {code}")

    if not code or not shop:
        print("❌ Ошибка: отсутствует `code` или `shop` в `auth_callback`.")
        return "❌ Ошибка авторизации: отсутствует `code` или `shop`", 400

    token_url = f"https://{shop}/admin/oauth/access_token"
    data = {
        "client_id": SHOPIFY_CLIENT_ID,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code
    }

    print(f"🔗 Отправляем запрос на {token_url} с данными: {data}")

    response = requests.post(token_url, json=data)

    print(f"📦 Ответ Shopify: {response.status_code} | {response.text}")

    if response.status_code != 200:
        print(f"❌ Ошибка авторизации! Shopify вернул {response.status_code} | {response.text}")
        return f"❌ Ошибка авторизации: {response.status_code} - {response.text}", 400

    try:
        json_response = response.json()
        access_token = json_response.get("access_token")
        if not access_token:
            print("❌ Ошибка: `access_token` отсутствует в ответе Shopify!")
            return f"❌ Ошибка: `access_token` не найден в ответе Shopify: {json_response}", 400

        print(f"✅ Shopify вернул токен: {access_token[:8]}***")

        # Сохраняем токен в Redis
        save_token(shop, access_token)

        response = make_response(redirect(f"/admin?shop={shop}"))
        response.set_cookie("shop", shop, httponly=True, samesite="None", secure=True)

        if redis_client.ping():
            start_sync_for_shop(shop, access_token)
        else:
            print("⚠️ Redis не подключен. Синхронизация не запущена.")

    except Exception as e:
        print(f"❌ Ошибка обработки JSON ответа Shopify: {e}")
        return f"❌ Ошибка обработки JSON ответа Shopify: {str(e)}", 400


@app.route("/admin")
def admin():
    """Встраиваемое приложение в Shopify Admin"""
    shop = request.args.get("shop") or request.cookies.get("shop")
    access_token = get_token(shop)

    if not shop or not access_token:
        print(f"❌ Ошибка: Токен для {shop} не найден или истёк.")
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
                    <button id="downloadCSV" type="button">Download CSV</button>
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
                            downloadBtn.addEventListener('click', function(event) {{
                                event.preventDefault();
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
        # 🔹 Проверка структуры первого товара
        if response:
            print(f"🔍 Пример товара: {json.dumps(response[0], indent=4, ensure_ascii=False)}")

        return response

    except Exception as e:
        print(f"❌ Ошибка получения товаров: {e}")
        return []


def fetch_product_info(product_id):
    """Запрос информации о товаре через dropshipping.getProductInfo с обработкой ошибки 403"""
    print(f"🔄 Запрос информации о товаре {product_id}...")

    client = None  # ✅ Инициализируем client заранее
    session = None
    max_retries = 3  # Количество попыток повторного подключения
    delay = 15  # Начальная задержка перед повтором

    for attempt in range(max_retries):
        try:
            print(f"🔄 Попытка {attempt + 1} подключения к WSDL...")
            client = Client(WSDL_URL)  # 🛠️ Попытка создать клиента SOAP
            session = client.service.login(USERNAME, PASSWORD)
            break  # Если успех — выходим из цикла

        except Exception as e:
            print(f"⚠️ Ошибка при подключении к WSDL: {e}")
            if "403" in str(e):
                print(f"⚠️ Ошибка 403. Ждём {delay} сек перед повторной попыткой...")
                time.sleep(delay)
                delay *= 2  # Увеличиваем задержку
            else:
                return None  # Прекращаем выполнение при других ошибках

    if not client or not session:
        print(f"❌ Не удалось подключиться к API PowerBody. Пропускаем товар {product_id}.")
        return None

    try:
        params = json.dumps({"id": str(product_id)})  # Преобразуем в строку JSON
        response = client.service.call(session, "dropshipping.getProductInfo", params)

        if isinstance(response, str):
            try:
                response = json.loads(response)
            except json.JSONDecodeError:
                print(f"❌ Ошибка декодирования JSON для товара {product_id}")
                return None

        client.service.endSession(session)
        return response

    except Exception as e:
        # 🛑 Если ошибка 403, пытаемся сделать повторный запрос
        if "403" in str(e):
            print(f"❌ Ошибка 403: Доступ запрещен для товара {product_id}. Логируем полный ответ...")

            try:
                response = client.service.call(session, "dropshipping.getProductInfo", {"id": str(product_id)})
                print(json.dumps(response, indent=4, ensure_ascii=False))
            except Exception as err:
                print(f"⚠️ Ошибка при повторном запросе для логирования: {err}")

        if "503" in str(e):
            print(f"⚠️ Ошибка 503 (Сервис недоступен). Ждём {delay} сек перед повтором...")
            time.sleep(delay)
            delay *= 2

        else:
            print(f"❌ Ошибка получения информации о товаре {product_id}: {e}")

        return None

def fetch_all_shopify_products(shop, access_token):
    print("🔄 Запрос товаров из Shopify API...")
    shopify_url = f"https://{shop}/admin/api/2024-01/products.json"
    headers = {"Content-Type": "application/json", "X-Shopify-Access-Token": access_token}
    params = {"fields": "id,variants", "limit": 250}
    all_products = []

    while True:
        time.sleep(0.6)  # ⏳ Shopify API ограничение: не более 2 запросов в секунду
        response = requests.get(shopify_url, headers=headers, params=params)

        if response.status_code == 429:  # Ошибка Too Many Requests
            retry_after = float(response.headers.get("Retry-After", 5))  # Исправлено: используем float
            print(f"⚠️ Ошибка 429 (Too Many Requests). Ждём {retry_after} секунд...")
            time.sleep(retry_after)  # Ждём перед повторной отправкой
            continue  # Повторяем запрос

        if response.status_code != 200:
            print(f"❌ Ошибка Shopify API: {response.status_code} | {response.text}")
            break

        products = response.json().get("products", [])
        all_products.extend(products)
        print(f"📦 Получено товаров: {len(products)}, всего: {len(all_products)}")

        # 🛑 Проверка лимитов API
        api_limit = response.headers.get("X-Shopify-Shop-Api-Call-Limit", "0/40")
        current_calls, max_calls = map(int, api_limit.split("/"))
        print(f"📊 Лимит API: {current_calls}/{max_calls}")

        if current_calls > max_calls * 0.8:  # Если загрузка API более 80%
            print(f"⚠️ API загружен ({current_calls}/{max_calls}). Ждём 2 сек...")
            time.sleep(2)  # Дополнительная пауза

        # Проверяем, есть ли следующая страница
        link_header = response.headers.get("Link")
        if link_header and 'rel="next"' in link_header:
            try:
                next_page_info = [l.split(";")[0].strip("<>") for l in link_header.split(",") if 'rel="next"' in l][0]
                params["page_info"] = next_page_info.split("page_info=")[1]
            except Exception as e:
                print(f"❌ Ошибка парсинга page_info: {e}")
                break
        else:
            break  # Если нет следующей страницы, выходим

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




def extract_flavor_advanced(product_name):
    if not product_name:
        return None, product_name

    packaging_keywords = [
        "pack", "caps", "grams", "ml", "softgels", "tabs", "vcaps", "servings",
        "g", "kg", "lb", "tablets", "capsules", "Small", "scoops", "Medium", "Mega Tabs", "x", "100 softgel"
    ]

    possible_flavors = [
        "Strawberry", "Chocolate", "Vanilla", "Orange", "Lemon", "Apple", "Peach", "Berry",
        "Tropical Punch", "Mango", "Pineapple", "Coconut", "Banana", "Raspberry", "Cherry",
        "Blueberry", "Watermelon", "Fruit Punch", "Caramel", "Mocha", "Hazelnut", "Cookies & Cream",
        "Neutral", "Grape", "Blue Raspberry", "Unflavored", "Original Flavor", "Grape Cooler", "Spearmint Flavor",
        "Grapefruit", "Cinnamon Vanilla", "Salted Caramel", "Peanut Butter", "Green Apple", "Triple Chocolate", "Tropic Blue", "Mandarin Orange",
        "French Vanilla", "Original", "Milk Chocolate", "Exotic Peach", "Chewable Orange", "Strawberry Banana",
        "Citrus", "Wild Berry Punch", "Pink Lemonade", "Vanilla Ice Cream", "Vanilla Ice Cream", "Vanilla Cake",
        "Chocolate Fudge Cake", "Grape Juiced", "Orange Juiced", "Raging Cola", "Lime Crime Mint",
        "Chocolate Peanut Butter", "Coconut Cream", "Chocolate Fudge Brownie", "Strawberry MilkShake",
        "Orange & Mango", "Smash Apple", "Strawberry Cream", "Vanilla Caramel", "Orange Cooler", "Spearmint Flavor",
        "Lemon Ice", "Fruit Punch Blast", "Sour Grape", "Lemon Ice", "Chocolate Fudge", "Citrus Lime", "Strawberry Kiwi",
        "Watermelon Blast", "Vanilla Cream", "Lemongrass", "Exotic Peach Mango", "Pomegranate Blueberry", "Lemon Twist",
        "Orange Juice", "Mango Pineapple", "Dutch Chocolate", "Vanilla Toffee", "Creamy Vanilla", "Cherry Limeade", "Wild Berry",
        "Blue Razz Watermelon", "Kiwi Strawberry", "Orange Mango", "Pineapple Mango", "Raspberry Lemonade", "Strawberry Limeade",
        "Strawberry Fit", "Forest Fruits", "Blueberry Madness", "Strawberry-Kiwi", "Unflavoured", "Cookies Cream", "Vanilla & Pineapple",
        "Raspberry Lemonade", "Vanilla Bean"
    ]

    # 🔽 Сортировка по убыванию длины — сначала длинные flavor-комбинации
    possible_flavors.sort(key=len, reverse=True)

    # 🔍 0. Исключение: если flavor в начале строки — НЕ считать его вкусом
    for flavor in possible_flavors:
        if product_name.strip().lower().startswith(flavor.lower()):
            return None, product_name.rstrip(",").strip()

    # 🔍 1. Явное совпадение со списком known flavors
    for flavor in possible_flavors:
        match = re.search(rf"\b{re.escape(flavor)}\b", product_name, re.IGNORECASE)
        if match:
            # 🛑 Проверка, не стоит ли после flavor слово "oil"
            after_match_pos = match.end()
            remainder = product_name[after_match_pos:].strip().lower()
            if remainder.startswith("oil"):
                continue  # Не считаем это вкусом

            item_name = re.sub(rf"\b{re.escape(flavor)}\b", "", product_name, flags=re.IGNORECASE).strip()
            item_name = item_name.rstrip(",").strip()
            return flavor, item_name

    # 🔍 2. Flavor через дефис или &
    match_combo = re.search(r"(\b\w+\b)\s*(?:[-,&])\s*(\b\w+\b)", product_name)
    if match_combo:
        part1 = match_combo.group(1)
        part2 = match_combo.group(2)
        combined = f"{part1} {part2}"
        if combined in possible_flavors:
            item_name = product_name.replace(match_combo.group(0), "").strip(" ,-")
            return combined, item_name

    # 🔍 3. with конструкция
    match_with = re.search(r"(.+?)\s+with\s+(.+)", product_name, re.IGNORECASE)
    if match_with:
        item_name = match_with.group(1).strip()
        possible_flavor = match_with.group(2).strip()
        if not re.search(r"\d", possible_flavor):
            return possible_flavor, item_name

    # 🔍 4. mg конструкция
    match_digit_flavor = re.search(r"(.+?)\s+(\d+mg)\s+(.+)", product_name)
    if match_digit_flavor:
        item_name = match_digit_flavor.group(1).strip()
        possible_flavor = match_digit_flavor.group(3).strip()
        if not re.search(r"\d", possible_flavor):
            return possible_flavor, item_name

    # 🔍 5. Flavor через запятую или дефис в конце
    match = re.search(r"(.+?)[,\-]\s*([\w\s&]+)$", product_name)
    if match:
        item_name = match.group(1).strip()
        possible_flavor = match.group(2).strip()
        if not re.search(r"\d", possible_flavor):
            return possible_flavor, item_name

    return None, product_name.rstrip(",").strip()



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

    max_retries = 5
    delay = 2  # Начальная задержка в секундах

    for attempt in range(max_retries):
        response = requests.put(update_variant_url, headers=headers, json=variant_data)

        if response.status_code == 200:
            print(f"✅ Успешно обновлена цена для variant {variant_id} (SKU: {sku}): {new_price}")
            break  # Выходим из цикла, если обновление прошло успешно
        elif response.status_code == 429:
            print(f"⚠️ Ошибка 429 (Too Many Requests) при обновлении цены {sku}. Повтор через {delay} секунд...")
            time.sleep(delay)
            delay *= 2  # Увеличиваем задержку в 2 раза
        else:
            print(
                f"❌ Ошибка обновления цены для variant {variant_id} (SKU: {sku}): {response.status_code} - {response.text}")
            break  # Прерываем цикл при других ошибках

    # Обновление количества товара
    update_inventory_url = f"https://{shop}/admin/api/2024-01/inventory_levels/set.json"
    inventory_data = {"location_id": 85726363936, "inventory_item_id": inventory_item_id, "available": new_quantity}

    delay = 2  # Сбрасываем задержку перед обновлением количества

    for attempt in range(max_retries):
        response = requests.post(update_inventory_url, headers=headers, json=inventory_data)

        if response.status_code == 200:
            print(f"✅ Количество обновлено для variant {variant_id} (SKU: {sku}): {new_quantity}")
            break
        elif response.status_code == 429:
            print(f"⚠️ Ошибка 429 (Too Many Requests) при обновлении количества {sku}. Повтор через {delay} секунд...")
            time.sleep(delay)
            delay *= 2  # Увеличиваем задержку
        else:
            print(
                f"❌ Ошибка обновления количества для variant {variant_id} (SKU: {sku}): {response.status_code} - {response.text}")
            break


def sync_products(shop):
    """Полная синхронизация товаров с немедленной записью в CSV (с использованием временного файла)"""
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

    shopify_sku_map = {
        v.get("sku"): (v["id"], v.get("inventory_item_id"), v.get("price"), v.get("inventory_quantity"))
        for p in shopify_products for v in p["variants"] if v.get("sku")
    }

    synced_count = 0
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    temp_filename = os.path.join(CSV_DIR, f"~sync_temp_{timestamp}.csv")
    final_filename = os.path.join(CSV_DIR, f"sync_report_{timestamp}.csv")

    # Создаём временный CSV-файл и записываем заголовки
    with open(temp_filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        header = ["SKU", "Brand Name", "Item Name", "Flavor", "Weight (grams)", "EAN", "Price API", "Price Shopify",
                  "Quantity"]
        writer.writerow(header)

        for pb_product in powerbody_products:
            if not isinstance(pb_product, dict):
                continue

            sku = pb_product.get("sku")
            product_id = str(pb_product.get("product_id") or "").strip()

            if not product_id or not sku or sku not in shopify_sku_map:
                print(f"⚠️ Пропущен товар SKU `{sku}`, product_id: `{product_id}`")
                continue

            print(f"🔄 Запрос информации о товаре `{product_id}` для SKU `{sku}`...")
            product_info = fetch_product_info(product_id)

            if not product_info:
                print(f"⚠️ Не удалось получить информацию о товаре `{product_id}`. Пропускаем.")
                continue

            base_price = float(pb_product.get("retail_price", pb_product.get("price", "0.00")) or 0.00)
            new_quantity = pb_product.get("qty")

            variant_id, inventory_item_id, old_price, old_quantity = shopify_sku_map[sku]

            brand_name = product_info.get("manufacturer")
            name = product_info.get("name", "")
            weight = product_info.get("weight", 0)
            ean = product_info.get("ean")

            # 🆕 Определяем `Flavor` и корректируем `Item Name`
            flavor, item_name = extract_flavor_advanced(name)

            # 🆕 Если `Flavor` пустой, записываем `None`
            if not flavor or flavor.lower() == "non":
                flavor = None

            # 🆕 Переводим вес в граммы
            weight_grams = int(float(weight) * 1000) if weight else None

            # 🛒 Рассчитываем новую цену
            final_price = calculate_final_price(base_price, vat, paypal_fees, second_paypal_fees, profit)

            # 💾 Записываем в CSV сразу!
            clean_item_name = re.sub(r"\s*,\s*", " ", item_name).strip() if item_name else None
            row = [sku, brand_name, clean_item_name, flavor, weight_grams, ean, base_price, final_price, new_quantity]
            writer.writerow(row)
            print(f"📦 Полное имя из PowerBody: {name}")
            print(f"✅ Записано в CSV: {row}")

            # 🔄 Проверяем, нужно ли обновлять товар
            if old_price != final_price or old_quantity != new_quantity:
                print(f"🔄 Обновляем SKU `{sku}`: Цена API `{base_price}` → Shopify `{final_price}`, Количество: `{old_quantity}` → `{new_quantity}`")
                update_shopify_variant(shop, access_token, variant_id, inventory_item_id, final_price, new_quantity, sku)
                synced_count += 1
                time.sleep(0.6)  # 🛑 Shopify API лимит - не более 2 запросов в секунду

    # ✅ Переименовываем временный файл в финальный только после успешной записи
    os.rename(temp_filename, final_filename)
    print(f"✅ Синхронизация завершена! Обновлено товаров: {synced_count}")
    print(f"📂 CSV-файл окончательно сохранён: `{final_filename}`")
    return final_filename




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
    """Сохраняет данные в CSV с логированием каждой строки после записи."""
    if not data:
        print("⚠️ Нет данных для сохранения в CSV.")
        return None  # Выход, если данных нет

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = os.path.join(CSV_DIR, f"sync_report_{timestamp}.csv")

    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        header = ["SKU", "Brand Name", "Item Name", "Flavor", "Weight (grams)", "EAN", "Price API", "Price Shopify",
                  "Quantity"]
        writer.writerow(header)

        print("\n📌 **Начинаем сохранение CSV-отчёта...**")
        print("------------------------------------------------------")
        print("📝 Заголовки:", header)

        for row in data:
            writer.writerow(row)
            print(f"✅ Добавлена запись в CSV: {row}")  # 🔥 Лог сразу после записи

        print("------------------------------------------------------")
        print(f"✅ CSV файл сохранён: `{filename}`")

    # 🔍 Проверяем содержимое файла после сохранения
    with open(filename, "r", encoding="utf-8") as file:
        print("\n🔍 **Содержимое CSV-файла:**")
        print(file.read())

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
        return "❌ No CSV files available.", 404
    return send_file(latest_file, as_attachment=True)


# 🔄 Запуск фоновой синхронизации
def start_sync_for_shop(shop, access_token):
    job_id = f"sync_{shop}"
    existing_job = scheduler.get_job(job_id)

    if not existing_job:
        print(f"🕒 Запуск фоновой синхронизации для {shop} каждые 300 минут.")
        scheduler.add_job(sync_products, 'interval', minutes=300, args=[shop], id=job_id, replace_existing=True)


# 🔄 Запуск фоновой синхронизации при старте сервера
def schedule_sync():
    """Запускает синхронизацию для всех магазинов, у которых сохранены токены в Redis."""
    keys = redis_client.keys("shopify_token:*")  # Получаем все ключи с токенами
    if not keys:
        print("❌ Нет сохранённых токенов в Redis.")
        return

    for key in keys:
        shop = key.split("shopify_token:")[-1]  # Извлекаем название магазина
        access_token = redis_client.get(key)

        if access_token:
            start_sync_for_shop(shop, access_token)
            print(f"🔄 Запущена синхронизация для {shop}")
        else:
            print(f"⚠️ Токен для {shop} отсутствует в Redis.")


if __name__ == "__main__":
    print("🚀 Запуск фоновой синхронизации...")
    schedule_sync()
    app.run(host='0.0.0.0', port=80, debug=False)