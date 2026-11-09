import os
import sys
import time
import signal
import atexit
import requests
import base64
import json
import threading
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
import copy
import mimetypes 
import tempfile 

mimetypes.init()
mimetypes.add_type('application/vnd.openxmlformats-officedocument.wordprocessingml.document', '.docx')
mimetypes.add_type('application/vnd.ms-excel', '.xls')
mimetypes.add_type('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx')

# --- تنظیمات Secrets ---
GEMINI_API_KEYS_ENV = os.getenv("GEMINI_API_KEYS", "") 
GEMINI_KEYS = [k.strip() for k in GEMINI_API_KEYS_ENV.split(",") if k.strip()]

BALE_TOKEN = os.getenv("BALE_TOKEN")
API_DOMAIN = "generativelanguage.googleapis.com"

DATABASE_URL = os.getenv("DATABASE_URL")

SPACE_URL = os.getenv("SPACE_URL", "")

# --- تنظیمات درگاه و ادمین ---
SUPER_ADMIN_ID = 270871838 
BALE_WALLET_TOKEN = os.getenv("BALE_WALLET_TOKEN", "") 

# --- متغیرهای حافظه و دسترسی ---
user_memories = {}
user_archives = {}
users_db = {} 
allowed_users = set()

bot_config = {
    "sub_price_rial": 2000000,
    "trial_enabled": True,
    "sales_count": 0,
    "trial_queue": [],
    "plan_perms": {
        "standard": {"text": False, "voice": False, "photo": False, "audio": False, "video": False, "document": False, "other": False},
        "trial": {"text": True, "voice": False, "photo": False, "audio": False, "video": False, "document": False, "other": False},
        "premium": {"text": True, "voice": True, "photo": True, "audio": True, "video": True, "document": True, "other": True},
        "vip": {"text": True, "voice": True, "photo": True, "audio": True, "video": True, "document": True, "other": True},
        "admin": {"text": True, "voice": True, "photo": True, "audio": True, "video": True, "document": True, "other": True}
    }
}

global_api_history = [] 

admin_states = {} 
user_states = {}
pending_phone_users = set() 
sales_open = True 

last_action_time = {}

DIRECT_PROXY = {"http": None, "https": None}
VLESS_PROXY_URL = "http://127.0.0.1:10809"
VLESS_PROXY = {"http": VLESS_PROXY_URL, "https": VLESS_PROXY_URL}

class _BaleFallbackSession(requests.Session):
    def request(self, method, url, *args, **kwargs):
        if "proxies" not in kwargs:
            try:
                res = super().request(method, url, *args, proxies=DIRECT_PROXY, **kwargs)
                if res.status_code < 500:
                    return res
                print(f"⚠️ خطای سرور بله در اتصال مستقیم (HTTP {res.status_code})؛ تلاش با پروکسی جایگزین VLESS...", flush=True)
            except Exception as e:
                if globals().get("_IS_SHUTTING_DOWN", False):
                    raise
                print(f"⚠️ خطا در اتصال مستقیم به بله ({e})؛ تلاش با پروکسی جایگزین VLESS...", flush=True)
            return super().request(method, url, *args, proxies=VLESS_PROXY, **kwargs)
        return super().request(method, url, *args, **kwargs)

bale_session = _BaleFallbackSession()
adapter = requests.adapters.HTTPAdapter(pool_connections=25, pool_maxsize=25)
bale_session.mount('https://', adapter)
bale_session.mount('http://', adapter)

db_pool = None
user_locks = {}
global_lock = threading.Lock()
api_history_lock = threading.Lock()

def get_user_lock(chat_id):
    with global_lock:
        if chat_id not in user_locks:
            user_locks[chat_id] = threading.Lock()
        return user_locks[chat_id]

executor = ThreadPoolExecutor(max_workers=20)

def notify_admin_error(context, exception=None):
    try:
        error_msg = f"⚠️ **هشدار سیستم (Error)**\n\n📌 بخش: {context}"
        if exception:
            error_msg += f"\nجزئیات: {str(exception)}"
        payload = {"chat_id": SUPER_ADMIN_ID, "text": error_msg}
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendMessage", json=payload, timeout=10)
        print(error_msg, flush=True) 
    except:
        pass

class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(b'{"status": "Bot is running and awake!"}')
 
    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        return

def run_health_check_server():
    server_address = ('0.0.0.0', 7860)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    print("Health check server mapped to 0.0.0.0:7860", flush=True)
    httpd.serve_forever()

# ==========================================
# سیستم جامع مدیریت Cache در رم (صفر کردن درگیری دیتابیس در طول اجرا)
# ==========================================
_STORE_CACHE = {}
_DIRTY_KEYS = set()
_DELETED_KEYS = set()
_DB_CACHE_LOCK = threading.Lock()
_IS_SHUTTING_DOWN = False

def init_db():
    global _STORE_CACHE
    if not DATABASE_URL:
        print("❌ ارور: آدرس دیتابیس DATABASE_URL یافت نشد.", flush=True)
        return
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS store (
            key VARCHAR PRIMARY KEY,
            data JSONB
        )
        """)
        conn.commit()
        cur.execute("SELECT key, data FROM store")
        rows = cur.fetchall()
        with _DB_CACHE_LOCK:
            for k, v in rows:
                _STORE_CACHE[str(k)] = v
        cur.close()
        print("Database initialized and loaded into RAM successfully.", flush=True)
    except Exception as e:
        if conn and conn.closed == 0: 
            try: conn.rollback()
            except: pass
        print(f"DB Init Error: {e}", flush=True)
    finally:
        if conn and conn.closed == 0:
            try: conn.close()
            except: pass

def db_save(key, data):
    with _DB_CACHE_LOCK:
        _STORE_CACHE[str(key)] = copy.deepcopy(data)
        _DIRTY_KEYS.add(str(key))
        _DELETED_KEYS.discard(str(key))

def db_load(key, default_val):
    with _DB_CACHE_LOCK:
        if str(key) in _STORE_CACHE:
            return copy.deepcopy(_STORE_CACHE[str(key)])
        return default_val

def load_all_users_from_db():
    loaded_users = {}
    with _DB_CACHE_LOCK:
        for k, v in _STORE_CACHE.items():
            if k.startswith("usr_"):
                uid = k.replace("usr_", "", 1)
                loaded_users[uid] = copy.deepcopy(v)
    return loaded_users

def flush_all_to_db():
    global _DIRTY_KEYS, _DELETED_KEYS
    if not DATABASE_URL:
        return
    with _DB_CACHE_LOCK:
        if not _DIRTY_KEYS and not _DELETED_KEYS:
            return
        keys_to_save = {k: copy.deepcopy(_STORE_CACHE[k]) for k in _DIRTY_KEYS if k in _STORE_CACHE}
        keys_to_delete = set(_DELETED_KEYS)
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        for k in keys_to_delete:
            cur.execute("DELETE FROM store WHERE key = %s", (k,))
        for k, v in keys_to_save.items():
            cur.execute("""
            INSERT INTO store (key, data)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (key) DO UPDATE
            SET data = EXCLUDED.data;
            """, (k, json.dumps(v)))
        conn.commit()
        cur.close()
        with _DB_CACHE_LOCK:
            for k in keys_to_save:
                _DIRTY_KEYS.discard(k)
            for k in keys_to_delete:
                _DELETED_KEYS.discard(k)
        print("✅ تمامی اطلاعات رم (ربات بله اسیستنت) با موفقیت در دیتابیس ذخیره شد.", flush=True)
    except Exception as e:
        if conn and conn.closed == 0:
            try: conn.rollback()
            except: pass
        print(f"❌ خطای همگام‌سازی دیتابیس بله اسیستنت در زمان خروج: {e}", flush=True)
    finally:
        if conn and conn.closed == 0:
            try: conn.close()
            except: pass

# --- دیسپچ اجرای بعدی ورک‌فلو: زنجیره خودکار اجراها ---
# موقع خاموشی، بعد از فلاش نهایی، با GITHUB_TOKEN همین ورک‌فلو را دوباره
# دیسپچ می‌کند تا اگر cron گیت‌هاب جا ماند، زنجیره قطع نشود.
# (رویداد workflow_dispatch با GITHUB_TOKEN هم اجرای جدید می‌سازد.)
# اول چک می‌کند اجرای دیگری در صف یا در حال اجرا نباشد تا اجرای تکراری
# ساخته نشود — مثلاً وقتی لغو به‌خاطر cron بوده و اجرای بعدی از قبل هست.
def _dispatch_next_run():
    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if not token or not repo or not run_id:
        print("⚠️ دیسپچ اجرای بعدی انجام نشد: GITHUB_TOKEN یا اطلاعات اجرا در دسترس نیست.", flush=True)
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        r = requests.get(
            f"https://api.github.com/repos/{repo}/actions/workflows/bot.yml/runs?per_page=10",
            headers=headers, timeout=15)
        if r.status_code == 200:
            for run in r.json().get("workflow_runs", []):
                if str(run.get("id")) != str(run_id) and run.get("status") in ("queued", "in_progress", "waiting", "requested", "pending"):
                    print(f"⏭️ اجرای بعدی از قبل وجود دارد (run #{run.get('id')})؛ دیسپچ انجام نشد.", flush=True)
                    return
        else:
            print(f"⚠️ بررسی اجراهای موجود ناموفق بود (HTTP {r.status_code})؛ دیسپچ انجام می‌شود.", flush=True)
    except Exception as e:
        print(f"⚠️ بررسی اجراهای موجود ناموفق بود ({e})؛ دیسپچ انجام می‌شود.", flush=True)
    try:
        ref = os.environ.get("GITHUB_REF_NAME", "main")
        r = requests.post(
            f"https://api.github.com/repos/{repo}/actions/workflows/bot.yml/dispatches",
            headers=headers, json={"ref": ref}, timeout=15)
        if r.status_code == 204:
            print("✅ اجرای بعدی ورک‌فلو با موفقیت دیسپچ شد.", flush=True)
        else:
            print(f"⚠️ دیسپچ اجرای بعدی ناموفق بود (HTTP {r.status_code}): {r.text[:200]}", flush=True)
    except Exception as e:
        print(f"⚠️ دیسپچ اجرای بعدی ناموفق بود: {e}", flush=True)

def _handle_shutdown_signal(signum, frame):
    global _IS_SHUTTING_DOWN
    if _IS_SHUTTING_DOWN:
        return
    _IS_SHUTTING_DOWN = True
    print("🛑 سیگنال توقف در بله اسیستنت دریافت شد. در حال ذخیره دیتا از رم به دیتابیس...", flush=True)
    flush_all_to_db()
    _dispatch_next_run()
    sys.exit(0)

atexit.register(flush_all_to_db)
signal.signal(signal.SIGTERM, _handle_shutdown_signal)
signal.signal(signal.SIGINT, _handle_shutdown_signal)

def load_databases():
    global allowed_users, users_db, user_memories, user_archives, bot_config, global_api_history, sales_open
    allowed_list = db_load("whitelist", [])
    allowed_users = set(allowed_list)
    
    old_users_db = db_load("users_db", None)
    if old_users_db is not None:
        print("Migrating old users_db to individual rows...")
        for uid, data in old_users_db.items():
            db_save(f"usr_{uid}", data)
        with _DB_CACHE_LOCK:
            _STORE_CACHE.pop("users_db", None)
            _DIRTY_KEYS.discard("users_db")
            _DELETED_KEYS.add("users_db")
    
    users_db = load_all_users_from_db()
    
    with _DB_CACHE_LOCK:
        for k, v in _STORE_CACHE.items():
            if k.startswith("mem_"):
                uid = k.replace("mem_", "", 1)
                user_memories[uid] = copy.deepcopy(v)
            elif k.startswith("arc_"):
                uid = k.replace("arc_", "", 1)
                user_archives[uid] = copy.deepcopy(v)
    
    saved_config = db_load("bot_config", {})
    if saved_config:
        if "plan_perms" not in saved_config:
            saved_config["plan_perms"] = bot_config["plan_perms"]
        else:
            for p in ["standard", "trial", "premium", "vip", "admin"]:
                if p not in saved_config["plan_perms"]:
                    saved_config["plan_perms"][p] = bot_config["plan_perms"].get(p, bot_config["plan_perms"]["standard"])
                else:
                    for k in ["text", "voice", "photo", "audio", "video", "document", "other"]:
                        if k not in saved_config["plan_perms"][p]:
                            saved_config["plan_perms"][p][k] = bot_config["plan_perms"][p].get(k, True)
        bot_config.update(saved_config)
    
    sales_open = db_load("sales_status", True)
    
    history = db_load("token_history_24h", [])
    now = time.time()
    global_api_history = [record for record in history if now - record[0] < 86400]

def save_whitelist(): 
    try:
        w_list = list(allowed_users)
        db_save("whitelist", w_list)
    except Exception as e:
        notify_admin_error("ذخیره لیست سفید", e)

def save_user_db(chat_id): 
    try:
        chat_id_str = str(chat_id)
        if chat_id_str in users_db:
            usr_snap = copy.deepcopy(users_db[chat_id_str])
            db_save(f"usr_{chat_id_str}", usr_snap)
    except Exception as e:
        notify_admin_error(f"ذخیره دیتابیس کاربر {chat_id}", e)

def save_config():
    try:
        cfg_snap = copy.deepcopy(bot_config)
        db_save("bot_config", cfg_snap)
        db_save("sales_status", sales_open)
    except Exception as e:
        notify_admin_error("ذخیره تنظیمات ربات (Config)", e)

def load_user_memory(chat_id):
    chat_id_str = str(chat_id)
    if chat_id_str not in user_memories:
        user_memories[chat_id_str] = db_load(f"mem_{chat_id_str}", [])

def save_user_memory(chat_id):
    chat_id_str = str(chat_id)
    if chat_id_str in user_memories:
        try:
            mem_snap = copy.deepcopy(user_memories[chat_id_str])
            db_save(f"mem_{chat_id_str}", mem_snap)
        except Exception as e:
            notify_admin_error(f"ذخیره حافظه چت کاربر {chat_id}", e)

def load_user_archives(chat_id):
    chat_id_str = str(chat_id)
    if chat_id_str not in user_archives:
        user_archives[chat_id_str] = db_load(f"arc_{chat_id_str}", [])

def save_user_archives(chat_id):
    chat_id_str = str(chat_id)
    if chat_id_str in user_archives:
        try:
            arc_snap = copy.deepcopy(user_archives[chat_id_str])
            db_save(f"arc_{chat_id_str}", arc_snap)
        except Exception as e:
            notify_admin_error(f"ذخیره بایگانی کاربر {chat_id}", e)

def init_user_db(chat_id):
    chat_id_str = str(chat_id)
    if chat_id_str not in users_db:
        users_db[chat_id_str] = {
            "trial_start": 0, 
            "premium_until": 0,
            "is_vip": False,
            "tokens": 0, 
            "requests": 0, 
            "warnings": 0, "bans": 0, "ban_until": 0, "last_msg_time": 0,
            "phone": "", "admin_msg_count": 0, "selected_model": "پیش‌فرض",
            "return_as_word": False
        }
        save_user_db(chat_id)
    else:
        changed = False
        if "phone" not in users_db[chat_id_str]: 
            users_db[chat_id_str]["phone"] = ""
            changed = True
        if "admin_msg_count" not in users_db[chat_id_str]: 
            users_db[chat_id_str]["admin_msg_count"] = 0
            changed = True
        if "is_vip" not in users_db[chat_id_str]:
            users_db[chat_id_str]["is_vip"] = False
            changed = True
        if "premium_until" not in users_db[chat_id_str]:
            users_db[chat_id_str]["premium_until"] = 0
            changed = True
        if "trial_start" not in users_db[chat_id_str]:
            users_db[chat_id_str]["trial_start"] = 0
            changed = True
        if "selected_model" not in users_db[chat_id_str]:
            users_db[chat_id_str]["selected_model"] = "پیش‌فرض"
            changed = True
        if users_db[chat_id_str].get("selected_model") == "default" or users_db[chat_id_str].get("selected_model") == "تصویرساز (Imagen)":
            users_db[chat_id_str]["selected_model"] = "پیش‌فرض"
            changed = True
        if "requests" not in users_db[chat_id_str]:
            users_db[chat_id_str]["requests"] = 0
            changed = True
        if "return_as_word" not in users_db[chat_id_str]:
            users_db[chat_id_str]["return_as_word"] = False
            changed = True
        
        if changed:
            save_user_db(chat_id)

def format_price(price_rial):
    return f"{price_rial:,} ریال"

def get_user_plan(chat_id):
    if chat_id == SUPER_ADMIN_ID: 
        return "admin"
    chat_id_str = str(chat_id)
    user = users_db.get(chat_id_str)
    if not user: 
        return "standard" 
    if user.get("is_vip", False): 
        return "vip"
    if user.get("premium_until", 0) > time.time(): 
        return "premium"
    
    trial_start = user.get("trial_start", 0)
    if trial_start > 0 and (time.time() - trial_start < 3600):
        return "trial"
    
    return "standard" 

def get_plan_persian_name(plan):
    names = {
        "standard": "کاربران عادی (استارت‌کننده)",
        "trial": "تست کنندگان یک ساعته (فعال)",
        "premium": "خریداران",
        "vip": "کسانی که دسترسی ویژه دارند (VIP)",
        "admin": "ادمین‌ها"
    }
    return names.get(plan, plan)

def is_text_document(mime_type, file_name):
    if not mime_type: 
        return False
    mime_type = mime_type.lower()
    file_name = file_name.lower() if file_name else ""
    text_mimes = ["text/plain", "application/pdf", "application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "application/rtf", "application/vnd.oasis.opendocument.text"]
    text_extensions = [".txt", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".csv", ".rtf", ".odt"]
    if mime_type in text_mimes or any(mime_type.startswith(x) for x in ["text/"]): 
        return True
    if any(file_name.endswith(ext) for ext in text_extensions): 
        return True
    return False

def get_user_actual_perms(chat_id_str):
    plan = get_user_plan(chat_id_str)
    return bot_config["plan_perms"].get(plan, {})

def get_welcome_prompt(chat_id):
    chat_id_str = str(chat_id)
    init_user_db(chat_id)
    plan = get_user_plan(chat_id)
    actual_perms = bot_config["plan_perms"].get(plan, {})
    has_file_access = chat_id == SUPER_ADMIN_ID or any(actual_perms.values())
    if has_file_access: 
        return "برای شروع مکالمه کافیست پیام متنی یا فایل خود را بفرستید."
    else: 
        return "برای شروع مکالمه کافیست پیام متنی خود را بفرستید."

def get_plan_perms_text(plan_name):
    perms = bot_config["plan_perms"].get(plan_name, {})
    return (f"متن:{'✅' if perms.get('text', True) else '❌'} | ویس:{'✅' if perms.get('voice', False) else '❌'} | عکس:{'✅' if perms.get('photo', False) else '❌'} | صدا:{'✅' if perms.get('audio', False) else '❌'} | ویدیو:{'✅' if perms.get('video', False) else '❌'} | سند متنی:{'✅' if perms.get('document', False) else '❌'} | دیگر فرمت‌ها:{'✅' if perms.get('other', False) else '❌'}")

def log_api_usage(token_count):
    global global_api_history
    now = time.time()
    with api_history_lock:
        global_api_history = [record for record in global_api_history if now - record[0] < 86400]
        global_api_history.append((now, token_count, 1))
        hist_snap = copy.deepcopy(global_api_history)
        db_save("token_history_24h", hist_snap)

def get_24h_api_usage():
    now = time.time()
    with api_history_lock:
        valid_history = [record for record in global_api_history if now - record[0] < 86400]
        total_tokens = sum(record[1] for record in valid_history)
        total_requests = sum(record[2] if len(record) > 2 else 1 for record in valid_history)
        return total_tokens, total_requests

def send_msg(chat_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "text": text}
    if keyboard: 
        payload["reply_markup"] = json.loads(keyboard) if isinstance(keyboard, str) else keyboard
    try: 
        res = bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendMessage", json=payload, timeout=15).json()
        if res.get("ok"): 
            return res["result"]["message_id"]
    except: 
        pass
    return None

def edit_msg(chat_id, msg_id, text, keyboard=None):
    if not msg_id: 
        return
    payload = {"chat_id": chat_id, "message_id": msg_id, "text": text}
    if keyboard: 
        payload["reply_markup"] = json.loads(keyboard) if isinstance(keyboard, str) else keyboard
    try: 
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/editMessageText", json=payload, timeout=15)
    except: 
        pass

def send_typing(chat_id):
    try: 
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendChatAction", json={"chat_id": chat_id, "action": "typing"}, timeout=5)
    except: 
        pass

def send_document(chat_id, file_data_b64, file_name, caption=""):
    try:
        file_bytes = base64.b64decode(file_data_b64)
        data = {"chat_id": chat_id, "caption": caption}
        files = {"document": (file_name, file_bytes)}
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendDocument", data=data, files=files, timeout=30)
    except Exception as e: 
        print(f"send_document error: {e}")

def download_bale_file(file_id):
    try:
        info_url = f"https://tapi.bale.ai/bot{BALE_TOKEN}/getFile?file_id={file_id}"
        file_info = bale_session.get(info_url, timeout=10).json()
        if file_info.get("ok"):
            path = file_info["result"]["file_path"]
            down_url = f"https://tapi.bale.ai/file/bot{BALE_TOKEN}/{path}"
            
            response = bale_session.get(down_url, stream=True, timeout=20)
            response.raise_for_status()
            
            max_size_bytes = 20 * 1024 * 1024 
            downloaded_size = 0
            
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        downloaded_size += len(chunk)
                        if downloaded_size > max_size_bytes:
                            temp_file.close()
                            os.remove(temp_file.name)
                            print("File size exceeded 20MB limit.")
                            return None, None
                        temp_file.write(chunk)
                temp_file_path = temp_file.name

            with open(temp_file_path, "rb") as f:
                encoded_string = base64.b64encode(f.read()).decode('utf-8')
            
            os.remove(temp_file_path)
            return encoded_string, file_info["result"].get("mime_type", "application/octet-stream")
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None, None

def send_invoice(chat_id):
    prem_perms_info = get_plan_perms_text("premium")
    payload = {
        "chat_id": chat_id,
        "title": "اشتراک یک‌ساله ربات هوش مصنوعی",
        "description": f"فعال‌سازی اشتراک پیشرفته (متن نامحدود)\nدسترسی‌های این پلن:\n{prem_perms_info}",
        "payload": f"sub_payment_{chat_id}",
        "provider_token": BALE_WALLET_TOKEN,
        "prices": [{"label": "اشتراک یک‌ساله", "amount": bot_config["sub_price_rial"]}]
    }
    try: 
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendInvoice", json=payload, timeout=15)
    except Exception as e: 
        print(f"Invoice error: {e}")

def answer_pre_checkout_query(query_id, ok, error_message=""):
    payload = {"pre_checkout_query_id": query_id, "ok": ok}
    if not ok: 
        payload["error_message"] = error_message
    try: 
        bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/answerPreCheckoutQuery", json=payload, timeout=15)
    except Exception as e: 
        print(f"PreCheckout error: {e}")

def get_user_keyboard(chat_id):
    chat_id_str = str(chat_id)
    load_user_memory(chat_id)
    init_user_db(chat_id)
    
    has_access, _ = check_access(chat_id)
    user_data = users_db.get(chat_id_str, {})
    used_trial = user_data.get("trial_start", 0) > 0
    
    row1 = []
    if has_access:
        row1.append({"text": "💬 گفتگوی جدید"})
    elif not used_trial:
        row1.append({"text": "⏳ فعال‌سازی تست ۱ ساعته"})
    
    keyboard = {
        "keyboard": [
            row1,
            [{"text": "💳 خرید اشتراک یک‌ساله"}, {"text": "⚙️ تنظیمات"}]
        ],
        "resize_keyboard": True
    }
    
    keyboard["keyboard"] = [row for row in keyboard["keyboard"] if row]

    if chat_id == SUPER_ADMIN_ID:
        keyboard["keyboard"].append([{"text": "🎨 تغییر مدل / رسانه ساز"}])
        keyboard["keyboard"].append([{"text": "👑 پنل مدیریت"}])
    
    return json.dumps(keyboard)

def get_settings_keyboard():
    return json.dumps({
        "keyboard": [
            [{"text": "ℹ️ وضعیت اشتراک"}, {"text": "🆔 شناسه کاربری من"}],
            [{"text": "🗄 بایگانی گفتگو"}, {"text": "📂 مشاهده بایگانی‌های گذشته"}],
            [{"text": "📞 ارتباط با پشتیبانی"}],
            [{"text": "🔙 بازگشت به منوی اصلی"}]
        ],
        "resize_keyboard": True
    })

def get_admin_users_keyboard(page=1):
    global sales_open
    keyboard = {"keyboard": [], "resize_keyboard": True}
    
    status_btn = "🔴 بستن فروش" if sales_open else "🟢 باز کردن فروش"
    trial_btn = "🔴 توقف تست رایگان" if bot_config["trial_enabled"] else "🟢 باز کردن تست رایگان"
    
    keyboard["keyboard"].append([{"text": status_btn}, {"text": trial_btn}])
    keyboard["keyboard"].append([{"text": "💰 تغییر قیمت اشتراک"}, {"text": "📦 پلن‌ها"}])
    keyboard["keyboard"].append([{"text": "➕ افزودن گروهی به لیست سفید"}, {"text": "📢 پیام همگانی"}])
    keyboard["keyboard"].append([{"text": "📈 آمار API (۲۴ساعت)"}, {"text": "🔙 بازگشت به منوی اصلی"}])
    return json.dumps(keyboard)

def get_admin_plan_perms_keyboard():
    kb = {
        "keyboard": [
            [{"text": "⚙️ کاربران عادی (استارت‌کننده)"}],
            [{"text": "⚙️ تست کنندگان یک ساعته"}],
            [{"text": "⚙️ خریداران"}],
            [{"text": "⚙️ کسانی که دسترسی ویژه دارند (VIP)"}],
            [{"text": "⚙️ ادمین‌ها"}],
            [{"text": "🔙 بازگشت به پنل مدیریت"}]
        ],
        "resize_keyboard": True
    }
    return json.dumps(kb)

def get_admin_plan_details_keyboard(plan):
    perms = bot_config["plan_perms"].get(plan, {})
    kb = {
        "keyboard": [
            [{"text": f"➕ افزودن کاربر با آیدی ({get_plan_persian_name(plan)})"}],
            [{"text": f"❌ حذف کاربر با آیدی ({get_plan_persian_name(plan)})"}]
        ],
        "resize_keyboard": True
    }
    for k in ["text", "voice", "photo", "audio", "video", "document", "other"]:
        status = "🟢 روشن" if perms.get(k, False) else "🔴 خاموش"
        kb["keyboard"].append([{"text": f"تغییر دسترسی {plan} {k}: {status}"}])
    kb["keyboard"].append([{"text": "⚙️ تنظیمات دسترسی فایل (پلن‌ها)"}])
    return json.dumps(kb)

def get_archives_keyboard(chat_id):
    chat_id_str = str(chat_id)
    load_user_archives(chat_id)
    archives = user_archives.get(chat_id_str, [])
    
    kb = {"keyboard": [], "resize_keyboard": True}
    for i, arc in enumerate(archives):
        date_str = arc.get("date", "")[:16] 
        kb["keyboard"].append([{"text": f"فایل بایگانی {i+1}: {date_str}"}])
    
    kb["keyboard"].append([{"text": "🔙 بازگشت به منوی اصلی"}])
    return json.dumps(kb)

def check_access(chat_id):
    if chat_id == SUPER_ADMIN_ID: 
        return True, ""
    chat_id_str = str(chat_id)
    init_user_db(chat_id)
    user = users_db[chat_id_str]
    now = time.time()

    if user["ban_until"] > now:
        remain = int((user["ban_until"] - now) // 60)
        return False, f"⛔️ شما به دلیل استفاده غیرمنصفانه تا {remain} دقیقه دیگر مسدود هستید."

    if user.get("is_vip", False): 
        return True, ""
    if user.get("premium_until", 0) > now: 
        return True, ""
    
    has_any_standard_perm = any(bot_config["plan_perms"].get("standard", {}).values())
    if has_any_standard_perm:
        return True, ""
    
    if user["trial_start"] > 0:
        if now - user["trial_start"] < 3600: 
            return True, ""
        return False, f"⏳ مهلت تست ۱ ساعته شما به پایان رسیده است.\n\n💳 برای ادامه دسترسی از طریق دکمه «خرید اشتراک» اقدام نمایید."
    
    return False, "شما اشتراک فعالی ندارید. برای شروع تست ۱ ساعته رایگان دکمه مربوطه را بزنید."

def handle_fair_usage(chat_id):
    if chat_id == SUPER_ADMIN_ID: 
        return True
    chat_id_str = str(chat_id)
    user = users_db[chat_id_str]
    now = time.time()
    
    if now - user["last_msg_time"] < 5: 
        user["warnings"] += 1
        if user["warnings"] >= 5:
            user["bans"] += 1
            user["warnings"] = 0
            user["ban_until"] = now + 300 
            save_user_db(chat_id)
            if user["bans"] >= 5: 
                send_msg(SUPER_ADMIN_ID, f"⚠️ کاربر {chat_id} مسدود شد!")
            send_msg(chat_id, "⛔️ اخطار: به دلیل ارسال سریع درخواست‌ها، شما به مدت ۵ دقیقه مسدود شدید.")
            return False
        
        save_user_db(chat_id)
        send_msg(chat_id, f"⚠️ لطفا بین درخواست‌ها فاصله بیندازید. (اخطار {user['warnings']} از 5)")
        return False

    user["warnings"] = 0 
    user["last_msg_time"] = now
    save_user_db(chat_id)
    return True

def process_trial_queue():
    while True:
        time.sleep(60)
        try:
            active_trials = sum(1 for u in users_db.values() if 0 < u.get("trial_start", 0) and time.time() - u["trial_start"] < 3600)
            changed = False
            
            while active_trials < 10 and len(bot_config["trial_queue"]) > 0:
                next_user = bot_config["trial_queue"].pop(0)
                u_str = str(next_user)
                if u_str in users_db:
                    users_db[u_str]["trial_start"] = time.time()
                    save_user_db(next_user)
                    send_msg(next_user, "🎉 نوبت شما رسید! تست رایگان ۱ ساعته شما از همین الان آغاز شد.", get_user_keyboard(next_user))
                    active_trials += 1
                    changed = True
            
            if changed:
                save_config()
        except Exception as e:
            notify_admin_error("پردازش صف تست رایگان", e)

def get_gemini_response(chat_id, user_text, file_data=None, mime_type=None):
    global user_memories, users_db, GEMINI_KEYS
    chat_id_str = str(chat_id)
    load_user_memory(chat_id)
    init_user_db(chat_id)
    
    selected_model = users_db[chat_id_str].get("selected_model", "پیش‌فرض")

    current_parts = []
    if file_data: 
        current_parts.append({"inline_data": {"mime_type": mime_type, "data": file_data}})
    
    if file_data and mime_type and mime_type.startswith("audio/"):
        if not user_text:
            user_text = "لطفاً به این فایل صوتی با دقت گوش بده. اگر در آن سوالی پرسیده شده به آن پاسخ بده و اگر دستوری داده شده آن را اجرا کن."
    elif file_data and not user_text:
        user_text = "لطفاً این فایل/تصویر را بررسی کن و در مورد آن توضیح بده."

    if user_text: 
        current_parts.append({"text": user_text})
    
    if not current_parts: 
        return "پیام خالی است."

    contents = [m for m in user_memories[chat_id_str]]
    contents.append({"role": "user", "parts": current_parts})
    
    default_models = [
        "gemini-3.7-flash", 
        "gemini-3.6-flash", 
        "gemini-3.5-flash", 
        "gemini-3.5-flash-lite", 
        "gemini-3.1-pro-preview", 
        "gemini-3-flash-preview", 
        "gemini-2.5-pro", 
        "gemini-2.5-flash"
    ]
    
    model_mapping = {
        "جمینی 3.5 فلش": "gemini-3.5-flash",
        "جمینی 3.1 پرو": "gemini-3.1-pro-preview",
        "جمینی 2.5 فلش": "gemini-2.5-flash",
        "پیش‌فرض": "default",
        "default": "default"
    }
    mapped_model = model_mapping.get(selected_model, "default")
    models = [mapped_model] if mapped_model != "default" else default_models
    
    payload = {"contents": contents}
    last_error = ""
    
    if not GEMINI_KEYS: 
        return "خطا: سیستم فاقد کلید API است."

    for m in models:
        current_keys = list(GEMINI_KEYS) 
        for api_key in current_keys:
            for attempt in range(2): 
                try:
                    url = f"https://{API_DOMAIN}/v1beta/models/{m}:generateContent?key={api_key}"
                    res = requests.post(url, json=payload, timeout=30)
                    
                    if res.status_code == 200:
                        data = res.json()
                        if "candidates" not in data or not data["candidates"]: 
                            continue

                        reply = data['candidates'][0]['content']['parts'][0]['text']
                        used_tokens = data.get('usageMetadata', {}).get('totalTokenCount', 0)
                        
                        if chat_id_str in users_db:
                            users_db[chat_id_str]["tokens"] += used_tokens
                            users_db[chat_id_str]["requests"] = users_db[chat_id_str].get("requests", 0) + 1
                            save_user_db(chat_id)
                            log_api_usage(used_tokens)

                        memory_text = user_text if not file_data else f"[یک فایل ارسال شد] {user_text if user_text else ''}"
                        user_memories[chat_id_str].append({"role": "user", "parts": [{"text": memory_text.strip()}]})
                        user_memories[chat_id_str].append({"role": "model", "parts": [{"text": reply}]})
                        
                        save_user_memory(chat_id)
                        return reply
                    
                    elif res.status_code == 429: 
                        if api_key in GEMINI_KEYS:
                            GEMINI_KEYS.remove(api_key)
                            GEMINI_KEYS.append(api_key)
                        last_error = "ارور 429 (محدودیت درخواست): کلید شما برای این مدل مسدود شده است."
                        break 
                    
                    elif res.status_code == 400:
                        return "متاسفانه فرمت فایل یا درخواست شما پشتیبانی نمی‌شود. (خطای 400 سرور هوش مصنوعی)"
                    else:
                        last_error = f"ارور {res.status_code} از گوگل: {res.text}"
                        break 
                except requests.exceptions.ConnectionError:
                    if attempt == 1: 
                        last_error = "ارور شبکه: ارتباط با سرورهای گوگل برقرار نشد."
                        break 
                    time.sleep(1)
                except requests.exceptions.ReadTimeout: 
                    last_error = "خطای تایم‌اوت: سرور گوگل در زمان مقرر پاسخ نداد."
                    break 
                except Exception as e: 
                    last_error = f"خطای نامشخص: {str(e)}"
                    break
    
    return f"❌ متاسفانه نتوانستیم پاسخی از مدل دریافت کنیم.\n\n📌 **دلیل خطا:**\n{last_error}"

def process_user_update(update):
    global sales_open
    try:
        if "pre_checkout_query" in update:
            pq = update["pre_checkout_query"]
            answer_pre_checkout_query(pq["id"], ok=True)
            return

        msg = update.get("message", {})
        chat_id = msg.get("chat", {}).get("id")
        text = msg.get("text") or msg.get("caption") or ""

        now = time.time()
        if chat_id:
            if chat_id in last_action_time and now - last_action_time[chat_id] < 1.0:
                return 
            last_action_time[chat_id] = now

        cb_data = None
        c_chat_id = None

        if "callback_query" in update:
            cq = update["callback_query"]
            c_chat_id = cq["message"]["chat"]["id"]
            cb_data = cq["data"]
        elif text:
            c_chat_id = chat_id
            if text in ["🔙 بازگشت به منوی اصلی", "🔙 بازگشت"]: 
                cb_data = "main_menu"
            elif text == "🔙 بازگشت به لیست بایگانی": 
                cb_data = "view_archives"
            elif text == "🔙 بازگشت به پنل مدیریت": 
                cb_data = "admin_panel"
            elif text == "⚙️ تنظیمات": 
                cb_data = "settings_menu"
            elif text == "💬 گفتگوی جدید": 
                cb_data = "new_chat"
            elif text == "✅ بله و شروع": 
                cb_data = "new_chat_archive"
            elif text == "❌ خیر فقط شروع": 
                cb_data = "new_chat_discard"
            elif text == "ℹ️ وضعیت اشتراک": 
                cb_data = "check_status"
            elif text == "💳 خرید اشتراک یک‌ساله": 
                cb_data = "buy_sub"
            elif text == "⏳ فعال‌سازی تست ۱ ساعته": 
                cb_data = "start_trial"
            elif text == "🆔 شناسه کاربری من": 
                cb_data = "show_my_id"
            elif text == "🗄 بایگانی گفتگو": 
                cb_data = "archive_mem"
            elif text == "📂 مشاهده بایگانی‌های گذشته": 
                cb_data = "view_archives"
            elif text == "📞 ارتباط با پشتیبانی": 
                cb_data = "contact_admin"
            elif text == "🎨 تغییر مدل / رسانه ساز": 
                cb_data = "select_model"
            elif text == "👑 پنل مدیریت": 
                cb_data = "admin_panel"
            elif text in ["🔴 بستن فروش", "🟢 باز کردن فروش"]: 
                cb_data = "toggle_sales"
            elif text in ["🔴 توقف تست رایگان", "🟢 باز کردن تست رایگان"]: 
                cb_data = "toggle_trial"
            elif text == "💰 تغییر قیمت اشتراک": 
                cb_data = "change_sub_price"
            
            elif text == "📦 پلن‌ها": 
                cb_data = "adm_plan_perms"
            elif text == "⚙️ تنظیمات دسترسی فایل (پلن‌ها)": 
                cb_data = "adm_plan_perms"
            
            elif text == "⚙️ کاربران عادی (استارت‌کننده)": 
                cb_data = "adm_plan_sel_standard"
            elif text == "⚙️ تست کنندگان یک ساعته": 
                cb_data = "adm_plan_sel_trial"
            elif text == "⚙️ خریداران": 
                cb_data = "adm_plan_sel_premium"
            elif text == "⚙️ کسانی که دسترسی ویژه دارند (VIP)": 
                cb_data = "adm_plan_sel_vip"
            elif text == "⚙️ ادمین‌ها": 
                cb_data = "adm_plan_sel_admin"
            
            elif text == f"🔑 تغییر دسترسی ({get_plan_persian_name('standard')})": 
                cb_data = "adm_plan_sub_perms_standard"
            elif text == f"🔑 تغییر دسترسی ({get_plan_persian_name('trial')})": 
                cb_data = "adm_plan_sub_perms_trial"
            elif text == f"🔑 تغییر دسترسی ({get_plan_persian_name('premium')})": 
                cb_data = "adm_plan_sub_perms_premium"
            elif text == f"🔑 تغییر دسترسی ({get_plan_persian_name('vip')})": 
                cb_data = "adm_plan_sub_perms_vip"
            elif text == f"🔑 تغییر دسترسی ({get_plan_persian_name('admin')})": 
                cb_data = "adm_plan_sub_perms_admin"
            
            elif text == f"👥 لیست دارندگان ({get_plan_persian_name('standard')})": 
                cb_data = "adm_plan_users_standard"
            elif text == f"👥 لیست دارندگان ({get_plan_persian_name('trial')})": 
                cb_data = "adm_plan_users_trial"
            elif text == f"👥 لیست دارندگان ({get_plan_persian_name('premium')})": 
                cb_data = "adm_plan_users_premium"
            elif text == f"👥 لیست دارندگان ({get_plan_persian_name('vip')})": 
                cb_data = "adm_plan_users_vip"
            elif text == f"👥 لیست دارندگان ({get_plan_persian_name('admin')})": 
                cb_data = "adm_plan_users_admin"
            
            elif text == "📢 پیام همگانی": 
                cb_data = "adm_broadcast"
            elif text == "📢 ارسال به همه کاربران": 
                cb_data = "adm_bc_target_all"
            elif text == "📢 ارسال به کاربران عادی (استارت‌کننده)": 
                cb_data = "adm_bc_target_standard"
            elif text == "📢 ارسال به تست کنندگان ۱ ساعته": 
                cb_data = "adm_bc_target_trial"
            elif text == "📢 ارسال به خریداران": 
                cb_data = "adm_bc_target_premium"
            elif text == "📢 ارسال به دارندگان دسترسی ویژه (VIP)": 
                cb_data = "adm_bc_target_vip"
            elif text == "📨 ارسال پیام تکی با آی‌دی": 
                cb_data = "adm_bc_target_single"
            
            elif text.startswith("➕ افزودن کاربر با آیدی ("):
                try:
                    p_name = text.replace("➕ افزودن کاربر با آیدی (", "").replace(")", "").strip()
                    rev_names = {"کاربران عادی (استارت‌کننده)": "standard", "تست کنندگان یک ساعته (فعال)": "trial", "خریداران": "premium", "کسانی که دسترسی ویژه دارند (VIP)": "vip", "ادمین‌ها": "admin"}
                    t_plan = rev_names.get(p_name)
                    if t_plan: 
                        cb_data = f"adm_plan_add_{t_plan}"
                except: 
                    pass
            elif text.startswith("❌ حذف کاربر با آیدی ("):
                try:
                    p_name = text.replace("❌ حذف کاربر با آیدی (", "").replace(")", "").strip()
                    rev_names = {"کاربران عادی (استارت‌کننده)": "standard", "تست کنندگان یک ساعته (فعال)": "trial", "خریداران": "premium", "کسانی که دسترسی ویژه دارند (VIP)": "vip", "ادمین‌ها": "admin"}
                    t_plan = rev_names.get(p_name)
                    if t_plan: 
                        cb_data = f"adm_plan_rem_{t_plan}"
                except: 
                    pass
            
            elif text.startswith("تغییر دسترسی "):
                try:
                    clean = text.replace("تغییر دسترسی ", "").strip()
                    parts = clean.split(":")
                    left = parts[0].strip()
                    target_plan, perm_key = left.split()
                    cb_data = f"adm_plan_toggle_{target_plan}_{perm_key}"
                except: 
                    pass
            
            elif text == "➕ افزودن گروهی به لیست سفید": 
                cb_data = "adm_bulk_white"
            elif text == "📈 آمار API (۲۴ساعت)": 
                cb_data = "adm_api_stats"
            
            elif text.startswith("تغییر اشتراک "): 
                cb_data = f"adm_tog_prem_{text.replace('تغییر اشتراک ', '').strip()}"
            elif text.startswith("تغییر وضعیت VIP "): 
                cb_data = f"adm_tog_vip_{text.replace('تغییر وضعیت VIP ', '').strip()}"
            elif text.startswith("✅ تایید کاربر "): 
                cb_data = f"adm_approve_{text.replace('✅ تایید کاربر ', '').strip()}"
            elif text.startswith("📨 پیام به "): 
                cb_data = f"adm_msg_{text.replace('📨 پیام به ', '').strip()}"
            elif text.startswith("👤 مدیریت کاربر "): 
                cb_data = f"adm_manage_usr_{text.replace('👤 مدیریت کاربر ', '').strip()}"
            elif text.startswith("مدل: "): 
                cb_data = f"set_model_{text.replace('مدل: ', '').strip()}"
            elif text == "📄 خروجی: متن / ورد": 
                cb_data = "toggle_word_output"
            elif text.startswith("▶️ ادامه بایگانی "): 
                try: 
                    cb_data = f"continue_arc_{int(text.replace('▶️ ادامه بایگانی ', '').strip()) - 1}"
                except: 
                    pass
            elif text.startswith("🗑 حذف بایگانی "): 
                try: 
                    cb_data = f"delete_arc_{int(text.replace('🗑 حذف بایگانی ', '').strip()) - 1}"
                except: 
                    pass
            elif text.startswith("فایل بایگانی "):
                try: 
                    cb_data = f"read_arc_{int(text.split(':')[0].replace('فایل بایگانی ', '').strip()) - 1}"
                except: 
                    pass

        if not c_chat_id: 
            c_chat_id = chat_id
        if not c_chat_id: 
            return

        with get_user_lock(c_chat_id):
            if cb_data and c_chat_id:
                c_str = str(c_chat_id)
                init_user_db(c_chat_id)
                load_user_memory(c_chat_id)

                if cb_data == "main_menu":
                    user_states.pop(c_chat_id, None)
                    admin_states.pop(c_chat_id, None)
                    send_msg(c_chat_id, "به منوی اصلی بازگشتید.", get_user_keyboard(c_chat_id))

                elif cb_data == "settings_menu":
                    send_msg(c_chat_id, "⚙️ به بخش تنظیمات خوش آمدید. لطفاً گزینه مورد نظر را انتخاب کنید:", get_settings_keyboard())

                elif cb_data == "new_chat":
                    current_mem = user_memories.get(c_str, [])
                    if len(current_mem) > 0:
                        kb = {"keyboard": [[{"text": "✅ بله و شروع"}, {"text": "❌ خیر فقط شروع"}], [{"text": "🔙 بازگشت به منوی اصلی"}]], "resize_keyboard": True}
                        send_msg(c_chat_id, "آیا می‌خواهید مکالمه قبلی خود را بایگانی کنید؟", json.dumps(kb))
                    else:
                        user_memories[c_str] = []
                        save_user_memory(c_chat_id)
                        send_msg(c_chat_id, "💬 یک گفتگوی جدید آغاز شد.\n\n" + get_welcome_prompt(c_chat_id), get_user_keyboard(c_chat_id))

                elif cb_data == "new_chat_archive":
                    current_mem = user_memories.get(c_str, [])
                    if current_mem:
                        load_user_archives(c_chat_id)
                        if len(user_archives.get(c_str, [])) >= 10:
                            send_msg(c_chat_id, "❌ ظرفیت بایگانی شما پر است (حداکثر ۱۰ فایل). مکالمه بدون بایگانی پاک شد.")
                        else:
                            user_archives.setdefault(c_str, []).append({"date": str(datetime.now()), "content": current_mem})
                            save_user_archives(c_chat_id)
                            send_msg(c_chat_id, "🗄 مکالمه قبلی با موفقیت بایگانی شد.")
                    
                    user_memories[c_str] = []
                    save_user_memory(c_chat_id)
                    send_msg(c_chat_id, "💬 یک گفتگوی جدید آغاز شد.\n\n" + get_welcome_prompt(c_chat_id), get_user_keyboard(c_chat_id))

                elif cb_data == "new_chat_discard":
                    user_memories[c_str] = []
                    save_user_memory(c_chat_id)
                    send_msg(c_chat_id, "🧹 حافظه چت پاک شد و گفتگوی جدید آغاز شد.\n\n" + get_welcome_prompt(c_chat_id), get_user_keyboard(c_chat_id))

                elif cb_data == "check_status":
                    has_access, err = check_access(c_chat_id)
                    p_until = users_db[c_str].get("premium_until", 0)
                    is_vip = users_db[c_str].get("is_vip", False)
                    trial_start = users_db[c_str].get("trial_start", 0)
                    
                    if p_until > time.time():
                        days_left = int((p_until - time.time()) / 86400)
                        send_msg(c_chat_id, f"✅ اشتراک ویژه شما فعال است.\n⏳ اعتبار باقیمانده: {days_left} روز\n\n📌 {get_plan_perms_text('premium')}", get_settings_keyboard())
                    elif is_vip:
                        send_msg(c_chat_id, f"🌟 شما دارای دسترسی ویژه (VIP) دائمی از سمت مدیریت هستید.\n\n📌 {get_plan_perms_text('vip')}", get_settings_keyboard())
                    elif trial_start > 0:
                        passed_time = time.time() - trial_start
                        if passed_time < 3600:
                            rem_seconds = 3600 - passed_time
                            rem_minutes = int(rem_seconds // 60)
                            send_msg(c_chat_id, f"✅ وضعیت حساب شما: تست ۱ ساعته فعال\n⏳ زمان باقیمانده: {rem_minutes} دقیقه\n\n📌 {get_plan_perms_text('trial')}", get_settings_keyboard())
                        else:
                            send_msg(c_chat_id, "❌ زمان تست ۱ ساعته شما منقضی شده است.\nبرای ادامه می‌توانید اشتراک تهیه کنید.", get_settings_keyboard())
                    else:
                        send_msg(c_chat_id, f"ℹ️ وضعیت حساب: کاربر عادی (بدون اشتراک فعال)\n\n📌 {get_plan_perms_text('standard')}", get_settings_keyboard())

                elif cb_data == "show_my_id":
                    send_msg(c_chat_id, f"👤 شناسه کاربری شما در سیستم:\n`{c_chat_id}`", get_settings_keyboard())

                elif cb_data == "contact_admin":
                    if users_db[c_str].get("premium_until", 0) < time.time() and users_db[c_str].get("admin_msg_count", 0) >= 10:
                        send_msg(c_chat_id, "❌ شما به سقف مجاز ارسال پیام به پشتیبانی رسیده‌اید.", get_settings_keyboard())
                    else:
                        user_states[c_chat_id] = {"action": "contact_admin"}
                        cancel_kb = {"keyboard": [[{"text": "لغو ارتباط"}]], "resize_keyboard": True}
                        send_msg(c_chat_id, "📝 لطفاً پیام خود را برای مدیریت بنویسید (برای لغو دکمه مربوطه را بزنید):", json.dumps(cancel_kb))

                elif cb_data == "start_trial":
                    if users_db[c_str]["trial_start"] > 0:
                        send_msg(c_chat_id, "❌ شما قبلاً از مهلت تست رایگان خود استفاده کرده‌اید.", get_user_keyboard(c_chat_id))
                    elif not bot_config["trial_enabled"]:
                        send_msg(c_chat_id, "❌ متاسفانه سیستم تست رایگان در حال حاضر توسط مدیریت بسته شده است.", get_user_keyboard(c_chat_id))
                    elif c_chat_id in bot_config["trial_queue"]:
                        pos = bot_config["trial_queue"].index(c_chat_id) + 1
                        send_msg(c_chat_id, f"⏳ شما در صف انتظار فعال‌سازی تست هستید (نفر {pos}). به محض خالی شدن ظرفیت، تست شما فعال خواهد شد.")
                    else:
                        active_trials = sum(1 for u in users_db.values() if 0 < u.get("trial_start", 0) and time.time() - u["trial_start"] < 3600)
                        if active_trials >= 10:
                            bot_config["trial_queue"].append(c_chat_id)
                            save_config()
                            pos = len(bot_config["trial_queue"])
                            send_msg(c_chat_id, f"⚠️ ظرفیت تست همزمان (۱۰ نفر) پر است.\nشما در صف انتظار قرار گرفتید (نفر {pos}). به محض پایان یافتن زمان نفرات قبلی، تست شما خودکار فعال می‌شود.", get_user_keyboard(c_chat_id))
                        else:
                            users_db[c_str]["trial_start"] = time.time()
                            save_user_db(c_chat_id)
                            prompt_text = get_welcome_prompt(c_chat_id)
                            send_msg(c_chat_id, f"🎉 تست رایگان ۱ ساعته شما با موفقیت آغاز شد!\nدسترسی‌های شما در این پلن:\n{get_plan_perms_text('trial')}\n\nاکنون {prompt_text}", get_user_keyboard(c_chat_id))

                elif cb_data == "buy_sub":
                    if not sales_open:
                        send_msg(c_chat_id, "❌ متاسفانه در حال حاضر فروش موقتاً بسته شده است.")
                    elif users_db[c_str].get("premium_until", 0) > time.time():
                        send_msg(c_chat_id, "🌟 شما در حال حاضر دارای اشتراک فعال هستید.")
                    else:
                        send_invoice(c_chat_id)
                        txt_price = format_price(bot_config['sub_price_rial'])
                        send_msg(c_chat_id, f"💳 فاکتور به مبلغ {txt_price} از طریق کیف پول بله صادر شد.")

                elif cb_data == "select_model":
                    if c_chat_id == SUPER_ADMIN_ID:
                        word_status = "روشن 🟢" if users_db[c_str].get("return_as_word") else "خاموش 🔴"
                        model_kb = {
                            "keyboard": [
                                [{"text": "مدل: پیش‌فرض"}],
                                [{"text": "مدل: جمینی 3.5 فلش"}],
                                [{"text": "مدل: جمینی 3.1 پرو"}, {"text": "مدل: جمینی 2.5 فلش"}],
                                [{"text": "📄 خروجی: متن / ورد"}],
                                [{"text": "🔙 بازگشت به منوی اصلی"}]
                            ],
                            "resize_keyboard": True
                        }
                        msg_text = f"🎨 مدل خود را انتخاب کنید:\n\nوضعیت خروجی فایل ورد در حال حاضر: {word_status}"
                        send_msg(c_chat_id, msg_text, json.dumps(model_kb))
                    else:
                        send_msg(c_chat_id, "❌ شما دسترسی لازم برای این بخش را ندارید.")
                
                elif cb_data == "toggle_word_output":
                    users_db[c_str]["return_as_word"] = not users_db[c_str].get("return_as_word", False)
                    save_user_db(c_chat_id)
                    st = "روشن" if users_db[c_str]["return_as_word"] else "خاموش"
                    send_msg(c_chat_id, f"✅ خروجی ورد برای پاسخ‌های متنی ربات {st} شد.", get_user_keyboard(c_chat_id))

                elif cb_data.startswith("set_model_"):
                    mdl = cb_data.replace("set_model_", "")
                    users_db[c_str]["selected_model"] = mdl
                    save_user_db(c_chat_id)
                    send_msg(c_chat_id, f"✅ مدل هوش مصنوعی با موفقیت به {mdl} تغییر یافت.", get_user_keyboard(c_chat_id))
                
                elif cb_data == "archive_mem":
                    load_user_archives(c_chat_id)
                    if len(user_archives.get(c_str, [])) >= 10:
                        send_msg(c_chat_id, "❌ ظرفیت بایگانی شما پر شده است.", get_settings_keyboard())
                    else:
                        current_mem = user_memories.get(c_str, [])
                        if current_mem:
                            user_archives.setdefault(c_str, []).append({"date": str(datetime.now()), "content": current_mem})
                            save_user_archives(c_chat_id)
                            user_memories[c_str] = []
                            save_user_memory(c_chat_id)
                            send_msg(c_chat_id, "🗄 گفتگو با موفقیت بایگانی شد.", get_settings_keyboard())
                        else:
                            send_msg(c_chat_id, "⚠️ حافظه در حال حاضر خالی است.", get_settings_keyboard())

                elif cb_data == "view_archives":
                    load_user_archives(c_chat_id)
                    if not user_archives.get(c_str, []):
                        send_msg(c_chat_id, "📂 لیست بایگانی‌ها خالی است.", get_settings_keyboard())
                    else:
                        send_msg(c_chat_id, "بایگانی مورد نظر را انتخاب کنید:", get_archives_keyboard(c_chat_id))

                elif cb_data.startswith("read_arc_"):
                    arc_idx = int(cb_data.split("_")[-1])
                    load_user_archives(c_chat_id)
                    try:
                        selected_archive = user_archives[c_str][arc_idx]["content"]
                        text_output = f"📜 محتوای بایگانی شماره {arc_idx + 1}:\n\n"
                        for msg_arc in selected_archive:
                            role = "🗣 شما" if msg_arc["role"] == "user" else "🤖 ربات"
                            text_output += f"{role}:\n{msg_arc['parts'][0]['text']}\n{'-'*20}\n"
                        
                        if len(text_output) > 3500:
                            file_b64 = base64.b64encode(text_output.encode('utf-8')).decode('utf-8')
                            send_document(c_chat_id, file_b64, f"Archive_{arc_idx+1}.txt", "متن بایگانی به دلیل طولانی بودن فایل شد.")
                            text_output = "متن بایگانی به صورت فایل ارسال گردید."
                        
                        arc_kb = {"keyboard": [
                            [{"text": f"▶️ ادامه بایگانی {arc_idx + 1}"}, {"text": f"🗑 حذف بایگانی {arc_idx + 1}"}],
                            [{"text": "🔙 بازگشت به لیست بایگانی"}]
                        ], "resize_keyboard": True}
                        send_msg(c_chat_id, text_output, json.dumps(arc_kb))
                    except: 
                        send_msg(c_chat_id, "❌ خطایی در خواندن بایگانی رخ داد.")
                
                elif cb_data.startswith("continue_arc_"):
                    load_user_archives(c_chat_id)
                    try:
                        arc_idx = int(cb_data.replace("continue_arc_", ""))
                        user_memories[c_str] = user_archives[c_str][arc_idx]["content"].copy()
                        save_user_memory(c_chat_id)
                        send_msg(c_chat_id, "✅ بایگانی مورد نظر بارگذاری شد.", get_user_keyboard(c_chat_id))
                    except: 
                        pass

                elif cb_data.startswith("delete_arc_"):
                    load_user_archives(c_chat_id)
                    try:
                        arc_idx = int(cb_data.replace("delete_arc_", ""))
                        del user_archives[c_str][arc_idx]
                        save_user_archives(c_chat_id)
                        send_msg(c_chat_id, "✅ بایگانی با موفقیت حذف شد.", get_archives_keyboard(c_chat_id))
                    except: 
                        pass

                elif c_chat_id == SUPER_ADMIN_ID:
                    if cb_data == "admin_panel":
                        send_msg(c_chat_id, "👑 به پنل مدیریت خوش آمدید.", get_admin_users_keyboard())
                    
                    elif cb_data == "toggle_sales":
                        sales_open = not sales_open
                        save_config()
                        state_text = "باز شد" if sales_open else "بسته شد"
                        send_msg(c_chat_id, f"✅ وضعیت فروش تغییر کرد و اکنون {state_text} است.", get_admin_users_keyboard())

                    elif cb_data == "toggle_trial":
                        bot_config["trial_enabled"] = not bot_config["trial_enabled"]
                        save_config()
                        state = "فعال" if bot_config["trial_enabled"] else "غیرفعال"
                        send_msg(c_chat_id, f"✅ وضعیت تست رایگان اکنون {state} است.", get_admin_users_keyboard())

                    elif cb_data == "change_sub_price":
                        admin_states[c_chat_id] = {"action": "change_price"}
                        send_msg(c_chat_id, f"قیمت فعلی: {format_price(bot_config['sub_price_rial'])}\n\nلطفاً قیمت جدید را به صورت عدد (ریال) وارد کنید:")

                    elif cb_data == "adm_api_stats":
                        t_tok, t_req = get_24h_api_usage()
                        send_msg(c_chat_id, f"📊 **آمار API در ۲۴ ساعت گذشته:**\n\nتعداد درخواست (ریکوئست): {t_req:,}\nتعداد کل توکن: {t_tok:,}", get_admin_users_keyboard())
                    
                    elif cb_data == "adm_plan_perms":
                        send_msg(c_chat_id, "⚙️ پلن مورد نظر را انتخاب کنید:", get_admin_plan_perms_keyboard())

                    elif cb_data.startswith("adm_plan_sel_"):
                        target_plan = cb_data.replace("adm_plan_sel_", "")
                        kb = {
                            "keyboard": [
                                [{"text": f"🔑 تغییر دسترسی ({get_plan_persian_name(target_plan)})"}],
                                [{"text": f"👥 لیست دارندگان ({get_plan_persian_name(target_plan)})"}],
                                [{"text": f"➕ افزودن کاربر با آیدی ({get_plan_persian_name(target_plan)})"}],
                                [{"text": f"❌ حذف کاربر با آیدی ({get_plan_persian_name(target_plan)})"}],
                                [{"text": "⚙️ تنظیمات دسترسی فایل (پلن‌ها)"}]
                            ],
                            "resize_keyboard": True
                        }
                        send_msg(c_chat_id, f"📂 پلن انتخاب شده: {get_plan_persian_name(target_plan)}\nلطفاً یکی از گزینه‌های زیر را انتخاب کنید:", json.dumps(kb))

                    elif cb_data.startswith("adm_plan_add_"):
                        t_plan = cb_data.replace("adm_plan_add_", "")
                        admin_states[c_chat_id] = {"action": "plan_add_user_id", "target_plan": t_plan}
                        send_msg(c_chat_id, f"👤 لطفاً آی‌دی عددی کاربر مورد نظر جهت افزودن به پنل ({get_plan_persian_name(t_plan)}) را ارسال کنید (یا کلمه لغو را بنویسید):")

                    elif cb_data.startswith("adm_plan_rem_"):
                        t_plan = cb_data.replace("adm_plan_rem_", "")
                        admin_states[c_chat_id] = {"action": "plan_rem_user_id", "target_plan": t_plan}
                        send_msg(c_chat_id, f"👤 لطفاً آی‌دی عددی کاربر مورد نظر جهت حذف از پنل ({get_plan_persian_name(t_plan)}) را ارسال کنید (یا کلمه لغو را بنویسید):")

                    elif cb_data.startswith("adm_plan_sub_perms_"):
                        target_plan = cb_data.replace("adm_plan_sub_perms_", "")
                        send_msg(c_chat_id, f"⚙️ در حال مدیریت دسترسی‌های پلن ({get_plan_persian_name(target_plan)}):", get_admin_plan_details_keyboard(target_plan))

                    elif cb_data.startswith("adm_plan_users_"):
                        target_plan = cb_data.replace("adm_plan_users_", "")
                        report = f"👥 **لیست دارندگان پلن ({get_plan_persian_name(target_plan)}):**\n\n"
                        kb = {"keyboard": [], "resize_keyboard": True}
                        count = 0
                        for uid, u_data in users_db.items():
                            if get_user_plan(uid) == target_plan:
                                count += 1
                                report += (f"👤 **کاربر {uid}:**\n"
                                f"📞 موبایل: {u_data.get('phone', 'ثبت نشده')}\n"
                                f"💬 پیام پشتیبانی: {u_data.get('admin_msg_count', 0)}\n"
                                f"🪙 توکن: {u_data.get('tokens', 0):,} | 🔄 درخواست‌ها: {u_data.get('requests', 0):,}\n"
                                f"⚠️ اخطار: {u_data.get('warnings', 0)} | بن: {u_data.get('bans', 0)}\n"
                                f"------------------------\n")
                                kb["keyboard"].append([{"text": f"👤 مدیریت کاربر {uid}"}])
                        if count == 0:
                            report += "هیچ کاربری در این پلن یافت نشد."
                        kb["keyboard"].append([{"text": "⚙️ تنظیمات دسترسی فایل (پلن‌ها)"}])
                        
                        if len(report) > 3500:
                            parts = [report[i:i+3500] for i in range(0, len(report), 3500)]
                            for part in parts[:-1]:
                                send_msg(c_chat_id, part)
                            send_msg(c_chat_id, parts[-1], json.dumps(kb))
                        else:
                            send_msg(c_chat_id, report, json.dumps(kb))

                    elif cb_data.startswith("adm_plan_toggle_"):
                        parts = cb_data.replace("adm_plan_toggle_", "").split("_")
                        target_plan = parts[0]
                        perm_key = parts[1]
                        if target_plan in bot_config["plan_perms"] and perm_key in bot_config["plan_perms"][target_plan]:
                            bot_config["plan_perms"][target_plan][perm_key] = not bot_config["plan_perms"][target_plan][perm_key]
                            save_config()
                            send_msg(c_chat_id, f"✅ دسترسی «{perm_key}» برای پلن «{get_plan_persian_name(target_plan)}» تغییر یافت.", get_admin_plan_details_keyboard(target_plan))

                    elif cb_data.startswith("adm_manage_usr_"):
                        target_id = cb_data.replace("adm_manage_usr_", "")
                        if target_id in users_db:
                            u_data = users_db[target_id]
                            prem_status = "✅ فعال" if u_data.get("premium_until", 0) > time.time() else "❌ غیرفعال"
                            vip_status = "✅ فعال" if u_data.get("is_vip", False) else "❌ غیرفعال"

                            msg_text = (f"👤 **مدیریت کاربر:** `{target_id}`\n"
                            f"📞 موبایل: {u_data.get('phone', 'ثبت نشده')}\n"
                            f"💎 اشتراک ویژه (خریدار): {prem_status}\n"
                            f"🌟 دسترسی ویژه (VIP): {vip_status}\n"
                            f"💬 پیام پشتیبانی: {u_data.get('admin_msg_count', 0)}\n"
                            f"🪙 توکن مصرفی: {u_data.get('tokens', 0)}\n"
                            f"🔄 درخواست‌ها: {u_data.get('requests', 0)}\n"
                            f"⚠️ اخطار: {u_data.get('warnings', 0)} | بن: {u_data.get('bans', 0)}")

                            kb = {
                                "keyboard": [
                                    [{"text": f"تغییر اشتراک {target_id}"}],
                                    [{"text": f"تغییر وضعیت VIP {target_id}"}],
                                    [{"text": f"✅ تایید کاربر {target_id}"}, {"text": f"📨 پیام به {target_id}"}],
                                    [{"text": "🔙 بازگشت به پنل مدیریت"}]
                                ],
                                "resize_keyboard": True
                            }
                            send_msg(c_chat_id, msg_text, json.dumps(kb))
                        else:
                            send_msg(c_chat_id, "❌ کاربری با این آیدی یافت نشد.")

                    elif cb_data.startswith("adm_tog_prem_"):
                        target_id = cb_data.replace("adm_tog_prem_", "")
                        if target_id in users_db:
                            current = users_db[target_id].get("premium_until", 0) > time.time()
                            users_db[target_id]["premium_until"] = 0 if current else (time.time() + (365*24*3600))
                            save_user_db(target_id)
                            st = "✅ فعال" if users_db[target_id]["premium_until"] > time.time() else "❌ غیرفعال"
                            send_msg(c_chat_id, f"اشتراک تغییر یافت: {st}", get_admin_users_keyboard())

                    elif cb_data.startswith("adm_tog_vip_"):
                        target_id = cb_data.replace("adm_tog_vip_", "")
                        if target_id in users_db:
                            users_db[target_id]["is_vip"] = not users_db[target_id].get("is_vip", False)
                            save_user_db(target_id)
                            st = "✅ فعال" if users_db[target_id]["is_vip"] else "❌ غیرفعال"
                            send_msg(c_chat_id, f"وضعیت VIP تغییر یافت: {st}", get_admin_users_keyboard())

                    elif cb_data.startswith("adm_approve_"):
                        target_id = cb_data.replace("adm_approve_", "")
                        init_user_db(target_id)
                        users_db[target_id]["premium_until"] = time.time() + (365*24*3600)
                        save_user_db(target_id)
                        send_msg(target_id, "🎉 تبریک! اشتراک ویژه توسط مدیریت برای شما فعال گردید.", get_user_keyboard(target_id))
                        send_msg(c_chat_id, f"✅ اشتراک برای {target_id} فعال شد.", get_admin_users_keyboard())
                    
                    elif cb_data.startswith("adm_msg_"):
                        target_id = cb_data.replace("adm_msg_", "")
                        admin_states[c_chat_id] = {"action": "send_message", "target": target_id}
                        send_msg(c_chat_id, f"📝 لطفاً پیامی که می‌خواهید برای کاربر {target_id} ارسال شود را بفرستید.")

                    elif cb_data == "adm_bulk_white":
                        admin_states[c_chat_id] = {"action": "bulk_whitelist"}
                        send_msg(c_chat_id, "📝 لیست آی‌دی‌ها را ارسال کنید. (با فاصله)")

                    elif cb_data == "adm_broadcast":
                        kb = {
                            "keyboard": [
                                [{"text": "📢 ارسال به همه کاربران"}],
                                [{"text": "📢 ارسال به کاربران عادی (استارت‌کننده)"}],
                                [{"text": "📢 ارسال به تست کنندگان ۱ ساعته"}],
                                [{"text": "📢 ارسال به خریداران"}],
                                [{"text": "📢 ارسال به دارندگان دسترسی ویژه (VIP)"}],
                                [{"text": "📨 ارسال پیام تکی با آی‌دی"}],
                                [{"text": "🔙 بازگشت به پنل مدیریت"}]
                            ],
                            "resize_keyboard": True
                        }
                        send_msg(c_chat_id, "📢 لطفا گروه هدف پیام همگانی را انتخاب کنید:", json.dumps(kb))

                    elif cb_data.startswith("adm_bc_target_"):
                        target_plan = cb_data.replace("adm_bc_target_", "")
                        if target_plan == "single":
                            admin_states[c_chat_id] = {"action": "broadcast_single_get_id"}
                            send_msg(c_chat_id, "👤 لطفاً آی‌دی عددی کاربر مورد نظر را ارسال کنید (یا کلمه لغو را بنویسید):")
                        else:
                            admin_states[c_chat_id] = {"action": "broadcast_message", "target_plan": target_plan}
                            send_msg(c_chat_id, f"📢 لطفاً پیام خود را برای ارسال به دسته «{get_plan_persian_name(target_plan) if target_plan != 'all' else 'همه کاربران'}» بنویسید (یا کلمه لغو را بنویسید):")
                
                return 

        if not c_chat_id: 
            return
        
        if "successful_payment" in msg:
            chat_id_str = str(c_chat_id)
            init_user_db(c_chat_id)
            users_db[chat_id_str]["premium_until"] = time.time() + (365*24*3600)
            save_user_db(c_chat_id)
            
            bot_config["sales_count"] += 1
            if bot_config["sales_count"] >= 5:
                sales_open = False
                bot_config["sales_count"] = 0
                send_msg(SUPER_ADMIN_ID, "⚠️ هشدار: فروش به صورت خودکار پس از ۵ خرید موفق بسته شد. برای باز کردن از پنل مدیریت اقدام کنید.")
            save_config()
            
            pending_phone_users.add(c_chat_id)
            send_msg(c_chat_id, "✅ پرداخت شما با موفقیت تایید شد!\n\n📞 لطفاً جهت تکمیل فرآیند، **شماره تماس** خود را پیام کنید:")
            return

        if c_chat_id in pending_phone_users:
            if text and not text.startswith("/"):
                pending_phone_users.remove(c_chat_id)
                init_user_db(c_chat_id)
                users_db[str(c_chat_id)]["phone"] = text
                save_user_db(c_chat_id)
                send_msg(SUPER_ADMIN_ID, f"💰 خرید موفق جدید:\nآی‌دی: {c_chat_id}\nموبایل: {text}")
                send_msg(c_chat_id, "✅ شماره تماس ثبت شد.\n🎉 اشتراک ویژه فعال گردید.", get_user_keyboard(c_chat_id))
            else: 
                send_msg(c_chat_id, "لطفاً شماره تماس را پیام کنید.")
            return
        
        if c_chat_id in user_states:
            if user_states[c_chat_id]["action"] == "contact_admin":
                if text.strip() in ["لغو", "لغو ارتباط"]:
                    del user_states[c_chat_id]
                    send_msg(c_chat_id, "عملیات لغو شد.", get_settings_keyboard())
                    return
                c_str = str(c_chat_id)
                users_db[c_str]["admin_msg_count"] = users_db[c_str].get("admin_msg_count", 0) + 1
                save_user_db(c_chat_id)
                del user_states[c_chat_id]
                admin_kb = {"keyboard": [[{"text": f"👤 مدیریت کاربر {c_chat_id}"}], [{"text": "🔙 بازگشت به منوی اصلی"}]], "resize_keyboard": True}
                send_msg(SUPER_ADMIN_ID, f"📨 پیام پشتیبانی از {c_chat_id}:\n\n{text}", json.dumps(admin_kb))
                send_msg(c_chat_id, "✅ پیام شما برای تیم پشتیبانی ارسال شد.", get_settings_keyboard())
                return

        if c_chat_id == SUPER_ADMIN_ID and c_chat_id in admin_states:
            state = admin_states[c_chat_id]
            if text.strip() == "لغو":
                del admin_states[c_chat_id]
                send_msg(c_chat_id, "عملیات لغو شد.", get_admin_users_keyboard())
                return
            
            if state["action"] == "change_price":
                if text.isdigit():
                    bot_config["sub_price_rial"] = int(text)
                    save_config()
                    send_msg(c_chat_id, f"✅ قیمت اشتراک به {format_price(bot_config['sub_price_rial'])} تغییر یافت.", get_admin_users_keyboard())
                else: 
                    send_msg(c_chat_id, "❌ لطفاً فقط عدد انگلیسی وارد کنید.")
                del admin_states[c_chat_id]
                return

            if state["action"] == "send_message":
                target = state["target"]
                send_msg(target, f"📩 پیام مدیریت:\n\n{text}")
                send_msg(c_chat_id, f"✅ پیام برای {target} ارسال شد.", get_admin_users_keyboard())
                del admin_states[c_chat_id]
                return

            if state["action"] == "broadcast_message":
                broadcast_text = text
                target_plan = state.get("target_plan", "all")
                del admin_states[c_chat_id]
                
                target_name = get_plan_persian_name(target_plan) if target_plan != "all" else "همه کاربران"
                send_msg(c_chat_id, f"⏳ ارسال همگانی به ({target_name}) آغاز شد.", get_admin_users_keyboard())
                
                def broadcast_task(msg_text, plan):
                    cnt = 0
                    for uid in list(users_db.keys()):
                        try:
                            if plan == "all" or get_user_plan(uid) == plan:
                                if bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendMessage", json={"chat_id": uid, "text": f"📢 پیام مدیریت:\n\n{msg_text}"}, timeout=5).status_code == 200: 
                                    cnt += 1
                        except: 
                            pass
                        time.sleep(0.05) 
                    send_msg(SUPER_ADMIN_ID, f"✅ پیام به {cnt} کاربر از دسته ({target_name}) رسید.")
                threading.Thread(target=broadcast_task, args=(broadcast_text, target_plan), daemon=True).start()
                return

            if state["action"] == "broadcast_single_get_id":
                if text.strip().isdigit():
                    target_id = text.strip()
                    admin_states[c_chat_id] = {"action": "broadcast_single_get_msg", "target_id": target_id}
                    send_msg(c_chat_id, f"📝 لطفاً پیام خود را برای کاربر {target_id} بنویسید (یا کلمه لغو را بنویسید):")
                else:
                    send_msg(c_chat_id, "❌ لطفاً فقط آی‌دی عددی معتبر ارسال کنید.")
                return

            if state["action"] == "broadcast_single_get_msg":
                target_id = state["target_id"]
                msg_text = text
                del admin_states[c_chat_id]
                send_msg(c_chat_id, f"⏳ در حال ارسال پیام به کاربر {target_id}...", get_admin_users_keyboard())
                try:
                    res_val = send_msg(target_id, f"📩 پیام مدیریت:\n\n{msg_text}")
                    if res_val:
                        send_msg(c_chat_id, f"✅ پیام با موفقیت به کاربر {target_id} ارسال شد.")
                    else:
                        send_msg(c_chat_id, f"❌ ارسال پیام به کاربر {target_id} ناموفق بود (احتمالاً کاربر ربات را متوقف کرده است).")
                except Exception as e:
                    send_msg(c_chat_id, f"❌ خطا در ارسال پیام: {e}")
                return

            if state["action"] == "plan_add_user_id":
                t_plan = state["target_plan"]
                del admin_states[c_chat_id]
                if text.strip().isdigit():
                    target_id = text.strip()
                    init_user_db(target_id)
                    if t_plan == "premium":
                        users_db[target_id]["premium_until"] = time.time() + (365*24*3600)
                        users_db[target_id]["is_vip"] = False
                        users_db[target_id]["trial_start"] = 0
                    elif t_plan == "vip":
                        users_db[target_id]["is_vip"] = True
                        users_db[target_id]["premium_until"] = 0
                        users_db[target_id]["trial_start"] = 0
                    elif t_plan == "trial":
                        users_db[target_id]["trial_start"] = time.time()
                        users_db[target_id]["is_vip"] = False
                        users_db[target_id]["premium_until"] = 0
                    elif t_plan == "standard":
                        users_db[target_id]["premium_until"] = 0
                        users_db[target_id]["is_vip"] = False
                        users_db[target_id]["trial_start"] = 0
                    save_user_db(target_id)
                    send_msg(c_chat_id, f"✅ کاربر {target_id} با موفقیت به پنل ({get_plan_persian_name(t_plan)}) اضافه شد.", get_admin_users_keyboard())
                    send_msg(target_id, f"🎉 پنل حساب کاربری شما توسط مدیریت به ({get_plan_persian_name(t_plan)}) تغییر یافت.", get_user_keyboard(target_id))
                else:
                    send_msg(c_chat_id, "❌ لطفاً فقط آی‌دی عددی معتبر ارسال کنید.", get_admin_users_keyboard())
                return

            if state["action"] == "plan_rem_user_id":
                t_plan = state["target_plan"]
                del admin_states[c_chat_id]
                if text.strip().isdigit():
                    target_id = text.strip()
                    if target_id in users_db:
                        if t_plan == "premium":
                            users_db[target_id]["premium_until"] = 0
                        elif t_plan == "vip":
                            users_db[target_id]["is_vip"] = False
                        elif t_plan == "trial":
                            users_db[target_id]["trial_start"] = 0
                        elif t_plan == "standard":
                            users_db[target_id]["premium_until"] = 0
                            users_db[target_id]["is_vip"] = False
                            users_db[target_id]["trial_start"] = 0
                        save_user_db(target_id)
                        send_msg(c_chat_id, f"✅ کاربر {target_id} با موفقیت از پنل ({get_plan_persian_name(t_plan)}) حذف شد.", get_admin_users_keyboard())
                        send_msg(target_id, f"⚠️ دسترسی شما به پنل ({get_plan_persian_name(t_plan)}) توسط مدیریت لغو شد.", get_user_keyboard(target_id))
                    else:
                        send_msg(c_chat_id, f"❌ کاربر {target_id} در دیتابیس یافت نشد.", get_admin_users_keyboard())
                else:
                    send_msg(c_chat_id, "❌ لطفاً فقط آی‌دی عددی معتبر ارسال کنید.", get_admin_users_keyboard())
                return

            if state["action"] == "bulk_whitelist":
                ids = [w.strip() for w in text.replace(',', ' ').split() if w.strip().isdigit()]
                for uid in ids:
                    init_user_db(uid)
                    users_db[uid]["premium_until"] = time.time() + (365*24*3600)
                    allowed_users.add(uid)
                    save_user_db(uid)
                save_whitelist()
                send_msg(c_chat_id, f"✅ {len(ids)} نفر به اشتراک اضافه شدند.", get_admin_users_keyboard())
                del admin_states[c_chat_id]
                return

        if text == "/start":
            welcome_msg = (
                "✨ **به پیشرفته‌ترین دستیار هوش مصنوعی خوش آمدید!** ✨\n\n"
                "ما در این بستر، قدرت آخرین مدل‌های شرکت گوگل را در اختیار شما قرار داده‌ایم.\n\n"
                "🎁 با استفاده از منو می‌توانید تست رایگان ۱ ساعته را شروع کنید یا اشتراک تهیه نمایید."
            )
            init_user_db(c_chat_id) 
            send_msg(c_chat_id, welcome_msg, get_user_keyboard(c_chat_id))
            return

        has_access, err_msg = check_access(c_chat_id)
        if not has_access:
            send_msg(c_chat_id, err_msg, get_user_keyboard(c_chat_id))
            return
        
        if not handle_fair_usage(c_chat_id): 
            return

        f_data, m_type = None, None
        file_keys = ["photo", "document", "video", "audio", "voice", "animation", "sticker", "file"]
        detected_file_key = next((k for k in file_keys if k in msg), None)

        perm_key = "text"
        if detected_file_key:
            if detected_file_key == "photo" or detected_file_key == "sticker":
                perm_key = "photo"
            elif detected_file_key == "voice":
                perm_key = "voice"
            elif detected_file_key == "audio":
                perm_key = "audio"
            elif detected_file_key == "video" or detected_file_key == "animation":
                perm_key = "video"
            elif detected_file_key in ["document", "file"]:
                f_name = ""
                if isinstance(msg[detected_file_key], dict):
                    f_name = msg[detected_file_key].get("file_name", "").lower()
                guessed_type = mimetypes.guess_type(f_name)[0]
                actual_mime = guessed_type or (msg[detected_file_key].get("mime_type", "") if isinstance(msg[detected_file_key], dict) else "")
                
                if is_text_document(actual_mime, f_name):
                    perm_key = "document"
                else:
                    perm_key = "other"

        plan = get_user_plan(c_chat_id)
        if plan != "admin" and not bot_config["plan_perms"].get(plan, {}).get(perm_key, False):
            persian_names = {
                "text": "ارسال پیام متنی",
                "voice": "ارسال ویس",
                "photo": "ارسال عکس و تصویر",
                "audio": "ارسال صوت و صدا",
                "video": "ارسال ویدیو و فیلم",
                "document": "ارسال فایل‌های متنی (docx...)",
                "other": "ارسال دیگر فرمت‌ها"
            }
            send_msg(c_chat_id, f"❌ دسترسی به بخش ({persian_names.get(perm_key, perm_key)}) برای پلن شما ({get_plan_persian_name(plan)}) مسدود است.")
            return

        wait_msg_id = None
        if text or detected_file_key:
            send_typing(c_chat_id)
            if detected_file_key:
                wait_msg_id = send_msg(c_chat_id, "📥 در حال دانلود فایل شما (لطفاً صبور باشید)...")
            else:
                wait_msg_id = send_msg(c_chat_id, "⏳ سیستم در حال پردازش درخواست شماست...")

        if detected_file_key:
            file_id = None
            if detected_file_key == "photo": 
                file_id = msg["photo"][-1]["file_id"]
            elif isinstance(msg[detected_file_key], dict) and "file_id" in msg[detected_file_key]:
                file_id = msg[detected_file_key]["file_id"]
            
            if file_id:
                f_data, m_type = download_bale_file(file_id)
            
            if f_data is None:
                if wait_msg_id: 
                    edit_msg(c_chat_id, wait_msg_id, "❌ فایل شما بیشتر از 20 مگابایت بود و یا در حین دانلود خطایی رخ داد.")
                return
            
            if wait_msg_id: 
                edit_msg(c_chat_id, wait_msg_id, "⏳ فایل دریافت شد، در حال پردازش توسط هوش مصنوعی...")

            if not m_type or m_type == "application/octet-stream":
                f_name = msg[detected_file_key].get("file_name", "").lower() if isinstance(msg[detected_file_key], dict) else ""
                guessed_type = mimetypes.guess_type(f_name)[0]
                if guessed_type: 
                    m_type = guessed_type
                else:
                    if perm_key == "photo": 
                        m_type = "image/jpeg"
                    elif perm_key == "video": 
                        m_type = "video/mp4"
                    elif perm_key == "audio": 
                        m_type = "audio/ogg"
                    else:
                        if f_name.endswith('.pdf'): 
                            m_type = "application/pdf"
                        else: 
                            m_type = "text/plain" 

        if text or f_data:
            reply = get_gemini_response(c_chat_id, text, f_data, m_type)
            c_str = str(c_chat_id)
            
            needs_word_output = users_db[c_str].get("return_as_word", False)
            is_long_text = len(reply) > 3500 and not reply.startswith("__FILE_GENERATED__:")
            
            warning_text = ""
            gemini_msg_count = sum(1 for m in user_memories.get(c_str, []) if m.get("role") == "model")
            if gemini_msg_count >= 30:
                user_memories[c_str] = []
                save_user_memory(c_chat_id)
                warning_text = "\n\n⚠️ حافظه به دلیل محدودیت حجم پیام‌ها پاک شد."
            
            if reply.startswith("__FILE_GENERATED__:"):
                img_data = reply.split(":")[1]
                try:
                    img_bytes = base64.b64decode(img_data)
                    bale_session.post(f"https://tapi.bale.ai/bot{BALE_TOKEN}/sendPhoto", data={"chat_id": c_chat_id}, files={"photo": ("image.jpg", img_bytes)})
                    if wait_msg_id: 
                        edit_msg(c_chat_id, wait_msg_id, "✅ تصویر ساخته شده ارسال شد.")
                except:
                    try:
                        send_document(c_chat_id, img_data, "Generated_Image.jpg", "تصویر ساخته شده")
                        if wait_msg_id: 
                            edit_msg(c_chat_id, wait_msg_id, "✅ تصویر آماده شد.")
                    except: 
                        pass
            elif needs_word_output or is_long_text:
                if wait_msg_id: 
                    edit_msg(c_chat_id, wait_msg_id, "✅ پاسخ شما در قالب فایل آماده شد.")
                
                full_text = reply + warning_text
                if needs_word_output:
                    html_content = f"<html><head><meta charset='utf-8'></head><body dir='rtl' style='font-family: Tahoma, Arial;'>{full_text.replace(chr(10), '<br>')}</body></html>"
                    file_b64 = base64.b64encode(html_content.encode('utf-8')).decode('utf-8')
                    send_document(c_chat_id, file_b64, "Gemini_Response.doc", "پاسخ متنی در قالب فایل Word")
                else:
                    file_b64 = base64.b64encode(full_text.encode('utf-8')).decode('utf-8')
                    send_document(c_chat_id, file_b64, "Gemini_Long_Response.txt", "به دلیل طولانی بودن، متن درون فایل ارسال شد.")
            else:
                final_reply = reply + warning_text
                if wait_msg_id: 
                    edit_msg(c_chat_id, wait_msg_id, final_reply)
                else: 
                    send_msg(c_chat_id, final_reply)

    except Exception as e: 
        print(f"Error for {update}: {e}")
        notify_admin_error("پردازش اصلی پیام (process_user_update)", e)

def bot_loop():
    last_id = 0
    init_db() 
    load_databases()
    print("--- Bot Loop Started ---", flush=True)
    
    while not _IS_SHUTTING_DOWN:
        try:
            res = bale_session.get(f"https://tapi.bale.ai/bot{BALE_TOKEN}/getUpdates?offset={last_id + 1}&timeout=20", timeout=30).json()
            if res.get("ok") and res.get("result"):
                for update in res["result"]:
                    last_id = update["update_id"]
                    executor.submit(process_user_update, update)
        except (SystemExit, KeyboardInterrupt):
            break
        except: 
            if _IS_SHUTTING_DOWN:
                break
            time.sleep(5)

if __name__ == "__main__":
    threading.Thread(target=run_health_check_server, daemon=True).start()
    threading.Thread(target=process_trial_queue, daemon=True).start()
    bot_loop()