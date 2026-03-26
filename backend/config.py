"""全局配置"""

import os

# AdsPower
ADS_API = os.getenv("ADS_API", "http://127.0.0.1:50325")
ADS_API_KEY = os.getenv("ADS_API_KEY", "")

# TabMail
TABMAIL_URL = os.getenv("TABMAIL_URL", "http://192.229.101.130:3000")
TABMAIL_ADMIN_KEY = os.getenv("TABMAIL_ADMIN_KEY", "10d56e3b8d50be8078b3345e920837acd542429f53cdb0dd23b0acc4620ba486")
TABMAIL_TENANT_ID = os.getenv("TABMAIL_TENANT_ID", "00000000-0000-0000-0000-000000000001")
TABMAIL_ZONE_ID = os.getenv("TABMAIL_ZONE_ID", "3610c708-212c-47f1-a65d-97db7cf465a9")

# 数据文件
_PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
_DATA_DIR = os.path.join(_PROJECT_ROOT, "data")
os.makedirs(_DATA_DIR, exist_ok=True)
ACCOUNTS_FILE = os.path.join(_PROJECT_ROOT, "accounts.jsonl")
DB_PATH = os.path.join(_DATA_DIR, "data.db")
