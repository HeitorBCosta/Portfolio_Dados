from zoneinfo import ZoneInfo
import os

class Settings:
    TIMEZONE = ZoneInfo("America/Sao_Paulo")

    SQL_CONN_STR = os.getenv("SQL_CONN_STR")
    API_URL1 = os.getenv("URL_1")
    API_KEY1 = os.getenv("API_KEY_1")

    API_URL2 = os.getenv("URL_2")
    API_KEY2 = os.getenv("API_KEY_2")
