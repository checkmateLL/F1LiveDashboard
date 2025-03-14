import os
from dotenv import load_dotenv

load_dotenv()

# SQLite & Cache settings
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./f1_data.db")
FASTF1_CACHE_DIR = os.getenv("FASTF1_CACHE_DIR", "./fastf1_cache")

# Redis settings (external – from RedisLabs)
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT", "13016"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_DECODE_RESPONSES = os.getenv("REDIS_DECODE_RESPONSES", "True") == "True"
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_URL_CORS = os.getenv("REDIS_URL_CORS")

# Weather service settings (using free Open-Meteo)
WEATHER_LATITUDE = os.getenv("WEATHER_LATITUDE", "45.620")
WEATHER_LONGITUDE = os.getenv("WEATHER_LONGITUDE", "9.281")
WEATHER_SERVICE_URL = os.getenv("WEATHER_SERVICE_URL", "https://api.open-meteo.com/v1/forecast")
