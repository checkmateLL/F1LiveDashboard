import threading
import time
import logging
import redis
import json
import requests

from backend.config import (
    REDIS_HOST,
    REDIS_PORT,
    REDIS_DB,
    REDIS_DECODE_RESPONSES,
    REDIS_PASSWORD,
    WEATHER_SERVICE_URL,
    WEATHER_LATITUDE,
    WEATHER_LONGITUDE
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class RedisLiveDataService:
    def __init__(self):
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=REDIS_DECODE_RESPONSES,
            ssl=True  # using 'rediss'
        )
        self._polling_thread = None
        self._stop_event = threading.Event()

    def start_polling(self):
        if self._polling_thread is None or not self._polling_thread.is_alive():
            self._stop_event.clear()
            self._polling_thread = threading.Thread(target=self._poll, daemon=True)
            self._polling_thread.start()
            logger.info("Started live data polling thread.")

    def stop_polling(self):
        self._stop_event.set()
        if self._polling_thread:
            self._polling_thread.join()
            logger.info("Stopped live data polling thread.")

    def _poll(self):
        while not self._stop_event.is_set():
            try:
                # Update live session data (dummy example)
                live_session = {
                    "session": "Race",
                    "year": 2021,
                    "timestamp": time.time()
                }
                self.redis_client.set("live_session", json.dumps(live_session))
                
                # Fetch live weather data from Open-Meteo
                response = requests.get(
                    WEATHER_SERVICE_URL,
                    params={
                        "latitude": WEATHER_LATITUDE,
                        "longitude": WEATHER_LONGITUDE,
                        "current_weather": "true"
                    },
                    timeout=5
                )
                if response.status_code == 200:
                    current_weather = response.json().get("current_weather", {})
                    self.redis_client.set("live_weather", json.dumps(current_weather))
                else:
                    logger.error(f"Failed to fetch weather data: {response.status_code}")
                
                time.sleep(1)
            except Exception as e:
                logger.error(f"Error during live data polling: {e}")
                time.sleep(5)

    def get_live_session(self):
        try:
            data = self.redis_client.get("live_session")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving live session: {e}")
        return None

    def get_live_standings(self):
        try:
            data = self.redis_client.get("live_standings")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving live standings: {e}")
        return None

    def get_live_weather(self):
        try:
            data = self.redis_client.get("live_weather")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving live weather: {e}")
        return None

    def get_live_timing(self):
        try:
            data = self.redis_client.get("live_timing")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving live timing: {e}")
        return []

    def get_live_tires(self):
        try:
            data = self.redis_client.get("live_tires")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving live tires: {e}")
        return {}

    def get_track_status(self):
        try:
            data = self.redis_client.get("track_status")
            if data:
                return json.loads(data)
        except Exception as e:
            logger.error(f"Error retrieving track status: {e}")
        return None
