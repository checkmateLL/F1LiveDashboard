import redis
import json
import logging
import os
import time
import threading
import fastf1
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configure FastF1 cache
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
fastf1.Cache.enable_cache(cache_dir)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# Key prefixes for Redis
KEY_PREFIX = "f1_live:"
STANDINGS_KEY = f"{KEY_PREFIX}standings"
TIMING_KEY = f"{KEY_PREFIX}timing"
WEATHER_KEY = f"{KEY_PREFIX}weather"
TIRES_KEY = f"{KEY_PREFIX}tires"
SESSION_KEY = f"{KEY_PREFIX}session"
STATUS_KEY = f"{KEY_PREFIX}status"
META_KEY = f"{KEY_PREFIX}meta"
LAST_UPDATE_KEY = f"{KEY_PREFIX}last_update"

# Data Expiration (in seconds)
DATA_TTL = 3600  # 1 hour

class RedisLiveDataService:
    """Service for managing live F1 data in Redis"""
    
    def __init__(self):
        """Initialize the Redis live data service"""
        self.redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=REDIS_DB,
            decode_responses=True
        )
        
        self.polling_thread = None
        self.polling_active = False
        self.current_session = None
        self.polling_interval = 1  # seconds between polling
        
        # Try to connect to Redis
        try:
            self.redis_client.ping()
            logger.info("Connected to Redis successfully")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def store_data(self, key, data, expire=True):
        """Store data in Redis as JSON"""
        try:
            json_data = json.dumps(data)
            self.redis_client.set(key, json_data)
            
            # Set expiration time
            if expire:
                self.redis_client.expire(key, DATA_TTL)
            
            # Update last update timestamp
            self.redis_client.set(LAST_UPDATE_KEY, datetime.now().isoformat())
            
            return True
        except Exception as e:
            logger.error(f"Error storing data in Redis: {e}")
            return False
    
    def get_data(self, key):
        """Get data from Redis as Python object"""
        try:
            data = self.redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error retrieving data from Redis: {e}")
            return None
    
    def detect_current_session(self):
        """Detect the current F1 session (if any)"""
        try:
            # Get current date/time in UTC
            now = datetime.utcnow()
            
            # Check if we're in a race weekend
            current_schedule = fastf1.get_events_remaining(now)
            
            if len(current_schedule) == 0:
                logger.info("No upcoming events found")
                return None
            
            # Get the nearest event
            nearest_event = current_schedule.iloc[0]
            
            # Check if we're within the event dates
            event_date = nearest_event['EventDate']
            
            # Check each session
            for i in range(1, 6):  # Sessions 1-5
                session_name = nearest_event[f'Session{i}']
                session_date = nearest_event[f'Session{i}DateUtc']
                
                if pd.isna(session_date):
                    continue
                
                # Check if session is happening now (with 4-hour buffer)
                session_end = session_date + timedelta(hours=4)
                
                if now >= session_date and now <= session_end:
                    # We're in a live session
                    event = fastf1.get_event(nearest_event['EventDate'].year, nearest_event['RoundNumber'])
                    session = event.get_session(session_name)
                    
                    return {
                        "year": nearest_event['EventDate'].year,
                        "round": int(nearest_event['RoundNumber']),
                        "event_name": nearest_event['EventName'],
                        "session_name": session_name,
                        "session_type": session.name,
                        "start_time": session_date.isoformat(),
                        "end_time": session_end.isoformat(),
                        "is_live": True
                    }
            
            # Check if event is within next 2 days
            if event_date - now <= timedelta(days=2):
                return {
                    "year": nearest_event['EventDate'].year,
                    "round": int(nearest_event['RoundNumber']),
                    "event_name": nearest_event['EventName'],
                    "session_name": "Next: " + nearest_event['Session1'],
                    "session_type": nearest_event['Session1'],
                    "start_time": nearest_event['Session1DateUtc'].isoformat() if not pd.isna(nearest_event['Session1DateUtc']) else None,
                    "is_live": False
                }
            
            return None
        except Exception as e:
            logger.error(f"Error detecting current session: {e}")
            return None
    
    def poll_live_data(self):
        """Poll live data from FastF1 API"""
        try:
            if not self.current_session or not self.current_session.get("is_live", False):
                # Try to detect current session
                self.current_session = self.detect_current_session()
                
                if self.current_session and self.current_session.get("is_live", False):
                    logger.info(f"Detected live session: {self.current_session['session_name']} at {self.current_session['event_name']}")
                    
                    # Store session metadata
                    self.store_data(SESSION_KEY, self.current_session, expire=False)
                else:
                    # No live session, store current metadata (or None)
                    self.store_data(SESSION_KEY, self.current_session, expire=False)
                    logger.info("No live session currently active")
                    
                    # Re-check after 5 minutes
                    time.sleep(300)
                    return
            
            # We have a live session, load it
            year = self.current_session["year"]
            event_round = self.current_session["round"]
            session_name = self.current_session["session_type"]
            
            # Load session from FastF1
            session = fastf1.get_session(year, event_round, session_name)
            
            try:
                # Try to load data with a timeout
                session.load(laps=True, telemetry=False, weather=True)
                
                # Check if session has started
                if not hasattr(session, 'laps') or len(session.laps) == 0:
                    logger.info(f"Session {session_name} has not started yet or has no data")
                    return
                
                # Process and store data
                self._process_live_session(session)
                
            except Exception as e:
                logger.error(f"Error loading session data: {e}")
        except Exception as e:
            logger.error(f"Error in poll_live_data: {e}")
    
    def _process_live_session(self, session):
        """Process and store data from a live session"""
        try:
            # Process standings
            standings = []
            
            for _, result in session.results.iterrows():
                driver_data = {
                    "position": int(result['Position']) if pd.notna(result['Position']) else None,
                    "driver_number": str(result['DriverNumber']),
                    "driver_abbr": result['Abbreviation'],
                    "driver_name": result['FullName'],
                    "team": result['TeamName'],
                    "team_color": result['TeamColor'],
                    "q1": str(result['Q1']) if pd.notna(result['Q1']) else None,
                    "q2": str(result['Q2']) if pd.notna(result['Q2']) else None,
                    "q3": str(result['Q3']) if pd.notna(result['Q3']) else None,
                    "time": str(result['Time']) if pd.notna(result['Time']) else None,
                    "status": result['Status'] if pd.notna(result['Status']) else None,
                    "points": float(result['Points']) if pd.notna(result['Points']) else None,
                }
                standings.append(driver_data)
            
            # Store standings
            self.store_data(STANDINGS_KEY, standings)
            
            # Process timing data (last lap for each driver)
            timing = []
            
            for driver_abbr in session.results['Abbreviation']:
                driver_laps = session.laps.pick_driver(driver_abbr).sort_values(by='LapNumber', ascending=False)
                
                if len(driver_laps) > 0:
                    last_lap = driver_laps.iloc[0]
                    
                    lap_data = {
                        "driver_abbr": driver_abbr,
                        "lap_number": int(last_lap['LapNumber']) if pd.notna(last_lap['LapNumber']) else None,
                        "lap_time": str(last_lap['LapTime']) if pd.notna(last_lap['LapTime']) else None,
                        "sector1": str(last_lap['Sector1Time']) if pd.notna(last_lap['Sector1Time']) else None,
                        "sector2": str(last_lap['Sector2Time']) if pd.notna(last_lap['Sector2Time']) else None,
                        "sector3": str(last_lap['Sector3Time']) if pd.notna(last_lap['Sector3Time']) else None,
                        "is_personal_best": bool(last_lap['IsPersonalBest']) if pd.notna(last_lap['IsPersonalBest']) else False,
                        "compound": last_lap['Compound'] if pd.notna(last_lap['Compound']) else None,
                        "tyre_life": float(last_lap['TyreLife']) if pd.notna(last_lap['TyreLife']) else None,
                        "track_status": last_lap['TrackStatus'] if pd.notna(last_lap['TrackStatus']) else None,
                    }
                    timing.append(lap_data)
            
            # Store timing data
            self.store_data(TIMING_KEY, timing)
            
            # Process tire data
            tires = {}
            
            for driver_abbr in session.results['Abbreviation']:
                driver_laps = session.laps.pick_driver(driver_abbr)
                if len(driver_laps) > 0:
                    current_stint = driver_laps['Stint'].max() if pd.notna(driver_laps['Stint'].max()) else 0
                    current_tires = driver_laps[driver_laps['Stint'] == current_stint]
                    
                    if len(current_tires) > 0:
                        tire_data = {
                            "compound": current_tires.iloc[0]['Compound'] if pd.notna(current_tires.iloc[0]['Compound']) else None,
                            "life": float(current_tires['TyreLife'].max()) if pd.notna(current_tires['TyreLife'].max()) else None,
                            "age": len(current_tires),
                            "fresh": bool(current_tires.iloc[0]['FreshTyre']) if pd.notna(current_tires.iloc[0]['FreshTyre']) else None,
                        }
                        tires[driver_abbr] = tire_data
            
            # Store tire data
            self.store_data(TIRES_KEY, tires)
            
            # Process weather data (most recent only)
            if hasattr(session, 'weather_data') and session.weather_data is not None and len(session.weather_data) > 0:
                latest_weather = session.weather_data.iloc[-1]
                
                weather_data = {
                    "air_temp": float(latest_weather['AirTemp']) if pd.notna(latest_weather['AirTemp']) else None,
                    "track_temp": float(latest_weather['TrackTemp']) if pd.notna(latest_weather['TrackTemp']) else None,
                    "humidity": float(latest_weather['Humidity']) if pd.notna(latest_weather['Humidity']) else None,
                    "pressure": float(latest_weather['Pressure']) if pd.notna(latest_weather['Pressure']) else None,
                    "wind_speed": float(latest_weather['WindSpeed']) if pd.notna(latest_weather['WindSpeed']) else None,
                    "wind_direction": int(latest_weather['WindDirection']) if pd.notna(latest_weather['WindDirection']) else None,
                    "rainfall": bool(latest_weather['Rainfall']) if pd.notna(latest_weather['Rainfall']) else False,
                    "timestamp": latest_weather.name.isoformat() if pd.notna(latest_weather.name) else None,
                }
                
                # Store weather data
                self.store_data(WEATHER_KEY, weather_data)
            
            # Track status
            if hasattr(session, 'track_status') and session.track_status is not None and len(session.track_status) > 0:
                latest_status = session.track_status.iloc[-1]
                
                status_data = {
                    "status": latest_status['Status'],
                    "status_message": self._translate_track_status(latest_status['Status']),
                    "timestamp": latest_status.name.isoformat() if pd.notna(latest_status.name) else None,
                }
                
                # Store status data
                self.store_data(STATUS_KEY, status_data)
            
            logger.info(f"Successfully processed and stored data for {session.name}")
        except Exception as e:
            logger.error(f"Error processing live session data: {e}")
    
    def _translate_track_status(self, status):
        """Translate track status code to human-readable message"""
        status_map = {
            '1': 'Track Clear',
            '2': 'Yellow Flag',
            '3': 'Safety Car Deployed',
            '4': 'Red Flag/Session Stopped',
            '5': 'Virtual Safety Car Deployed',
            '6': 'Virtual Safety Car Ending'
        }
        
        return status_map.get(status, f"Unknown Status: {status}")
    
    def start_polling(self):
        """Start polling for live data in a background thread"""
        if self.polling_thread and self.polling_thread.is_alive():
            logger.info("Polling is already active")
            return
        
        self.polling_active = True
        self.polling_thread = threading.Thread(target=self._polling_loop)
        self.polling_thread.daemon = True
        self.polling_thread.start()
        
        logger.info("Started live data polling")
    
    def stop_polling(self):
        """Stop polling for live data"""
        self.polling_active = False
        
        if self.polling_thread:
            self.polling_thread.join(timeout=10)
            
        logger.info("Stopped live data polling")
    
    def _polling_loop(self):
        """Background thread function for continuous polling"""
        while self.polling_active:
            try:
                self.poll_live_data()
                time.sleep(self.polling_interval)
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
                time.sleep(5)  # Wait a bit longer if there was an error
    
    def get_last_update(self):
        """Get the timestamp of the last data update"""
        try:
            last_update = self.redis_client.get(LAST_UPDATE_KEY)
            if last_update:
                return last_update
            return None
        except Exception as e:
            logger.error(f"Error getting last update timestamp: {e}")
            return None
    
    def get_live_session(self):
        """Get information about the current live session"""
        return self.get_data(SESSION_KEY)
    
    def get_live_standings(self):
        """Get current standings from Redis"""
        return self.get_data(STANDINGS_KEY)
    
    def get_live_timing(self):
        """Get current timing data from Redis"""
        return self.get_data(TIMING_KEY)
    
    def get_live_tires(self):
        """Get current tire data from Redis"""
        return self.get_data(TIRES_KEY)
    
    def get_live_weather(self):
        """Get current weather data from Redis"""
        return self.get_data(WEATHER_KEY)
    
    def get_track_status(self):
        """Get current track status from Redis"""
        return self.get_data(STATUS_KEY)
    
    def clear_all_data(self):
        """Clear all live data from Redis"""
        try:
            keys = self.redis_client.keys(f"{KEY_PREFIX}*")
            if keys:
                self.redis_client.delete(*keys)
            logger.info("Cleared all live data from Redis")
            return True
        except Exception as e:
            logger.error(f"Error clearing data from Redis: {e}")
            return False

# Example usage
if __name__ == "__main__":
    service = RedisLiveDataService()
    
    # Check for current session
    current_session = service.detect_current_session()
    if current_session:
        if current_session.get("is_live", False):
            print(f"Live session detected: {current_session['session_name']} at {current_session['event_name']}")
            service.start_polling()
            
            # Run for a while then stop
            try:
                time.sleep(60)  # Run for 1 minute
            except KeyboardInterrupt:
                print("Stopped by user")
            finally:
                service.stop_polling()
        else:
            print(f"No live session now. Next session: {current_session['session_name']} at {current_session['event_name']}")
    else:
        print("No upcoming F1 sessions detected")