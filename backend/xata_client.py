from xata.client import XataClient
import os
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(env_path)

# Load API key and database URL from environment variables
XATA_API_KEY = os.getenv("XATA_API_KEY")
XATA_DB_URL = os.getenv("DATABASE_URL")

# Ensure DATABASE_URL is correctly set
if not XATA_DB_URL or "xata.sh/db" not in XATA_DB_URL:
    raise ValueError("Invalid DATABASE_URL! Use the Xata REST API URL.")

# Initialize Xata client
xata = XataClient(api_key=XATA_API_KEY, db_url=XATA_DB_URL)

class XataF1Client:
    """
    Client for interacting with F1 data in Xata database.
    This class provides methods for storing and retrieving F1 data in a structured way.
    """
    
    def __init__(self, xata_client):
        self.client = xata_client
    
    # Events methods
    def get_events(self, year=None):
        """Get all events, optionally filtered by year"""
        query = {}
        if year:
            query["year"] = year
        return self.client.db.table("events").filter(query).getMany().records
    
    def get_event(self, year, round_number):
        """Get a specific event by year and round number"""
        return self.client.db.table("events").filter({
            "year": year,
            "round_number": round_number
        }).getFirst()
    
    def create_event(self, event_data):
        """Create a new event record"""
        return self.client.db.table("events").create(event_data)
    
    def event_exists(self, year, round_number):
        """Check if an event exists by year and round number"""
        result = self.client.db.table("events").filter({
            "year": year,
            "round_number": round_number
        }).getFirst()
        return result is not None
    
    # Sessions methods
    def get_sessions(self, event_id=None):
        """Get all sessions, optionally filtered by event"""
        query = {}
        if event_id:
            query["event_id"] = event_id
        return self.client.db.table("sessions").filter(query).getMany().records
    
    def get_session(self, event_id, session_name):
        """Get a specific session by event and name"""
        return self.client.db.table("sessions").filter({
            "event_id": event_id,
            "name": session_name
        }).getFirst()
    
    def get_session_by_id(self, session_id):
        """Get a session by ID"""
        return self.client.db.table("sessions").get(session_id)
    
    def create_session(self, session_data):
        """Create a new session record"""
        return self.client.db.table("sessions").create(session_data)
    
    def update_session(self, session_id, session_data):
        """Update a session record"""
        return self.client.db.table("sessions").update(session_id, session_data)
    
    def session_exists(self, event_id, session_name):
        """Check if a session exists"""
        result = self.client.db.table("sessions").filter({
            "event_id": event_id,
            "name": session_name
        }).getFirst()
        return result is not None
    
    # Teams methods
    def get_teams(self, year=None):
        """Get all teams, optionally filtered by year"""
        query = {}
        if year:
            query["year"] = year
        return self.client.db.table("teams").filter(query).getMany().records
    
    def get_team(self, team_name, year):
        """Get a specific team by name and year"""
        return self.client.db.table("teams").filter({
            "name": team_name,
            "year": year
        }).getFirst()
    
    def create_team(self, team_data):
        """Create a new team record"""
        return self.client.db.table("teams").create(team_data)
    
    def team_exists(self, team_name, year):
        """Check if a team exists"""
        result = self.client.db.table("teams").filter({
            "name": team_name,
            "year": year
        }).getFirst()
        return result is not None
    
    # Drivers methods
    def get_drivers(self, year=None, team_id=None):
        """Get all drivers, optionally filtered by year or team"""
        query = {}
        if year:
            query["year"] = year
        if team_id:
            query["team_id"] = team_id
        return self.client.db.table("drivers").filter(query).getMany().records
    
    def get_driver(self, abbreviation, year):
        """Get a specific driver by abbreviation and year"""
        return self.client.db.table("drivers").filter({
            "abbreviation": abbreviation,
            "year": year
        }).getFirst()
    
    def create_driver(self, driver_data):
        """Create a new driver record"""
        return self.client.db.table("drivers").create(driver_data)
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        result = self.client.db.table("drivers").filter({
            "abbreviation": abbreviation,
            "year": year
        }).getFirst()
        return result is not None
    
    # Results methods
    def get_results(self, session_id=None, driver_id=None):
        """Get results, optionally filtered by session or driver"""
        query = {}
        if session_id:
            query["session_id"] = session_id
        if driver_id:
            query["driver_id"] = driver_id
        return self.client.db.table("results").filter(query).getMany().records
    
    def create_result(self, result_data):
        """Create a new result record"""
        return self.client.db.table("results").create(result_data)
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        result = self.client.db.table("results").filter({
            "session_id": session_id,
            "driver_id": driver_id
        }).getFirst()
        return result is not None
    
    # Laps methods
    def get_laps(self, session_id=None, driver_id=None):
        """Get laps, optionally filtered by session or driver"""
        query = {}
        if session_id:
            query["session_id"] = session_id
        if driver_id:
            query["driver_id"] = driver_id
        return self.client.db.table("laps").filter(query).getMany().records
    
    def get_lap(self, session_id, driver_id, lap_number):
        """Get a specific lap"""
        return self.client.db.table("laps").filter({
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_number": lap_number
        }).getFirst()
    
    def create_lap(self, lap_data):
        """Create a new lap record"""
        return self.client.db.table("laps").create(lap_data)
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        result = self.client.db.table("laps").filter({
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_number": lap_number
        }).getFirst()
        return result is not None
    
    # Telemetry methods
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        return self.client.db.table("telemetry").create(telemetry_data)
    
    # Weather methods
    def get_weather(self, session_id):
        """Get weather data for a session"""
        return self.client.db.table("weather").filter({
            "session_id": session_id
        }).getMany().records
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        return self.client.db.table("weather").create(weather_data)
    
    def weather_exists(self, session_id, time):
        """Check if a weather record exists"""
        result = self.client.db.table("weather").filter({
            "session_id": session_id,
            "time": time
        }).getFirst()
        return result is not None
    
    # Tire compounds methods
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        return self.client.db.table("tyre_compounds").create(compound_data)
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        result = self.client.db.table("tyre_compounds").filter({
            "compound_name": compound_name,
            "year": year
        }).getFirst()
        return result is not None

# Initialize our F1 client
f1_client = XataF1Client(xata)

# Legacy function support for backward compatibility
def get_live_standings():
    return xata.db.table("standings").getMany().records

def get_live_timings():
    return xata.db.table("timings").getMany().records

def get_tire_data():
    return xata.db.table("tires").getMany().records

def get_live_weather():
    return xata.db.table("weather").getMany().records

def get_current_session():
    return xata.db.table("sessions").getMany().records

def get_telemetry(driver_name, lap_number):
    return xata.db.table("telemetry").filter({
        "driver_name": driver_name,
        "lap_number": lap_number
    }).getMany().records

def insert_telemetry(data):
    return xata.db.table("telemetry").create(data)

def get_driver_comparison(driver1, driver2, lap_number):
    return xata.db.table("driver_comparisons").filter({
        "driver1": driver1,
        "driver2": driver2,
        "lap_number": lap_number
    }).getMany().records

def insert_driver_comparison(data):
    return xata.db.table("driver_comparisons").create(data)