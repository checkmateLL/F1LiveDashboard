import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates

def migrate_results(db_client, session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    for _, result in session.results.iterrows():
        driver_id = driver_map.get(result['Abbreviation'])
        
        if not driver_id:
            logger.warning(f"Driver {result['Abbreviation']} not found in database, skipping result")
            continue
        
        result_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "position": int(result['Position']) if pd.notna(result['Position']) else None,
            "classified_position": result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            "grid_position": int(result['GridPosition']) if pd.notna(result['GridPosition']) else None,
            "q1_time": str(result['Q1']) if pd.notna(result['Q1']) else None,
            "q2_time": str(result['Q2']) if pd.notna(result['Q2']) else None,
            "q3_time": str(result['Q3']) if pd.notna(result['Q3']) else None,
            "race_time": str(result['Time']) if pd.notna(result['Time']) else None,
            "status": result['Status'] if pd.notna(result['Status']) else None,
            "points": float(result['Points']) if pd.notna(result['Points']) else None
        }
        
        # Check if result already exists
        if not db_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            db_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")
import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates

def migrate_laps(db_client, session, session_id, year):
    """Migrate lap data for a session"""
    if not hasattr(session, 'laps') or len(session.laps) == 0:
        logger.warning(f"No lap data available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    # Batch process laps to avoid too many API calls
    batch_size = 50
    lap_count = 0
    
    for _, lap in tqdm(session.laps.iterrows(), desc="Processing laps", total=len(session.laps)):
        driver_id = driver_map.get(lap['Driver'])
        
        if not driver_id:
            logger.warning(f"Driver {lap['Driver']} not found in database, skipping lap")
            continue
        
        # Skip laps without a lap number
        if pd.isna(lap['LapNumber']):
            continue
            
        lap_number = int(lap['LapNumber'])
        
        lap_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_time": str(lap['LapTime']) if pd.notna(lap['LapTime']) else None,
            "lap_number": lap_number,
            "stint": int(lap['Stint']) if pd.notna(lap['Stint']) else None,
            "pit_out_time": str(lap['PitOutTime']) if pd.notna(lap['PitOutTime']) else None,
            "pit_in_time": str(lap['PitInTime']) if pd.notna(lap['PitInTime']) else None,
            "sector1_time": str(lap['Sector1Time']) if pd.notna(lap['Sector1Time']) else None,
            "sector2_time": str(lap['Sector2Time']) if pd.notna(lap['Sector2Time']) else None,
            "sector3_time": str(lap['Sector3Time']) if pd.notna(lap['Sector3Time']) else None,
            "sector1_session_time": str(lap['Sector1SessionTime']) if pd.notna(lap['Sector1SessionTime']) else None,
            "sector2_session_time": str(lap['Sector2SessionTime']) if pd.notna(lap['Sector2SessionTime']) else None,
            "sector3_session_time": str(lap['Sector3SessionTime']) if pd.notna(lap['Sector3SessionTime']) else None,
            "speed_i1": float(lap['SpeedI1']) if pd.notna(lap['SpeedI1']) else None,
            "speed_i2": float(lap['SpeedI2']) if pd.notna(lap['SpeedI2']) else None,
            "speed_fl": float(lap['SpeedFL']) if pd.notna(lap['SpeedFL']) else None,
            "speed_st": float(lap['SpeedST']) if pd.notna(lap['SpeedST']) else None,
            "is_personal_best": bool(lap['IsPersonalBest']) if pd.notna(lap['IsPersonalBest']) else None,
            "compound": lap['Compound'] if pd.notna(lap['Compound']) else None,
            "tyre_life": float(lap['TyreLife']) if pd.notna(lap['TyreLife']) else None,
            "fresh_tyre": bool(lap['FreshTyre']) if pd.notna(lap['FreshTyre']) else None,
            "lap_start_time": str(lap['LapStartTime']) if pd.notna(lap['LapStartTime']) else None,
            "lap_start_date": lap['LapStartDate'].isoformat() if pd.notna(lap['LapStartDate']) else None,
            "track_status": lap['TrackStatus'] if pd.notna(lap['TrackStatus']) else None,
            "position": int(lap['Position']) if pd.notna(lap['Position']) else None,
            "deleted": bool(lap['Deleted']) if pd.notna(lap['Deleted']) else None,
            "deleted_reason": lap['DeletedReason'] if pd.notna(lap['DeletedReason']) else None,
            "fast_f1_generated": bool(lap['FastF1Generated']) if pd.notna(lap['FastF1Generated']) else None,
            "is_accurate": bool(lap['IsAccurate']) if pd.notna(lap['IsAccurate']) else None,
            "time": str(lap['Time']) if pd.notna(lap['Time']) else None,
            "session_time": str(lap['SessionTime']) if pd.notna(lap['SessionTime']) else None
        }
        
        # Check if lap already exists
        if not db_client.lap_exists(session_id, driver_id, lap_number):
            logger.info(f"Adding lap {lap_number} for {lap['Driver']}")
            db_client.create_lap(lap_data)
            
            # For selected interesting laps, add some telemetry data
            if lap_data["is_personal_best"] or (lap_number % 10 == 0):
                migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year)
                
            lap_count += 1
            
            # Throttle to avoid API rate limits
            if lap_count % batch_size == 0:
                logger.info(f"Processed {lap_count} laps, pausing briefly")
                time.sleep(2)
        else:
            logger.info(f"Lap already exists: {lap_number} for {lap['Driver']}")
def migrate_results(db_client, session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    for _, result in session.results.iterrows():
        driver_id = driver_map.get(result['Abbreviation'])
        
        if not driver_id:
            logger.warning(f"Driver {result['Abbreviation']} not found in database, skipping result")
            continue
        
        result_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "position": int(result['Position']) if pd.notna(result['Position']) else None,
            "classified_position": result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            "grid_position": int(result['GridPosition']) if pd.notna(result['GridPosition']) else None,
            "q1_time": str(result['Q1']) if pd.notna(result['Q1']) else None,
            "q2_time": str(result['Q2']) if pd.notna(result['Q2']) else None,
            "q3_time": str(result['Q3']) if pd.notna(result['Q3']) else None,
            "race_time": str(result['Time']) if pd.notna(result['Time']) else None,
            "status": result['Status'] if pd.notna(result['Status']) else None,
            "points": float(result['Points']) if pd.notna(result['Points']) else None
        }
        
        # Check if result already exists
        if not db_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            db_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")
import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates

def migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year):
    """Migrate telemetry data for a specific lap"""
    try:
        # Skip if lap doesn't have a lap number
        if pd.isna(lap['LapNumber']):
            return
            
        lap_number = int(lap['LapNumber'])
        
        # Get a reasonable amount of telemetry data (not all points)
        telemetry = None
        try:
            telemetry = lap.get_telemetry()
        except Exception as e:
            logger.warning(f"Failed to get telemetry for lap {lap_number}: {e}")
            return
            
        if telemetry is None or telemetry.empty:
            logger.warning(f"No telemetry available for lap {lap_number}")
            return
            
        # Sample the telemetry to avoid too much data
        sample_size = 100  # SQLite can handle more data than Xata
        if len(telemetry) > sample_size:
            telemetry = telemetry.iloc[::len(telemetry)//sample_size]
            
        for idx, tel in telemetry.iterrows():
            tel_data = {
                "driver_id": driver_id,
                "lap_number": lap_number,
                "session_id": session_id,
                "time": str(tel['Time']) if 'Time' in tel and pd.notna(tel['Time']) else None,
                "session_time": str(tel['SessionTime']) if 'SessionTime' in tel and pd.notna(tel['SessionTime']) else None,
                "date": tel['Date'].isoformat() if 'Date' in tel and pd.notna(tel['Date']) else None,
                "speed": float(tel['Speed']) if 'Speed' in tel and pd.notna(tel['Speed']) else None,
                "rpm": float(tel['RPM']) if 'RPM' in tel and pd.notna(tel['RPM']) else None,
                "gear": int(tel['nGear']) if 'nGear' in tel and pd.notna(tel['nGear']) else None,
                "throttle": float(tel['Throttle']) if 'Throttle' in tel and pd.notna(tel['Throttle']) else None,
                "brake": bool(tel['Brake']) if 'Brake' in tel and pd.notna(tel['Brake']) else None,
                "drs": int(tel['DRS']) if 'DRS' in tel and pd.notna(tel['DRS']) else None,
                "x": float(tel['X']) if 'X' in tel and pd.notna(tel['X']) else None,
                "y": float(tel['Y']) if 'Y' in tel and pd.notna(tel['Y']) else None,
                "z": float(tel['Z']) if 'Z' in tel and pd.notna(tel['Z']) else None,
                "source": tel['Source'] if 'Source' in tel and pd.notna(tel['Source']) else None,
                "year": year
            }
            
            # Insert telemetry data without checking for duplicates (we're already sampling)
            db_client.create_telemetry(tel_data)
    except Exception as e:
        logger.error(f"Failed to process telemetry: {e}")
def migrate_laps(db_client, session, session_id, year):
    """Migrate lap data for a session"""
    if not hasattr(session, 'laps') or len(session.laps) == 0:
        logger.warning(f"No lap data available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    # Batch process laps to avoid too many API calls
    batch_size = 50
    lap_count = 0
    
    for _, lap in tqdm(session.laps.iterrows(), desc="Processing laps", total=len(session.laps)):
        driver_id = driver_map.get(lap['Driver'])
        
        if not driver_id:
            logger.warning(f"Driver {lap['Driver']} not found in database, skipping lap")
            continue
        
        # Skip laps without a lap number
        if pd.isna(lap['LapNumber']):
            continue
            
        lap_number = int(lap['LapNumber'])
        
        lap_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_time": str(lap['LapTime']) if pd.notna(lap['LapTime']) else None,
            "lap_number": lap_number,
            "stint": int(lap['Stint']) if pd.notna(lap['Stint']) else None,
            "pit_out_time": str(lap['PitOutTime']) if pd.notna(lap['PitOutTime']) else None,
            "pit_in_time": str(lap['PitInTime']) if pd.notna(lap['PitInTime']) else None,
            "sector1_time": str(lap['Sector1Time']) if pd.notna(lap['Sector1Time']) else None,
            "sector2_time": str(lap['Sector2Time']) if pd.notna(lap['Sector2Time']) else None,
            "sector3_time": str(lap['Sector3Time']) if pd.notna(lap['Sector3Time']) else None,
            "sector1_session_time": str(lap['Sector1SessionTime']) if pd.notna(lap['Sector1SessionTime']) else None,
            "sector2_session_time": str(lap['Sector2SessionTime']) if pd.notna(lap['Sector2SessionTime']) else None,
            "sector3_session_time": str(lap['Sector3SessionTime']) if pd.notna(lap['Sector3SessionTime']) else None,
            "speed_i1": float(lap['SpeedI1']) if pd.notna(lap['SpeedI1']) else None,
            "speed_i2": float(lap['SpeedI2']) if pd.notna(lap['SpeedI2']) else None,
            "speed_fl": float(lap['SpeedFL']) if pd.notna(lap['SpeedFL']) else None,
            "speed_st": float(lap['SpeedST']) if pd.notna(lap['SpeedST']) else None,
            "is_personal_best": bool(lap['IsPersonalBest']) if pd.notna(lap['IsPersonalBest']) else None,
            "compound": lap['Compound'] if pd.notna(lap['Compound']) else None,
            "tyre_life": float(lap['TyreLife']) if pd.notna(lap['TyreLife']) else None,
            "fresh_tyre": bool(lap['FreshTyre']) if pd.notna(lap['FreshTyre']) else None,
            "lap_start_time": str(lap['LapStartTime']) if pd.notna(lap['LapStartTime']) else None,
            "lap_start_date": lap['LapStartDate'].isoformat() if pd.notna(lap['LapStartDate']) else None,
            "track_status": lap['TrackStatus'] if pd.notna(lap['TrackStatus']) else None,
            "position": int(lap['Position']) if pd.notna(lap['Position']) else None,
            "deleted": bool(lap['Deleted']) if pd.notna(lap['Deleted']) else None,
            "deleted_reason": lap['DeletedReason'] if pd.notna(lap['DeletedReason']) else None,
            "fast_f1_generated": bool(lap['FastF1Generated']) if pd.notna(lap['FastF1Generated']) else None,
            "is_accurate": bool(lap['IsAccurate']) if pd.notna(lap['IsAccurate']) else None,
            "time": str(lap['Time']) if pd.notna(lap['Time']) else None,
            "session_time": str(lap['SessionTime']) if pd.notna(lap['SessionTime']) else None
        }
        
        # Check if lap already exists
        if not db_client.lap_exists(session_id, driver_id, lap_number):
            logger.info(f"Adding lap {lap_number} for {lap['Driver']}")
            db_client.create_lap(lap_data)
            
            # For selected interesting laps, add some telemetry data
            if lap_data["is_personal_best"] or (lap_number % 10 == 0):
                migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year)
                
            lap_count += 1
            
            # Throttle to avoid API rate limits
            if lap_count % batch_size == 0:
                logger.info(f"Processed {lap_count} laps, pausing briefly")
                time.sleep(2)
        else:
            logger.info(f"Lap already exists: {lap_number} for {lap['Driver']}")
def migrate_results(db_client, session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    for _, result in session.results.iterrows():
        driver_id = driver_map.get(result['Abbreviation'])
        
        if not driver_id:
            logger.warning(f"Driver {result['Abbreviation']} not found in database, skipping result")
            continue
        
        result_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "position": int(result['Position']) if pd.notna(result['Position']) else None,
            "classified_position": result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            "grid_position": int(result['GridPosition']) if pd.notna(result['GridPosition']) else None,
            "q1_time": str(result['Q1']) if pd.notna(result['Q1']) else None,
            "q2_time": str(result['Q2']) if pd.notna(result['Q2']) else None,
            "q3_time": str(result['Q3']) if pd.notna(result['Q3']) else None,
            "race_time": str(result['Time']) if pd.notna(result['Time']) else None,
            "status": result['Status'] if pd.notna(result['Status']) else None,
            "points": float(result['Points']) if pd.notna(result['Points']) else None
        }
        
        # Check if result already exists
        if not db_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            db_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")
import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates

def migrate_weather(db_client, session, session_id):
    """Migrate weather data for a session"""
    if not hasattr(session, 'weather_data') or session.weather_data is None or session.weather_data.empty:
        logger.warning(f"No weather data available for session {session.name}")
        return
    
    # Sample the weather data (it's usually quite frequent and consistent)
    # For SQLite we can keep more points than for Xata
    weather_data = session.weather_data
    if len(weather_data) > 50:
        weather_data = weather_data.iloc[::len(weather_data)//50]
    
    for idx, weather in weather_data.iterrows():
        time_str = str(weather['Time']) if pd.notna(weather['Time']) else None
        
        weather_record = {
            "session_id": session_id,
            "time": time_str,
            "air_temp": float(weather['AirTemp']) if pd.notna(weather['AirTemp']) else None,
            "humidity": float(weather['Humidity']) if pd.notna(weather['Humidity']) else None,
            "pressure": float(weather['Pressure']) if pd.notna(weather['Pressure']) else None,
            "rainfall": bool(weather['Rainfall']) if pd.notna(weather['Rainfall']) else None,
            "track_temp": float(weather['TrackTemp']) if pd.notna(weather['TrackTemp']) else None,
            "wind_direction": int(weather['WindDirection']) if pd.notna(weather['WindDirection']) else None,
            "wind_speed": float(weather['WindSpeed']) if pd.notna(weather['WindSpeed']) else None
        }
        
        # Check if weather record already exists
        if not db_client.weather_exists(session_id, time_str):
            logger.info(f"Adding weather data point for {session.name}")
            db_client.create_weather(weather_record)
        else:
            logger.info(f"Weather data already exists for {session.name} at {time_str}")
def migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year):
    """Migrate telemetry data for a specific lap"""
    try:
        # Skip if lap doesn't have a lap number
        if pd.isna(lap['LapNumber']):
            return
            
        lap_number = int(lap['LapNumber'])
        
        # Get a reasonable amount of telemetry data (not all points)
        telemetry = None
        try:
            telemetry = lap.get_telemetry()
        except Exception as e:
            logger.warning(f"Failed to get telemetry for lap {lap_number}: {e}")
            return
            
        if telemetry is None or telemetry.empty:
            logger.warning(f"No telemetry available for lap {lap_number}")
            return
            
        # Sample the telemetry to avoid too much data
        sample_size = 100  # SQLite can handle more data than Xata
        if len(telemetry) > sample_size:
            telemetry = telemetry.iloc[::len(telemetry)//sample_size]
            
        for idx, tel in telemetry.iterrows():
            tel_data = {
                "driver_id": driver_id,
                "lap_number": lap_number,
                "session_id": session_id,
                "time": str(tel['Time']) if 'Time' in tel and pd.notna(tel['Time']) else None,
                "session_time": str(tel['SessionTime']) if 'SessionTime' in tel and pd.notna(tel['SessionTime']) else None,
                "date": tel['Date'].isoformat() if 'Date' in tel and pd.notna(tel['Date']) else None,
                "speed": float(tel['Speed']) if 'Speed' in tel and pd.notna(tel['Speed']) else None,
                "rpm": float(tel['RPM']) if 'RPM' in tel and pd.notna(tel['RPM']) else None,
                "gear": int(tel['nGear']) if 'nGear' in tel and pd.notna(tel['nGear']) else None,
                "throttle": float(tel['Throttle']) if 'Throttle' in tel and pd.notna(tel['Throttle']) else None,
                "brake": bool(tel['Brake']) if 'Brake' in tel and pd.notna(tel['Brake']) else None,
                "drs": int(tel['DRS']) if 'DRS' in tel and pd.notna(tel['DRS']) else None,
                "x": float(tel['X']) if 'X' in tel and pd.notna(tel['X']) else None,
                "y": float(tel['Y']) if 'Y' in tel and pd.notna(tel['Y']) else None,
                "z": float(tel['Z']) if 'Z' in tel and pd.notna(tel['Z']) else None,
                "source": tel['Source'] if 'Source' in tel and pd.notna(tel['Source']) else None,
                "year": year
            }
            
            # Insert telemetry data without checking for duplicates (we're already sampling)
            db_client.create_telemetry(tel_data)
    except Exception as e:
        logger.error(f"Failed to process telemetry: {e}")
def migrate_laps(db_client, session, session_id, year):
    """Migrate lap data for a session"""
    if not hasattr(session, 'laps') or len(session.laps) == 0:
        logger.warning(f"No lap data available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    # Batch process laps to avoid too many API calls
    batch_size = 50
    lap_count = 0
    
    for _, lap in tqdm(session.laps.iterrows(), desc="Processing laps", total=len(session.laps)):
        driver_id = driver_map.get(lap['Driver'])
        
        if not driver_id:
            logger.warning(f"Driver {lap['Driver']} not found in database, skipping lap")
            continue
        
        # Skip laps without a lap number
        if pd.isna(lap['LapNumber']):
            continue
            
        lap_number = int(lap['LapNumber'])
        
        lap_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_time": str(lap['LapTime']) if pd.notna(lap['LapTime']) else None,
            "lap_number": lap_number,
            "stint": int(lap['Stint']) if pd.notna(lap['Stint']) else None,
            "pit_out_time": str(lap['PitOutTime']) if pd.notna(lap['PitOutTime']) else None,
            "pit_in_time": str(lap['PitInTime']) if pd.notna(lap['PitInTime']) else None,
            "sector1_time": str(lap['Sector1Time']) if pd.notna(lap['Sector1Time']) else None,
            "sector2_time": str(lap['Sector2Time']) if pd.notna(lap['Sector2Time']) else None,
            "sector3_time": str(lap['Sector3Time']) if pd.notna(lap['Sector3Time']) else None,
            "sector1_session_time": str(lap['Sector1SessionTime']) if pd.notna(lap['Sector1SessionTime']) else None,
            "sector2_session_time": str(lap['Sector2SessionTime']) if pd.notna(lap['Sector2SessionTime']) else None,
            "sector3_session_time": str(lap['Sector3SessionTime']) if pd.notna(lap['Sector3SessionTime']) else None,
            "speed_i1": float(lap['SpeedI1']) if pd.notna(lap['SpeedI1']) else None,
            "speed_i2": float(lap['SpeedI2']) if pd.notna(lap['SpeedI2']) else None,
            "speed_fl": float(lap['SpeedFL']) if pd.notna(lap['SpeedFL']) else None,
            "speed_st": float(lap['SpeedST']) if pd.notna(lap['SpeedST']) else None,
            "is_personal_best": bool(lap['IsPersonalBest']) if pd.notna(lap['IsPersonalBest']) else None,
            "compound": lap['Compound'] if pd.notna(lap['Compound']) else None,
            "tyre_life": float(lap['TyreLife']) if pd.notna(lap['TyreLife']) else None,
            "fresh_tyre": bool(lap['FreshTyre']) if pd.notna(lap['FreshTyre']) else None,
            "lap_start_time": str(lap['LapStartTime']) if pd.notna(lap['LapStartTime']) else None,
            "lap_start_date": lap['LapStartDate'].isoformat() if pd.notna(lap['LapStartDate']) else None,
            "track_status": lap['TrackStatus'] if pd.notna(lap['TrackStatus']) else None,
            "position": int(lap['Position']) if pd.notna(lap['Position']) else None,
            "deleted": bool(lap['Deleted']) if pd.notna(lap['Deleted']) else None,
            "deleted_reason": lap['DeletedReason'] if pd.notna(lap['DeletedReason']) else None,
            "fast_f1_generated": bool(lap['FastF1Generated']) if pd.notna(lap['FastF1Generated']) else None,
            "is_accurate": bool(lap['IsAccurate']) if pd.notna(lap['IsAccurate']) else None,
            "time": str(lap['Time']) if pd.notna(lap['Time']) else None,
            "session_time": str(lap['SessionTime']) if pd.notna(lap['SessionTime']) else None
        }
        
        # Check if lap already exists
        if not db_client.lap_exists(session_id, driver_id, lap_number):
            logger.info(f"Adding lap {lap_number} for {lap['Driver']}")
            db_client.create_lap(lap_data)
            
            # For selected interesting laps, add some telemetry data
            if lap_data["is_personal_best"] or (lap_number % 10 == 0):
                migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year)
                
            lap_count += 1
            
            # Throttle to avoid API rate limits
            if lap_count % batch_size == 0:
                logger.info(f"Processed {lap_count} laps, pausing briefly")
                time.sleep(2)
        else:
            logger.info(f"Lap already exists: {lap_number} for {lap['Driver']}")
def migrate_results(db_client, session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    for _, result in session.results.iterrows():
        driver_id = driver_map.get(result['Abbreviation'])
        
        if not driver_id:
            logger.warning(f"Driver {result['Abbreviation']} not found in database, skipping result")
            continue
        
        result_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "position": int(result['Position']) if pd.notna(result['Position']) else None,
            "classified_position": result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            "grid_position": int(result['GridPosition']) if pd.notna(result['GridPosition']) else None,
            "q1_time": str(result['Q1']) if pd.notna(result['Q1']) else None,
            "q2_time": str(result['Q2']) if pd.notna(result['Q2']) else None,
            "q3_time": str(result['Q3']) if pd.notna(result['Q3']) else None,
            "race_time": str(result['Time']) if pd.notna(result['Time']) else None,
            "status": result['Status'] if pd.notna(result['Status']) else None,
            "points": float(result['Points']) if pd.notna(result['Points']) else None
        }
        
        # Check if result already exists
        if not db_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            db_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")
import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates

def migrate_compounds(db_client, year):
    """Migrate tyre compound information"""
    # Get a reference session to extract compound data
    schedule = fastf1.get_event_schedule(year)
    
    if len(schedule) == 0:
        logger.warning(f"No events found for {year}")
        return
        
    # Try to get compound information from any session
    compounds = list(fastf1.plotting.COMPOUND_COLORS.keys())
    
    for compound in compounds:
        compound_data = {
            "compound_name": compound,
            "color_code": fastf1.plotting.COMPOUND_COLORS.get(compound, "#FFFFFF"),
            "year": year
        }
        
        # Check if compound already exists
        if not db_client.tire_compound_exists(compound, year):
            logger.info(f"Adding compound: {compound}")
            db_client.create_tire_compound(compound_data)
        else:
            logger.info(f"Compound already exists: {compound}")

def get_database_size(db_path):
    """Get the size of the SQLite database file in MB"""
    try:
        size_bytes = os.path.getsize(db_path)
        size_mb = size_bytes / (1024 * 1024)
        return size_mb
    except Exception as e:
        logger.error(f"Error getting database size: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description='Migrate FastF1 data to SQLite database')
    parser.add_argument('year', type=int, help='Year to extract (e.g., 2023)')
    parser.add_argument('--db-path', type=str, default='./f1_data.db', help='Path to SQLite database file')
    parser.add_argument('--no-telemetry', action='store_true', help='Skip telemetry data to save time/space')
    parser.add_argument('--events-only', action='store_true', help='Only migrate events data (faster)')
    parser.add_argument('--sample-telemetry', action='store_true', help='Sample telemetry data (faster, less storage)')
    args = parser.parse_args()
    
    year = args.year
    db_path = args.db_path
    include_telemetry = not args.no_telemetry
    events_only = args.events_only
    sample_telemetry = args.sample_telemetry
    
    logger.info(f"Starting FastF1 to SQLite migration for year {year}")
    logger.info(f"Database path: {db_path}")
    
    # Initialize SQLite client
    db_client = SQLiteF1Client(db_path)
    
    try:
        # Migrate base event data
        schedule = migrate_events(db_client, year)
        migrate_sessions(db_client, schedule, year)
        
        if not events_only:
            # Migrate team and driver data
            migrate_drivers_and_teams(db_client, year)
            migrate_compounds(db_client, year)
            
            # Migrate detailed session data (can be time-consuming)
            migrate_session_details(db_client, schedule, year)
        else:
            logger.info("Skipping detailed data migration (--events-only flag set)")
        
        # Get final database size
        db_size = get_database_size(db_path)
        if db_size:
            logger.info(f"Final database size: {db_size:.2f} MB")
        
        logger.info(f"Migration for year {year} completed successfully")
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Close database connection
        db_client.close()

if __name__ == "__main__":
    main()
def migrate_weather(db_client, session, session_id):
    """Migrate weather data for a session"""
    if not hasattr(session, 'weather_data') or session.weather_data is None or session.weather_data.empty:
        logger.warning(f"No weather data available for session {session.name}")
        return
    
    # Sample the weather data (it's usually quite frequent and consistent)
    # For SQLite we can keep more points than for Xata
    weather_data = session.weather_data
    if len(weather_data) > 50:
        weather_data = weather_data.iloc[::len(weather_data)//50]
    
    for idx, weather in weather_data.iterrows():
        time_str = str(weather['Time']) if pd.notna(weather['Time']) else None
        
        weather_record = {
            "session_id": session_id,
            "time": time_str,
            "air_temp": float(weather['AirTemp']) if pd.notna(weather['AirTemp']) else None,
            "humidity": float(weather['Humidity']) if pd.notna(weather['Humidity']) else None,
            "pressure": float(weather['Pressure']) if pd.notna(weather['Pressure']) else None,
            "rainfall": bool(weather['Rainfall']) if pd.notna(weather['Rainfall']) else None,
            "track_temp": float(weather['TrackTemp']) if pd.notna(weather['TrackTemp']) else None,
            "wind_direction": int(weather['WindDirection']) if pd.notna(weather['WindDirection']) else None,
            "wind_speed": float(weather['WindSpeed']) if pd.notna(weather['WindSpeed']) else None
        }
        
        # Check if weather record already exists
        if not db_client.weather_exists(session_id, time_str):
            logger.info(f"Adding weather data point for {session.name}")
            db_client.create_weather(weather_record)
        else:
            logger.info(f"Weather data already exists for {session.name} at {time_str}")
def migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year):
    """Migrate telemetry data for a specific lap"""
    try:
        # Skip if lap doesn't have a lap number
        if pd.isna(lap['LapNumber']):
            return
            
        lap_number = int(lap['LapNumber'])
        
        # Get a reasonable amount of telemetry data (not all points)
        telemetry = None
        try:
            telemetry = lap.get_telemetry()
        except Exception as e:
            logger.warning(f"Failed to get telemetry for lap {lap_number}: {e}")
            return
            
        if telemetry is None or telemetry.empty:
            logger.warning(f"No telemetry available for lap {lap_number}")
            return
            
        # Sample the telemetry to avoid too much data
        sample_size = 100  # SQLite can handle more data than Xata
        if len(telemetry) > sample_size:
            telemetry = telemetry.iloc[::len(telemetry)//sample_size]
            
        for idx, tel in telemetry.iterrows():
            tel_data = {
                "driver_id": driver_id,
                "lap_number": lap_number,
                "session_id": session_id,
                "time": str(tel['Time']) if 'Time' in tel and pd.notna(tel['Time']) else None,
                "session_time": str(tel['SessionTime']) if 'SessionTime' in tel and pd.notna(tel['SessionTime']) else None,
                "date": tel['Date'].isoformat() if 'Date' in tel and pd.notna(tel['Date']) else None,
                "speed": float(tel['Speed']) if 'Speed' in tel and pd.notna(tel['Speed']) else None,
                "rpm": float(tel['RPM']) if 'RPM' in tel and pd.notna(tel['RPM']) else None,
                "gear": int(tel['nGear']) if 'nGear' in tel and pd.notna(tel['nGear']) else None,
                "throttle": float(tel['Throttle']) if 'Throttle' in tel and pd.notna(tel['Throttle']) else None,
                "brake": bool(tel['Brake']) if 'Brake' in tel and pd.notna(tel['Brake']) else None,
                "drs": int(tel['DRS']) if 'DRS' in tel and pd.notna(tel['DRS']) else None,
                "x": float(tel['X']) if 'X' in tel and pd.notna(tel['X']) else None,
                "y": float(tel['Y']) if 'Y' in tel and pd.notna(tel['Y']) else None,
                "z": float(tel['Z']) if 'Z' in tel and pd.notna(tel['Z']) else None,
                "source": tel['Source'] if 'Source' in tel and pd.notna(tel['Source']) else None,
                "year": year
            }
            
            # Insert telemetry data without checking for duplicates (we're already sampling)
            db_client.create_telemetry(tel_data)
    except Exception as e:
        logger.error(f"Failed to process telemetry: {e}")
def migrate_laps(db_client, session, session_id, year):
    """Migrate lap data for a session"""
    if not hasattr(session, 'laps') or len(session.laps) == 0:
        logger.warning(f"No lap data available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    # Batch process laps to avoid too many API calls
    batch_size = 50
    lap_count = 0
    
    for _, lap in tqdm(session.laps.iterrows(), desc="Processing laps", total=len(session.laps)):
        driver_id = driver_map.get(lap['Driver'])
        
        if not driver_id:
            logger.warning(f"Driver {lap['Driver']} not found in database, skipping lap")
            continue
        
        # Skip laps without a lap number
        if pd.isna(lap['LapNumber']):
            continue
            
        lap_number = int(lap['LapNumber'])
        
        lap_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_time": str(lap['LapTime']) if pd.notna(lap['LapTime']) else None,
            "lap_number": lap_number,
            "stint": int(lap['Stint']) if pd.notna(lap['Stint']) else None,
            "pit_out_time": str(lap['PitOutTime']) if pd.notna(lap['PitOutTime']) else None,
            "pit_in_time": str(lap['PitInTime']) if pd.notna(lap['PitInTime']) else None,
            "sector1_time": str(lap['Sector1Time']) if pd.notna(lap['Sector1Time']) else None,
            "sector2_time": str(lap['Sector2Time']) if pd.notna(lap['Sector2Time']) else None,
            "sector3_time": str(lap['Sector3Time']) if pd.notna(lap['Sector3Time']) else None,
            "sector1_session_time": str(lap['Sector1SessionTime']) if pd.notna(lap['Sector1SessionTime']) else None,
            "sector2_session_time": str(lap['Sector2SessionTime']) if pd.notna(lap['Sector2SessionTime']) else None,
            "sector3_session_time": str(lap['Sector3SessionTime']) if pd.notna(lap['Sector3SessionTime']) else None,
            "speed_i1": float(lap['SpeedI1']) if pd.notna(lap['SpeedI1']) else None,
            "speed_i2": float(lap['SpeedI2']) if pd.notna(lap['SpeedI2']) else None,
            "speed_fl": float(lap['SpeedFL']) if pd.notna(lap['SpeedFL']) else None,
            "speed_st": float(lap['SpeedST']) if pd.notna(lap['SpeedST']) else None,
            "is_personal_best": bool(lap['IsPersonalBest']) if pd.notna(lap['IsPersonalBest']) else None,
            "compound": lap['Compound'] if pd.notna(lap['Compound']) else None,
            "tyre_life": float(lap['TyreLife']) if pd.notna(lap['TyreLife']) else None,
            "fresh_tyre": bool(lap['FreshTyre']) if pd.notna(lap['FreshTyre']) else None,
            "lap_start_time": str(lap['LapStartTime']) if pd.notna(lap['LapStartTime']) else None,
            "lap_start_date": lap['LapStartDate'].isoformat() if pd.notna(lap['LapStartDate']) else None,
            "track_status": lap['TrackStatus'] if pd.notna(lap['TrackStatus']) else None,
            "position": int(lap['Position']) if pd.notna(lap['Position']) else None,
            "deleted": bool(lap['Deleted']) if pd.notna(lap['Deleted']) else None,
            "deleted_reason": lap['DeletedReason'] if pd.notna(lap['DeletedReason']) else None,
            "fast_f1_generated": bool(lap['FastF1Generated']) if pd.notna(lap['FastF1Generated']) else None,
            "is_accurate": bool(lap['IsAccurate']) if pd.notna(lap['IsAccurate']) else None,
            "time": str(lap['Time']) if pd.notna(lap['Time']) else None,
            "session_time": str(lap['SessionTime']) if pd.notna(lap['SessionTime']) else None
        }
        
        # Check if lap already exists
        if not db_client.lap_exists(session_id, driver_id, lap_number):
            logger.info(f"Adding lap {lap_number} for {lap['Driver']}")
            db_client.create_lap(lap_data)
            
            # For selected interesting laps, add some telemetry data
            if lap_data["is_personal_best"] or (lap_number % 10 == 0):
                migrate_telemetry_for_lap(db_client, session, lap, driver_id, session_id, year)
                
            lap_count += 1
            
            # Throttle to avoid API rate limits
            if lap_count % batch_size == 0:
                logger.info(f"Processed {lap_count} laps, pausing briefly")
                time.sleep(2)
        else:
            logger.info(f"Lap already exists: {lap_number} for {lap['Driver']}")
def migrate_results(db_client, session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = db_client.get_drivers(year)
    driver_map = {d['abbreviation']: d['id'] for d in drivers}
    
    for _, result in session.results.iterrows():
        driver_id = driver_map.get(result['Abbreviation'])
        
        if not driver_id:
            logger.warning(f"Driver {result['Abbreviation']} not found in database, skipping result")
            continue
        
        result_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "position": int(result['Position']) if pd.notna(result['Position']) else None,
            "classified_position": result['ClassifiedPosition'] if pd.notna(result['ClassifiedPosition']) else None,
            "grid_position": int(result['GridPosition']) if pd.notna(result['GridPosition']) else None,
            "q1_time": str(result['Q1']) if pd.notna(result['Q1']) else None,
            "q2_time": str(result['Q2']) if pd.notna(result['Q2']) else None,
            "q3_time": str(result['Q3']) if pd.notna(result['Q3']) else None,
            "race_time": str(result['Time']) if pd.notna(result['Time']) else None,
            "status": result['Status'] if pd.notna(result['Status']) else None,
            "points": float(result['Points']) if pd.notna(result['Points']) else None
        }
        
        # Check if result already exists
        if not db_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            db_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")
import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
import sqlite3
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create cache directory if it doesn't exist
cache_dir = "./fastf1_cache"
if not os.path.exists(cache_dir):
    logger.info(f"Creating cache directory: {cache_dir}")
    os.makedirs(cache_dir)

# Configure FastF1 cache
fastf1.Cache.enable_cache(cache_dir)

class SQLiteF1Client:
    """Client for storing F1 data in SQLite database"""
    
    def __init__(self, db_path):
        """Initialize the SQLite client"""
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()
        
    def connect(self):
        """Connect to the SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            raise
    
    def close(self):
        """Close the connection to the SQLite database"""
        if self.conn:
            self.conn.close()
            logger.info("Connection to SQLite database closed")
    
    def create_tables(self):
        """Create all the necessary tables if they don't exist"""
        try:
            # Events table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    round_number INTEGER,
                    year INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(round_number, year)
                )
            ''')
            
            # Sessions table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER,
                    name TEXT,
                    date TEXT,
                    session_type TEXT,
                    total_laps INTEGER,
                    session_start_time TEXT,
                    t0_date TEXT,
                    UNIQUE(event_id, name),
                    FOREIGN KEY(event_id) REFERENCES events(id)
                )
            ''')
            
            # Teams table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS teams (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT,
                    team_id TEXT,
                    team_color TEXT,
                    year INTEGER,
                    UNIQUE(name, year)
                )
            ''')
            
            # Drivers table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_number TEXT,
                    broadcast_name TEXT,
                    abbreviation TEXT,
                    driver_id TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    full_name TEXT,
                    headshot_url TEXT,
                    country_code TEXT,
                    team_id INTEGER,
                    year INTEGER,
                    UNIQUE(abbreviation, year),
                    FOREIGN KEY(team_id) REFERENCES teams(id)
                )
            ''')
            
            # Results table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    position INTEGER,
                    classified_position TEXT,
                    grid_position INTEGER,
                    q1_time TEXT,
                    q2_time TEXT,
                    q3_time TEXT,
                    race_time TEXT,
                    status TEXT,
                    points REAL,
                    UNIQUE(session_id, driver_id),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Laps table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS laps (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    driver_id INTEGER,
                    lap_time TEXT,
                    lap_number INTEGER,
                    stint INTEGER,
                    pit_out_time TEXT,
                    pit_in_time TEXT,
                    sector1_time TEXT,
                    sector2_time TEXT,
                    sector3_time TEXT,
                    sector1_session_time TEXT,
                    sector2_session_time TEXT,
                    sector3_session_time TEXT,
                    speed_i1 REAL,
                    speed_i2 REAL,
                    speed_fl REAL,
                    speed_st REAL,
                    is_personal_best INTEGER,
                    compound TEXT,
                    tyre_life REAL,
                    fresh_tyre INTEGER,
                    lap_start_time TEXT,
                    lap_start_date TEXT,
                    track_status TEXT,
                    position INTEGER,
                    deleted INTEGER,
                    deleted_reason TEXT,
                    fast_f1_generated INTEGER,
                    is_accurate INTEGER,
                    time TEXT,
                    session_time TEXT,
                    UNIQUE(session_id, driver_id, lap_number),
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Telemetry table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    driver_id INTEGER,
                    lap_number INTEGER,
                    session_id INTEGER,
                    time TEXT,
                    session_time TEXT,
                    date TEXT,
                    speed REAL,
                    rpm REAL,
                    gear INTEGER,
                    throttle REAL,
                    brake INTEGER,
                    drs INTEGER,
                    x REAL,
                    y REAL,
                    z REAL,
                    source TEXT,
                    year INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id),
                    FOREIGN KEY(driver_id) REFERENCES drivers(id)
                )
            ''')
            
            # Weather table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS weather (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id INTEGER,
                    time TEXT,
                    air_temp REAL,
                    humidity REAL,
                    pressure REAL,
                    rainfall INTEGER,
                    track_temp REAL,
                    wind_direction INTEGER,
                    wind_speed REAL,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                )
            ''')
            
            # Tire compounds table
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS tyre_compounds (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compound_name TEXT,
                    color_code TEXT,
                    year INTEGER,
                    UNIQUE(compound_name, year)
                )
            ''')
            
            self.conn.commit()
            logger.info("Created database tables successfully")
        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def event_exists(self, year, round_number):
        """Check if an event exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if event exists: {e}")
            raise
    
    def create_event(self, event_data):
        """Create a new event"""
        try:
            self.cursor.execute(
                """
                INSERT INTO events (
                    round_number, year, country, location, official_event_name,
                    event_name, event_date, event_format, f1_api_support
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_data["round_number"], 
                    event_data["year"], 
                    event_data["country"], 
                    event_data["location"], 
                    event_data["official_event_name"], 
                    event_data["event_name"], 
                    event_data["event_date"], 
                    event_data["event_format"], 
                    1 if event_data["f1_api_support"] else 0
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating event: {e}")
            self.conn.rollback()
            raise
    
    def get_event(self, year, round_number):
        """Get an event by year and round number"""
        try:
            self.cursor.execute(
                "SELECT * FROM events WHERE year = ? AND round_number = ?",
                (year, round_number)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            raise
    
    def session_exists(self, event_id, name):
        """Check if a session exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if session exists: {e}")
            raise
    
    def create_session(self, session_data):
        """Create a new session"""
        try:
            self.cursor.execute(
                """
                INSERT INTO sessions (
                    event_id, name, date, session_type
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    session_data["event_id"],
                    session_data["name"],
                    session_data["date"],
                    session_data["session_type"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating session: {e}")
            self.conn.rollback()
            raise
    
    def update_session(self, session_id, session_data):
        """Update a session"""
        try:
            update_fields = []
            update_values = []
            
            for key, value in session_data.items():
                if value is not None:
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)
            
            if not update_fields:
                return
                
            update_values.append(session_id)
            
            self.cursor.execute(
                f"UPDATE sessions SET {', '.join(update_fields)} WHERE id = ?",
                tuple(update_values)
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error updating session: {e}")
            self.conn.rollback()
            raise
    
    def get_session(self, event_id, name):
        """Get a session by event ID and name"""
        try:
            self.cursor.execute(
                "SELECT * FROM sessions WHERE event_id = ? AND name = ?",
                (event_id, name)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting session: {e}")
            raise
    
    def team_exists(self, name, year):
        """Check if a team exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if team exists: {e}")
            raise
    
    def create_team(self, team_data):
        """Create a new team"""
        try:
            self.cursor.execute(
                """
                INSERT INTO teams (
                    name, team_id, team_color, year
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    team_data["name"],
                    team_data["team_id"],
                    team_data["team_color"],
                    team_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating team: {e}")
            self.conn.rollback()
            raise
    
    def get_team(self, name, year):
        """Get a team by name and year"""
        try:
            self.cursor.execute(
                "SELECT * FROM teams WHERE name = ? AND year = ?",
                (name, year)
            )
            return self.cursor.fetchone()
        except sqlite3.Error as e:
            logger.error(f"Error getting team: {e}")
            raise
    
    def driver_exists(self, abbreviation, year):
        """Check if a driver exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM drivers WHERE abbreviation = ? AND year = ?",
                (abbreviation, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if driver exists: {e}")
            raise
    
    def create_driver(self, driver_data):
        """Create a new driver"""
        try:
            self.cursor.execute(
                """
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    driver_data["driver_number"],
                    driver_data["broadcast_name"],
                    driver_data["abbreviation"],
                    driver_data["driver_id"],
                    driver_data["first_name"],
                    driver_data["last_name"],
                    driver_data["full_name"],
                    driver_data["headshot_url"],
                    driver_data["country_code"],
                    driver_data["team_id"],
                    driver_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating driver: {e}")
            self.conn.rollback()
            raise
    
    def get_drivers(self, year=None):
        """Get all drivers, optionally filtered by year"""
        try:
            if year:
                self.cursor.execute("SELECT * FROM drivers WHERE year = ?", (year,))
            else:
                self.cursor.execute("SELECT * FROM drivers")
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            raise
    
    def result_exists(self, session_id, driver_id):
        """Check if a result exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM results WHERE session_id = ? AND driver_id = ?",
                (session_id, driver_id)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if result exists: {e}")
            raise
    
    def create_result(self, result_data):
        """Create a new result"""
        try:
            self.cursor.execute(
                """
                INSERT INTO results (
                    session_id, driver_id, position, classified_position,
                    grid_position, q1_time, q2_time, q3_time, race_time,
                    status, points
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result_data["session_id"],
                    result_data["driver_id"],
                    result_data["position"],
                    result_data["classified_position"],
                    result_data["grid_position"],
                    result_data["q1_time"],
                    result_data["q2_time"],
                    result_data["q3_time"],
                    result_data["race_time"],
                    result_data["status"],
                    result_data["points"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating result: {e}")
            self.conn.rollback()
            raise
    
    def lap_exists(self, session_id, driver_id, lap_number):
        """Check if a lap exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?",
                (session_id, driver_id, lap_number)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if lap exists: {e}")
            raise
    
    def create_lap(self, lap_data):
        """Create a new lap"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in lap_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO laps ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating lap: {e}")
            self.conn.rollback()
            raise
    
    def create_telemetry(self, telemetry_data):
        """Create a new telemetry record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in telemetry_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO telemetry ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating telemetry: {e}")
            self.conn.rollback()
            raise
    
    def weather_exists(self, session_id, time_str):
        """Check if a weather record exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM weather WHERE session_id = ? AND time = ?",
                (session_id, time_str)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if weather exists: {e}")
            raise
    
    def create_weather(self, weather_data):
        """Create a new weather record"""
        try:
            # Remove None values and build dynamic SQL query
            fields = []
            values = []
            
            for key, value in weather_data.items():
                if value is not None:
                    fields.append(key)
                    values.append(value)
            
            placeholders = ", ".join(["?"] * len(fields))
            fields_str = ", ".join(fields)
            
            query = f"INSERT INTO weather ({fields_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, tuple(values))
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating weather: {e}")
            self.conn.rollback()
            raise
    
    def tire_compound_exists(self, compound_name, year):
        """Check if a tire compound exists"""
        try:
            self.cursor.execute(
                "SELECT 1 FROM tyre_compounds WHERE compound_name = ? AND year = ?",
                (compound_name, year)
            )
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            logger.error(f"Error checking if tire compound exists: {e}")
            raise
    
    def create_tire_compound(self, compound_data):
        """Create a new tire compound record"""
        try:
            self.cursor.execute(
                """
                INSERT INTO tyre_compounds (
                    compound_name, color_code, year
                ) VALUES (?, ?, ?)
                """,
                (
                    compound_data["compound_name"],
                    compound_data["color_code"],
                    compound_data["year"]
                )
            )
            self.conn.commit()
            return self.cursor.lastrowid
        except sqlite3.Error as e:
            logger.error(f"Error creating tire compound: {e}")
            self.conn.rollback()
            raise

def migrate_events(db_client, year):
    """Migrate events data for a specific year to SQLite"""
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    
    for idx, event in schedule.iterrows():
        event_data = {
            "round_number": int(event['RoundNumber']),
            "year": year,
            "country": event['Country'],
            "location": event['Location'],
            "official_event_name": event['OfficialEventName'],
            "event_name": event['EventName'],
            "event_date": event['EventDate'].isoformat() if pd.notna(event['EventDate']) else None,
            "event_format": event['EventFormat'],
            "f1_api_support": bool(event['F1ApiSupport'])
        }
        
        # Check if event already exists
        if not db_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            db_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(db_client, schedule, year):
    """Migrate sessions data for events in a year to SQLite"""
    for idx, event in schedule.iterrows():
        # Get event from SQLite
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record['id']
        
        # Process each session
        for i in range(1, 6):  # Sessions 1-5
            session_name = event[f'Session{i}']
            if pd.isna(session_name):
                continue
                
            session_date = event[f'Session{i}Date']
            session_date_utc = event[f'Session{i}DateUtc']
            
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": _determine_session_type(session_name)
            }
            
            # Check if session already exists
            if not db_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                db_client.create_session(session_data)
            else:
                logger.info(f"Session already exists: {session_name} for {event['EventName']}")

def _determine_session_type(session_name):
    """Helper to determine the type of session"""
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        if "Shootout" in session_name:
            return "sprint_shootout"
        elif "Qualifying" in session_name:
            return "sprint_qualifying"
        else:
            return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_drivers_and_teams(db_client, year):
    """Migrate drivers and teams data for a specific year to SQLite"""
    # Get a reference session to extract driver and team data
    schedule = fastf1.get_event_schedule(year)
    
    # Try to find a race session with full data
    session = None
    for idx, event in schedule.iterrows():
        try:
            session = fastf1.get_session(year, event['RoundNumber'], 'R')
            session.load(laps=False, telemetry=False, weather=False)
            if hasattr(session, 'results') and len(session.results) > 0:
                break
        except Exception as e:
            logger.warning(f"Could not load results for {event['EventName']}: {e}")
    
    # If no race data, try qualifying
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        for idx, event in schedule.iterrows():
            try:
                session = fastf1.get_session(year, event['RoundNumber'], 'Q')
                session.load(laps=False, telemetry=False, weather=False)
                if hasattr(session, 'results') and len(session.results) > 0:
                    break
            except Exception as e:
                logger.warning(f"Could not load results for {event['EventName']} qualifying: {e}")
    
    if not session or not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No valid session with results found for {year}")
        return
    
    # Process teams
    teams_processed = set()
    for _, driver_data in session.results.iterrows():
        team_name = driver_data['TeamName']
        
        if team_name not in teams_processed:
            team_data = {
                "name": team_name,
                "team_id": driver_data['TeamId'],
                "team_color": driver_data['TeamColor'],
                "year": year
            }
            
            # Check if team already exists
            if not db_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                db_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = db_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record['id']
        
        driver_info = {
            "driver_number": str(driver_data['DriverNumber']),
            "broadcast_name": driver_data['BroadcastName'],
            "abbreviation": driver_data['Abbreviation'],
            "driver_id": driver_data['DriverId'],
            "first_name": driver_data['FirstName'],
            "last_name": driver_data['LastName'],
            "full_name": driver_data['FullName'],
            "headshot_url": driver_data['HeadshotUrl'],
            "country_code": driver_data['CountryCode'],
            "team_id": team_id,
            "year": year
        }
        
        # Check if driver already exists
        if not db_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            db_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(db_client, schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = db_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from SQLite
                session_record = db_client.get_session(event_record['id'], session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record['id']
                
                # Load session data
                logger.info(f"Loading data for {session.name} at {event['EventName']}")
                try:
                    session.load()
                except Exception as e:
                    logger.error(f"Failed to load session: {e}")
                    continue
                
                # Update session with additional details
                session_updates = {
                    "total_laps": session.total_laps if hasattr(session, 'total_laps') else None,
                    "session_start_time": str(session.session_start_time) if hasattr(session, 'session_start_time') else None,
                    "t0_date": session.t0_date.isoformat() if hasattr(session, 't0_date') and session.t0_date is not None else None
                }
                
                # Filter out None values
                session_updates = {k: v for k, v in session_updates.items() if v is not None}
                
                if session_updates:
                    db_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(db_client, session, session_id, year)
                
                # Process laps data
                migrate_laps(db_client, session, session_id, year)
                
                # Process weather data
                migrate_weather(db_client, session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

                session_updates