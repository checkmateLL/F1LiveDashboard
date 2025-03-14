import os
import sqlite3
import logging
import time
import argparse

import fastf1
import pandas as pd
from tqdm import tqdm

from config import FASTF1_CACHE_DIR, SQLITE_DB_PATH
from data_service import F1DataService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Ensure the FastF1 cache directory exists.
if not os.path.exists(FASTF1_CACHE_DIR):
    os.makedirs(FASTF1_CACHE_DIR)
    logger.info(f"Created cache directory: {FASTF1_CACHE_DIR}")

# Enable the FastF1 cache.
fastf1.Cache.enable_cache(FASTF1_CACHE_DIR)

#############################
# SQLite Setup and Helpers
#############################

class SQLiteF1Client:
    def __init__(self, db_path=SQLITE_DB_PATH):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_tables()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to SQLite database: {self.db_path}")
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite: {e}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Closed SQLite connection")

    def commit(self):
        if self.conn:
            self.conn.commit()

    def create_tables(self):
        """Creates the necessary tables if they don't exist yet."""
        try:
            # Events
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    year INTEGER,
                    round_number INTEGER,
                    country TEXT,
                    location TEXT,
                    official_event_name TEXT,
                    event_name TEXT,
                    event_date TEXT,
                    event_format TEXT,
                    f1_api_support INTEGER,
                    UNIQUE(year, round_number)
                )
            ''')

            # Sessions
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

            # Teams
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

            # Drivers
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

            # Results
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

            # Laps
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

            # Telemetry
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

            # Weather
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

            self.commit()
            logger.info("Created/verified all tables successfully.")

        except sqlite3.Error as e:
            logger.error(f"Error creating tables: {e}")
            raise

    ###########################
    # Utility / Insert Methods
    ###########################
    # Insert methods go here. For brevity, we'll do a few examples:

    def insert_event(self, event_data: dict) -> int:
        """
        Insert an event if it doesn't exist. Return event_id (existing or new).
        """
        self.cursor.execute("""
            SELECT id FROM events
            WHERE year = ? AND round_number = ?
        """, (event_data['year'], event_data['round_number']))
        row = self.cursor.fetchone()
        if row:
            return row['id']

        self.cursor.execute("""
            INSERT INTO events (
                year, round_number, country, location, official_event_name,
                event_name, event_date, event_format, f1_api_support
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_data["year"],
            event_data["round_number"],
            event_data["country"],
            event_data["location"],
            event_data["official_event_name"],
            event_data["event_name"],
            event_data["event_date"],
            event_data["event_format"],
            1 if event_data["f1_api_support"] else 0
        ))
        self.commit()
        return self.cursor.lastrowid

    def insert_session(self, session_data: dict) -> int:
        """
        Insert a session if it doesn't exist. Return session_id.
        """
        self.cursor.execute("""
            SELECT id FROM sessions
            WHERE event_id = ? AND name = ?
        """, (session_data["event_id"], session_data["name"]))
        row = self.cursor.fetchone()
        if row:
            return row['id']

        self.cursor.execute("""
            INSERT INTO sessions (
                event_id, name, date, session_type
            ) VALUES (?, ?, ?, ?)
        """, (
            session_data["event_id"],
            session_data["name"],
            session_data["date"],
            session_data["session_type"]
        ))
        self.commit()
        return self.cursor.lastrowid

    # Additional insert methods for drivers, teams, results, laps, etc. can be added similarly.
    # For brevity, we’ll do them inline in the "migrate_xxx" functions.

#############################
# Migrate Functions
#############################

def migrate_events(db: SQLiteF1Client, year: int) -> pd.DataFrame:
    """
    Create or update events for the given year in the DB,
    and return the full schedule from FastF1.
    """
    logger.info(f"Fetching event schedule for {year}")
    schedule = fastf1.get_event_schedule(year)
    for idx, ev in schedule.iterrows():
        event_data = {
            "year": year,
            "round_number": int(ev["RoundNumber"]),
            "country": ev["Country"],
            "location": ev["Location"],
            "official_event_name": ev["OfficialEventName"],
            "event_name": ev["EventName"],
            "event_date": ev["EventDate"].isoformat() if pd.notna(ev["EventDate"]) else None,
            "event_format": ev["EventFormat"],
            "f1_api_support": bool(ev["F1ApiSupport"])
        }
        db.insert_event(event_data)
    return schedule

def _session_type(session_name: str) -> str:
    """Helper to classify session name into 'practice', 'qualifying', 'race', etc."""
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
    return "unknown"

def migrate_sessions(db: SQLiteF1Client, schedule: pd.DataFrame, year: int):
    """
    For each event in the schedule, insert sessions into DB (FP1, FP2, etc.).
    """
    for idx, ev in schedule.iterrows():
        event_id = db.cursor.execute("""
            SELECT id FROM events WHERE year = ? AND round_number = ?
        """, (year, int(ev["RoundNumber"]))).fetchone()
        if not event_id:
            continue
        event_id = event_id["id"]

        # For each session in 1..5
        for i in range(1, 6):
            s_name = ev.get(f"Session{i}")
            if pd.isna(s_name):
                continue
            s_date_utc = ev.get(f"Session{i}DateUtc")
            s_data = {
                "event_id": event_id,
                "name": s_name,
                "date": s_date_utc.isoformat() if pd.notna(s_date_utc) else None,
                "session_type": _session_type(s_name)
            }
            db.insert_session(s_data)

def migrate_teams_and_drivers(db: SQLiteF1Client, session_obj, year: int):
    """
    Insert all teams and drivers from session_obj.results into DB.
    """
    # Insert teams first
    for _, row in session_obj.results.iterrows():
        team_name = row["TeamName"]
        # Check if team exists
        db.cursor.execute("""
            SELECT id FROM teams WHERE name = ? AND year = ?
        """, (team_name, year))
        existing_team = db.cursor.fetchone()
        if existing_team:
            team_id = existing_team["id"]
        else:
            # Insert
            db.cursor.execute("""
                INSERT INTO teams (name, team_id, team_color, year)
                VALUES (?, ?, ?, ?)
            """, (
                team_name,
                row["TeamId"],
                row["TeamColor"],
                year
            ))
            db.commit()
            team_id = db.cursor.lastrowid

        # Now driver
        abbr = row["Abbreviation"]
        db.cursor.execute("""
            SELECT id FROM drivers WHERE abbreviation = ? AND year = ?
        """, (abbr, year))
        existing_driver = db.cursor.fetchone()
        if existing_driver:
            driver_id = existing_driver["id"]
        else:
            db.cursor.execute("""
                INSERT INTO drivers (
                    driver_number, broadcast_name, abbreviation, driver_id,
                    first_name, last_name, full_name, headshot_url, country_code,
                    team_id, year
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(row["DriverNumber"]),
                row["BroadcastName"],
                abbr,
                row["DriverId"],
                row["FirstName"],
                row["LastName"],
                row["FullName"],
                row["HeadshotUrl"],
                row["CountryCode"],
                team_id,
                year
            ))
            db.commit()

def migrate_results(db: SQLiteF1Client, session_obj, session_id: int, year: int):
    """
    Insert session results from session_obj.results into DB.
    """
    if not hasattr(session_obj, "results") or session_obj.results is None or len(session_obj.results) == 0:
        return

    # Map drivers
    drivers_map = {}
    for _, row in session_obj.results.iterrows():
        abbr = row["Abbreviation"]
        db.cursor.execute("""
            SELECT id FROM drivers WHERE abbreviation = ? AND year = ?
        """, (abbr, year))
        found = db.cursor.fetchone()
        if found:
            drivers_map[abbr] = found["id"]

    for _, row in session_obj.results.iterrows():
        abbr = row["Abbreviation"]
        driver_id = drivers_map.get(abbr)
        if not driver_id:
            continue
        # Insert
        db.cursor.execute("""
            SELECT id FROM results WHERE session_id = ? AND driver_id = ?
        """, (session_id, driver_id))
        if db.cursor.fetchone():
            continue  # already inserted

        db.cursor.execute("""
            INSERT INTO results (
                session_id, driver_id, position, classified_position,
                grid_position, q1_time, q2_time, q3_time, race_time,
                status, points
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            driver_id,
            int(row["Position"]) if pd.notna(row["Position"]) else None,
            row["ClassifiedPosition"] if pd.notna(row["ClassifiedPosition"]) else None,
            int(row["GridPosition"]) if pd.notna(row["GridPosition"]) else None,
            str(row["Q1"]) if pd.notna(row["Q1"]) else None,
            str(row["Q2"]) if pd.notna(row["Q2"]) else None,
            str(row["Q3"]) if pd.notna(row["Q3"]) else None,
            str(row["Time"]) if pd.notna(row["Time"]) else None,
            row["Status"] if pd.notna(row["Status"]) else None,
            float(row["Points"]) if pd.notna(row["Points"]) else None
        ))
        db.commit()

def migrate_laps(db: SQLiteF1Client, session_obj, session_id: int, year: int):
    """
    Insert laps from session_obj.laps into DB (including partial telemetry).
    """
    if not hasattr(session_obj, "laps") or session_obj.laps is None or len(session_obj.laps) == 0:
        return

    # Map drivers
    drivers_map = {}
    for _, row in session_obj.results.iterrows():
        abbr = row["Abbreviation"]
        db.cursor.execute("""
            SELECT id FROM drivers WHERE abbreviation = ? AND year = ?
        """, (abbr, year))
        found = db.cursor.fetchone()
        if found:
            drivers_map[abbr] = found["id"]

    # For performance, let's skip advanced telemetry on every lap,
    # and only do it for "best" laps or every 10th lap, for example.
    laps_df = session_obj.laps
    for _, lap in tqdm(laps_df.iterrows(), total=len(laps_df), desc="Migrating laps"):
        abbr = lap["Driver"]
        driver_id = drivers_map.get(abbr)
        if not driver_id:
            continue
        lap_number = int(lap["LapNumber"]) if pd.notna(lap["LapNumber"]) else None
        if not lap_number:
            continue

        # Insert lap
        db.cursor.execute("""
            SELECT id FROM laps WHERE session_id = ? AND driver_id = ? AND lap_number = ?
        """, (session_id, driver_id, lap_number))
        if db.cursor.fetchone():
            # already inserted
            continue

        lap_data = {
            "session_id": session_id,
            "driver_id": driver_id,
            "lap_time": str(lap["LapTime"]) if pd.notna(lap["LapTime"]) else None,
            "lap_number": lap_number,
            "stint": int(lap["Stint"]) if pd.notna(lap["Stint"]) else None,
            "pit_out_time": str(lap["PitOutTime"]) if pd.notna(lap["PitOutTime"]) else None,
            "pit_in_time": str(lap["PitInTime"]) if pd.notna(lap["PitInTime"]) else None,
            "sector1_time": str(lap["Sector1Time"]) if pd.notna(lap["Sector1Time"]) else None,
            "sector2_time": str(lap["Sector2Time"]) if pd.notna(lap["Sector2Time"]) else None,
            "sector3_time": str(lap["Sector3Time"]) if pd.notna(lap["Sector3Time"]) else None,
            "sector1_session_time": str(lap["Sector1SessionTime"]) if pd.notna(lap["Sector1SessionTime"]) else None,
            "sector2_session_time": str(lap["Sector2SessionTime"]) if pd.notna(lap["Sector2SessionTime"]) else None,
            "sector3_session_time": str(lap["Sector3SessionTime"]) if pd.notna(lap["Sector3SessionTime"]) else None,
            "speed_i1": float(lap["SpeedI1"]) if pd.notna(lap["SpeedI1"]) else None,
            "speed_i2": float(lap["SpeedI2"]) if pd.notna(lap["SpeedI2"]) else None,
            "speed_fl": float(lap["SpeedFL"]) if pd.notna(lap["SpeedFL"]) else None,
            "speed_st": float(lap["SpeedST"]) if pd.notna(lap["SpeedST"]) else None,
            "is_personal_best": 1 if (pd.notna(lap["IsPersonalBest"]) and lap["IsPersonalBest"]) else 0,
            "compound": lap["Compound"] if pd.notna(lap["Compound"]) else None,
            "tyre_life": float(lap["TyreLife"]) if pd.notna(lap["TyreLife"]) else None,
            "fresh_tyre": 1 if (pd.notna(lap["FreshTyre"]) and lap["FreshTyre"]) else 0,
            "lap_start_time": str(lap["LapStartTime"]) if pd.notna(lap["LapStartTime"]) else None,
            "lap_start_date": lap["LapStartDate"].isoformat() if pd.notna(lap["LapStartDate"]) else None,
            "track_status": lap["TrackStatus"] if pd.notna(lap["TrackStatus"]) else None,
            "position": int(lap["Position"]) if pd.notna(lap["Position"]) else None,
            "deleted": 1 if (pd.notna(lap["Deleted"]) and lap["Deleted"]) else 0,
            "deleted_reason": lap["DeletedReason"] if pd.notna(lap["DeletedReason"]) else None,
            "fast_f1_generated": 1 if (pd.notna(lap["FastF1Generated"]) and lap["FastF1Generated"]) else 0,
            "is_accurate": 1 if (pd.notna(lap["IsAccurate"]) and lap["IsAccurate"]) else 0,
            "time": str(lap["Time"]) if pd.notna(lap["Time"]) else None,
            "session_time": str(lap["SessionTime"]) if pd.notna(lap["SessionTime"]) else None
        }

        # Build dynamic insert
        keys = ",".join(lap_data.keys())
        placeholders = ",".join(["?"] * len(lap_data))
        values = list(lap_data.values())

        db.cursor.execute(f"""
            INSERT INTO laps ({keys}) VALUES ({placeholders})
        """, values)
        db.commit()

        # (Optional) Telemetry
        # e.g. if personal best or every 10th lap
        if lap_data["is_personal_best"] == 1 or (lap_number % 10 == 0):
            try:
                tel = lap.get_telemetry()
                if tel is not None and not tel.empty:
                    # Sample it to avoid massive data
                    sample_size = 100
                    if len(tel) > sample_size:
                        tel = tel.iloc[:: len(tel)//sample_size]
                    for _, tel_row in tel.iterrows():
                        # Insert telemetry
                        tel_data = {
                            "driver_id": driver_id,
                            "lap_number": lap_number,
                            "session_id": session_id,
                            "time": str(tel_row["Time"]) if pd.notna(tel_row["Time"]) else None,
                            "session_time": str(tel_row["SessionTime"]) if pd.notna(tel_row["SessionTime"]) else None,
                            "date": tel_row["Date"].isoformat() if pd.notna(tel_row["Date"]) else None,
                            "speed": float(tel_row["Speed"]) if pd.notna(tel_row["Speed"]) else None,
                            "rpm": float(tel_row["RPM"]) if pd.notna(tel_row["RPM"]) else None,
                            "gear": int(tel_row["nGear"]) if pd.notna(tel_row["nGear"]) else None,
                            "throttle": float(tel_row["Throttle"]) if pd.notna(tel_row["Throttle"]) else None,
                            "brake": 1 if (pd.notna(tel_row["Brake"]) and tel_row["Brake"]) else 0,
                            "drs": int(tel_row["DRS"]) if pd.notna(tel_row["DRS"]) else None,
                            "x": float(tel_row["X"]) if pd.notna(tel_row["X"]) else None,
                            "y": float(tel_row["Y"]) if pd.notna(tel_row["Y"]) else None,
                            "z": float(tel_row["Z"]) if pd.notna(tel_row["Z"]) else None,
                            "source": tel_row["Source"] if pd.notna(tel_row["Source"]) else None,
                            "year": year
                        }
                        k2 = ",".join(tel_data.keys())
                        p2 = ",".join(["?"] * len(tel_data))
                        v2 = list(tel_data.values())
                        db.cursor.execute(f"""
                            INSERT INTO telemetry ({k2}) VALUES ({p2})
                        """, v2)
                    db.commit()
            except Exception as e:
                logger.error(f"Telemetry error lap {lap_number}, driver {abbr}: {e}")

def migrate_weather(db: SQLiteF1Client, session_obj, session_id: int):
    """
    Insert historical weather from session_obj.weather_data
    """
    if not hasattr(session_obj, "weather_data") or session_obj.weather_data is None or session_obj.weather_data.empty:
        return
    wdf = session_obj.weather_data
    for _, wrow in wdf.iterrows():
        time_str = str(wrow["Time"]) if pd.notna(wrow["Time"]) else None
        # Insert if not existing
        db.cursor.execute("""
            SELECT id FROM weather WHERE session_id = ? AND time = ?
        """, (session_id, time_str))
        if db.cursor.fetchone():
            continue
        # Insert
        db.cursor.execute("""
            INSERT INTO weather (
                session_id, time, air_temp, humidity, pressure, rainfall,
                track_temp, wind_direction, wind_speed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_id,
            time_str,
            float(wrow["AirTemp"]) if pd.notna(wrow["AirTemp"]) else None,
            float(wrow["Humidity"]) if pd.notna(wrow["Humidity"]) else None,
            float(wrow["Pressure"]) if pd.notna(wrow["Pressure"]) else None,
            1 if (pd.notna(wrow["Rainfall"]) and wrow["Rainfall"]) else 0,
            float(wrow["TrackTemp"]) if pd.notna(wrow["TrackTemp"]) else None,
            int(wrow["WindDirection"]) if pd.notna(wrow["WindDirection"]) else None,
            float(wrow["WindSpeed"]) if pd.notna(wrow["WindSpeed"]) else None
        ))
    db.commit()

def migrate_session_details(db: SQLiteF1Client, schedule: pd.DataFrame, year: int):
    """
    For each event, for each session, load data from FastF1 and store in DB.
    """
    for _, ev in tqdm(schedule.iterrows(), total=len(schedule), desc="Events"):
        if not ev["F1ApiSupport"]:
            logger.info(f"Skipping event {ev['EventName']} because no F1 API support.")
            continue
        # Get event ID from DB
        row = db.cursor.execute("""
            SELECT id FROM events WHERE year = ? AND round_number = ?
        """, (year, int(ev["RoundNumber"]))).fetchone()
        if not row:
            continue
        event_id = row["id"]

        # Attempt sessions for known session identifiers
        # e.g. FP1, FP2, FP3, Q, R, S, SQ, SS, etc.
        for sid in ["FP1", "FP2", "FP3", "Q", "R", "S", "SQ", "SS"]:
            try:
                session_obj = fastf1.get_session(year, ev["RoundNumber"], sid)
                session_obj.load()
            except Exception as e:
                # If session doesn't exist, skip
                logger.warning(f"No session {sid} for {ev['EventName']}: {e}")
                continue

            # Find the session row in DB
            db.cursor.execute("""
                SELECT id FROM sessions WHERE event_id = ? AND name = ?
            """, (event_id, session_obj.name))
            sess_row = db.cursor.fetchone()
            if not sess_row:
                logger.info(f"Session {session_obj.name} not found in DB, skipping.")
                continue
            session_id = sess_row["id"]

            # Update session with extra details
            try:
                # session_start_time, total_laps, t0_date, etc.
                db.cursor.execute("""
                    UPDATE sessions
                    SET total_laps = ?,
                        session_start_time = ?,
                        t0_date = ?
                    WHERE id = ?
                """, (
                    session_obj.total_laps if hasattr(session_obj, "total_laps") else None,
                    str(session_obj.session_start_time) if hasattr(session_obj, "session_start_time") else None,
                    session_obj.t0_date.isoformat() if (hasattr(session_obj, "t0_date") and session_obj.t0_date) else None,
                    session_id
                ))
                db.commit()
            except Exception as e2:
                logger.error(f"Failed to update session row: {e2}")

            # Migrate teams & drivers
            if hasattr(session_obj, "results") and session_obj.results is not None and len(session_obj.results) > 0:
                migrate_teams_and_drivers(db, session_obj, year)
                # Migrate results
                migrate_results(db, session_obj, session_id, year)

            # Migrate laps (including partial telemetry)
            if hasattr(session_obj, "laps"):
                migrate_laps(db, session_obj, session_id, year)

            # Migrate weather
            migrate_weather(db, session_obj, session_id)

            # Sleep a bit to avoid rate limiting
            time.sleep(1)

def main():
    parser = argparse.ArgumentParser(description="Migrate full F1 data to SQLite.")
    parser.add_argument("--year", type=int, required=True, help="Which year to migrate")
    args = parser.parse_args()

    db = SQLiteF1Client(SQLITE_DB_PATH)
    try:
        schedule = migrate_events(db, args.year)
        migrate_sessions(db, schedule, args.year)
        migrate_session_details(db, schedule, args.year)
        logger.info("Migration complete!")
    finally:
        db.close()

if __name__ == "__main__":
    main()
