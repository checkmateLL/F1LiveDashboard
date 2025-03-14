import sqlite3
import logging
import os
import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class F1DataService:
    """
    Abstraction layer for F1 data access
    
    This class provides a unified interface for accessing F1 data, regardless of where it's stored
    (SQLite, Redis, Xata, etc.). It automatically selects the appropriate data source based 
    on what's being requested (historical vs. live data).
    """
    
    def __init__(self, sqlite_path='./f1_data.db'):
        """Initialize the F1 data service"""
        self.sqlite_path = sqlite_path
        self.sqlite_conn = None
        self.redis_service = None
        
        # Initialize SQLite connection
        self._init_sqlite()
        
        # Try to initialize Redis service
        try:
            from redis_live_service import RedisLiveDataService
            self.redis_service = RedisLiveDataService()
            logger.info("Redis live data service initialized")
        except Exception as e:
            logger.warning(f"Redis live data service not available: {e}")
            self.redis_service = None
    
    def _init_sqlite(self):
        """Initialize SQLite connection"""
        try:
            if os.path.exists(self.sqlite_path):
                self.sqlite_conn = sqlite3.connect(self.sqlite_path)
                self.sqlite_conn.row_factory = sqlite3.Row
                logger.info(f"Connected to SQLite database: {self.sqlite_path}")
            else:
                logger.warning(f"SQLite database does not exist: {self.sqlite_path}")
                self.sqlite_conn = None
        except sqlite3.Error as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            self.sqlite_conn = None
    
    def _get_sqlite_cursor(self):
        """Get a SQLite cursor, reconnecting if necessary"""
        if not self.sqlite_conn:
            self._init_sqlite()
            
        if self.sqlite_conn:
            return self.sqlite_conn.cursor()
        return None
    
    def close(self):
        """Close all connections"""
        if self.sqlite_conn:
            self.sqlite_conn.close()
            logger.info("Closed SQLite connection")
        
        if self.redis_service:
            self.redis_service.stop_polling()
            logger.info("Stopped Redis polling")
    
    def get_available_years(self) -> List[int]:
        """Get list of available years in the database"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("SELECT DISTINCT year FROM events ORDER BY year DESC")
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting available years: {e}")
            return []
    
    def get_current_session(self) -> Optional[Dict[str, Any]]:
        """Get information about the current live session (if any)"""
        if self.redis_service:
            return self.redis_service.get_live_session()
        return None
    
    def start_live_polling(self) -> bool:
        """Start polling for live data"""
        if self.redis_service:
            self.redis_service.start_polling()
            return True
        return False
    
    def get_events(self, year: int) -> List[Dict[str, Any]]:
        """Get all events for a specific year"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT e.id, e.round_number, e.country, e.location, e.official_event_name,
                       e.event_name, e.event_date, e.event_format, e.f1_api_support
                FROM events e
                WHERE e.year = ?
                ORDER BY e.round_number
            """, (year,))
            
            events = []
            for row in cursor.fetchall():
                events.append({
                    'id': row['id'],
                    'round_number': row['round_number'],
                    'country': row['country'],
                    'location': row['location'],
                    'official_event_name': row['official_event_name'],
                    'event_name': row['event_name'],
                    'event_date': row['event_date'],
                    'event_format': row['event_format'],
                    'f1_api_support': bool(row['f1_api_support'])
                })
            return events
        except sqlite3.Error as e:
            logger.error(f"Error getting events: {e}")
            return []
    
    def get_event(self, year: int, round_number: int) -> Optional[Dict[str, Any]]:
        """Get a specific event by year and round number"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return None
        
        try:
            cursor.execute("""
                SELECT e.id, e.round_number, e.country, e.location, e.official_event_name,
                       e.event_name, e.event_date, e.event_format, e.f1_api_support
                FROM events e
                WHERE e.year = ? AND e.round_number = ?
            """, (year, round_number))
            
            row = cursor.fetchone()
            if row:
                return {
                    'id': row['id'],
                    'round_number': row['round_number'],
                    'country': row['country'],
                    'location': row['location'],
                    'official_event_name': row['official_event_name'],
                    'event_name': row['event_name'],
                    'event_date': row['event_date'],
                    'event_format': row['event_format'],
                    'f1_api_support': bool(row['f1_api_support'])
                }
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting event: {e}")
            return None
    
    def get_sessions(self, event_id: int) -> List[Dict[str, Any]]:
        """Get all sessions for a specific event"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT s.id, s.name, s.date, s.session_type, 
                       s.total_laps, s.session_start_time, s.t0_date
                FROM sessions s
                WHERE s.event_id = ?
                ORDER BY CASE 
                    WHEN s.session_type = 'practice' THEN 1
                    WHEN s.session_type = 'qualifying' THEN 2
                    WHEN s.session_type = 'sprint_shootout' THEN 3
                    WHEN s.session_type = 'sprint_qualifying' THEN 4
                    WHEN s.session_type = 'sprint' THEN 5
                    WHEN s.session_type = 'race' THEN 6
                    ELSE 7
                END
            """, (event_id,))
            
            sessions = []
            for row in cursor.fetchall():
                sessions.append({
                    'id': row['id'],
                    'name': row['name'],
                    'date': row['date'],
                    'session_type': row['session_type'],
                    'total_laps': row['total_laps'],
                    'session_start_time': row['session_start_time'],
                    't0_date': row['t0_date']
                })
            return sessions
        except sqlite3.Error as e:
            logger.error(f"Error getting sessions: {e}")
            return []
    
    def get_teams(self, year: int) -> List[Dict[str, Any]]:
        """Get all teams for a specific year"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT t.id, t.name, t.team_id, t.team_color
                FROM teams t
                WHERE t.year = ?
                ORDER BY t.name
            """, (year,))
            
            teams = []
            for row in cursor.fetchall():
                teams.append({
                    'id': row['id'],
                    'name': row['name'],
                    'team_id': row['team_id'],
                    'team_color': row['team_color']
                })
            return teams
        except sqlite3.Error as e:
            logger.error(f"Error getting teams: {e}")
            return []
    
    def get_drivers(self, year: int, team_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get all drivers for a specific year, optionally filtered by team"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            query = """
                SELECT d.id, d.driver_number, d.broadcast_name, d.abbreviation,
                       d.driver_id, d.first_name, d.last_name, d.full_name,
                       d.headshot_url, d.country_code, d.team_id, t.name as team_name,
                       t.team_color
                FROM drivers d
                JOIN teams t ON d.team_id = t.id
                WHERE d.year = ?
            """
            params = [year]
            
            if team_id is not None:
                query += " AND d.team_id = ?"
                params.append(team_id)
            
            query += " ORDER BY t.name, d.full_name"
            
            cursor.execute(query, params)
            
            drivers = []
            for row in cursor.fetchall():
                drivers.append({
                    'id': row['id'],
                    'driver_number': row['driver_number'],
                    'broadcast_name': row['broadcast_name'],
                    'abbreviation': row['abbreviation'],
                    'driver_id': row['driver_id'],
                    'first_name': row['first_name'],
                    'last_name': row['last_name'],
                    'full_name': row['full_name'],
                    'headshot_url': row['headshot_url'],
                    'country_code': row['country_code'],
                    'team_id': row['team_id'],
                    'team_name': row['team_name'],
                    'team_color': row['team_color']
                })
            return drivers
        except sqlite3.Error as e:
            logger.error(f"Error getting drivers: {e}")
            return []
    
    def get_driver_standings(self, year: int) -> List[Dict[str, Any]]:
        """Get driver standings for a specific year"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        # Check if we have live standings for current year
        current_session = self.get_current_session()
        if current_session and current_session.get('year') == year and self.redis_service:
            live_standings = self.redis_service.get_live_standings()
            if live_standings:
                return live_standings
        
        # Fallback to historical data
        try:
            cursor.execute("""
                SELECT d.id, d.full_name, d.abbreviation, t.name as team_name, t.team_color,
                       SUM(r.points) as total_points
                FROM drivers d
                JOIN teams t ON d.team_id = t.id
                JOIN results r ON d.id = r.driver_id
                JOIN sessions s ON r.session_id = s.id
                JOIN events e ON s.event_id = e.id
                WHERE e.year = ? AND s.session_type = 'race'
                GROUP BY d.id
                ORDER BY total_points DESC
            """, (year,))
            
            standings = []
            for i, row in enumerate(cursor.fetchall()):
                standings.append({
                    'position': i + 1,
                    'driver_id': row['id'],
                    'driver_name': row['full_name'],
                    'abbreviation': row['abbreviation'],
                    'team': row['team_name'],
                    'team_color': row['team_color'],
                    'points': row['total_points']
                })
            return standings
        except sqlite3.Error as e:
            logger.error(f"Error getting driver standings: {e}")
            return []
    
    def get_constructor_standings(self, year: int) -> List[Dict[str, Any]]:
        """Get constructor standings for a specific year"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT t.name as team_name, t.team_color,
                       SUM(r.points) as total_points
                FROM teams t
                JOIN drivers d ON t.id = d.team_id
                JOIN results r ON d.id = r.driver_id
                JOIN sessions s ON r.session_id = s.id
                JOIN events e ON s.event_id = e.id
                WHERE e.year = ? AND s.session_type = 'race'
                GROUP BY t.id
                ORDER BY total_points DESC
            """, (year,))
            
            standings = []
            for i, row in enumerate(cursor.fetchall()):
                standings.append({
                    'position': i + 1,
                    'team': row['team_name'],
                    'team_color': row['team_color'],
                    'points': row['total_points']
                })
            return standings
        except sqlite3.Error as e:
            logger.error(f"Error getting constructor standings: {e}")
            return []
    
    def get_race_results(self, session_id: int) -> List[Dict[str, Any]]:
        """Get race results for a specific session"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT r.position, d.full_name as driver_name, d.abbreviation, t.name as team_name, 
                       r.grid_position, r.status, r.points, r.race_time
                FROM results r
                JOIN drivers d ON r.driver_id = d.id
                JOIN teams t ON d.team_id = t.id
                WHERE r.session_id = ?
                ORDER BY 
                    CASE WHEN r.position IS NULL THEN 999 ELSE r.position END
            """, (session_id,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'position': row['position'],
                    'driver_name': row['driver_name'],
                    'abbreviation': row['abbreviation'],
                    'team': row['team_name'],
                    'grid_position': row['grid_position'],
                    'status': row['status'],
                    'points': row['points'],
                    'race_time': row['race_time']
                })
            return results
        except sqlite3.Error as e:
            logger.error(f"Error getting race results: {e}")
            return []
    
    def get_qualifying_results(self, session_id: int) -> List[Dict[str, Any]]:
        """Get qualifying results for a specific session"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT r.position, d.full_name as driver_name, d.abbreviation, t.name as team_name, 
                       r.q1_time, r.q2_time, r.q3_time
                FROM results r
                JOIN drivers d ON r.driver_id = d.id
                JOIN teams t ON d.team_id = t.id
                WHERE r.session_id = ?
                ORDER BY 
                    CASE WHEN r.position IS NULL THEN 999 ELSE r.position END
            """, (session_id,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'position': row['position'],
                    'driver_name': row['driver_name'],
                    'abbreviation': row['abbreviation'],
                    'team': row['team_name'],
                    'q1_time': row['q1_time'],
                    'q2_time': row['q2_time'],
                    'q3_time': row['q3_time']
                })
            return results
        except sqlite3.Error as e:
            logger.error(f"Error getting qualifying results: {e}")
            return []
    
    def get_driver_lap_times(self, session_id: int, driver_abbr: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get lap times for a specific session, optionally filtered by driver"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            query = """
                SELECT l.lap_number, l.lap_time, d.abbreviation, t.team_color,
                       l.sector1_time, l.sector2_time, l.sector3_time,
                       l.compound, l.tyre_life, l.is_personal_best
                FROM laps l
                JOIN drivers d ON l.driver_id = d.id
                JOIN teams t ON d.team_id = t.id
                WHERE l.session_id = ? AND l.lap_time IS NOT NULL
            """
            params = [session_id]
            
            if driver_abbr:
                query += " AND d.abbreviation = ?"
                params.append(driver_abbr)
            
            query += " ORDER BY d.abbreviation, l.lap_number"
            
            cursor.execute(query, params)
            
            lap_times = []
            for row in cursor.fetchall():
                # Extract lap time in seconds (format: 0 days 00:01:30.000000)
                lap_time_sec = None
                lap_time_str = row['lap_time']
                if lap_time_str and "days" in lap_time_str:
                    time_parts = lap_time_str.split()
                    if len(time_parts) >= 3:
                        time_str = time_parts[2]
                        # Convert to seconds
                        h, m, s = time_str.split(':')
                        lap_time_sec = int(h) * 3600 + int(m) * 60 + float(s)
                
                lap_times.append({
                    'lap_number': row['lap_number'],
                    'lap_time': row['lap_time'],
                    'lap_time_sec': lap_time_sec,
                    'driver': row['abbreviation'],
                    'team_color': row['team_color'],
                    'sector1': row['sector1_time'],
                    'sector2': row['sector2_time'],
                    'sector3': row['sector3_time'],
                    'compound': row['compound'],
                    'tyre_life': row['tyre_life'],
                    'is_personal_best': bool(row['is_personal_best']) if row['is_personal_best'] is not None else False
                })
            return lap_times
        except sqlite3.Error as e:
            logger.error(f"Error getting lap times: {e}")
            return []
    
    def get_telemetry(self, session_id: int, driver_id: int, lap_number: int) -> List[Dict[str, Any]]:
        """Get telemetry data for a specific lap"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT t.session_time, t.speed, t.rpm, t.gear, t.throttle, t.brake, t.drs, t.x, t.y
                FROM telemetry t
                WHERE t.session_id = ? AND t.driver_id = ? AND t.lap_number = ?
                ORDER BY t.session_time
            """, (session_id, driver_id, lap_number))
            
            telemetry = []
            for row in cursor.fetchall():
                telemetry.append({
                    'session_time': row['session_time'],
                    'speed': row['speed'],
                    'rpm': row['rpm'],
                    'gear': row['gear'],
                    'throttle': row['throttle'],
                    'brake': bool(row['brake']) if row['brake'] is not None else None,
                    'drs': row['drs'],
                    'x': row['x'],
                    'y': row['y']
                })
            return telemetry
        except sqlite3.Error as e:
            logger.error(f"Error getting telemetry: {e}")
            return []
    
    def get_weather(self, session_id: int) -> List[Dict[str, Any]]:
        """Get weather data for a specific session"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        # Check if we have live weather data
        current_session = self.get_current_session()
        if current_session and self.redis_service:
            # For future improvement: check if session_id corresponds to current live session
            live_weather = self.redis_service.get_live_weather()
            if live_weather:
                return [live_weather]  # Return as a list for consistency
        
        # Fallback to historical data
        try:
            cursor.execute("""
                SELECT w.time, w.air_temp, w.humidity, w.pressure, w.rainfall,
                       w.track_temp, w.wind_direction, w.wind_speed
                FROM weather w
                WHERE w.session_id = ?
                ORDER BY w.time
            """, (session_id,))
            
            weather = []
            for row in cursor.fetchall():
                weather.append({
                    'time': row['time'],
                    'air_temp': row['air_temp'],
                    'humidity': row['humidity'],
                    'pressure': row['pressure'],
                    'rainfall': bool(row['rainfall']) if row['rainfall'] is not None else None,
                    'track_temp': row['track_temp'],
                    'wind_direction': row['wind_direction'],
                    'wind_speed': row['wind_speed']
                })
            return weather
        except sqlite3.Error as e:
            logger.error(f"Error getting weather data: {e}")
            return []
    
    def get_tire_compounds(self, year: int) -> List[Dict[str, Any]]:
        """Get tire compounds for a specific year"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            cursor.execute("""
                SELECT compound_name, color_code
                FROM tyre_compounds
                WHERE year = ?
            """, (year,))
            
            compounds = []
            for row in cursor.fetchall():
                compounds.append({
                    'name': row['compound_name'],
                    'color': row['color_code']
                })
            return compounds
        except sqlite3.Error as e:
            logger.error(f"Error getting tire compounds: {e}")
            return []
    
    def get_live_timing(self) -> List[Dict[str, Any]]:
        """Get current live timing data"""
        if self.redis_service:
            return self.redis_service.get_live_timing() or []
        return []
    
    def get_live_tires(self) -> Dict[str, Any]:
        """Get current live tire data"""
        if self.redis_service:
            return self.redis_service.get_live_tires() or {}
        return {}
    
    def get_track_status(self) -> Optional[Dict[str, Any]]:
        """Get current track status"""
        if self.redis_service:
            return self.redis_service.get_track_status()
        return None
    
    def get_compare_drivers(self, year: int, driver1_abbr: str, driver2_abbr: str) -> List[Dict[str, Any]]:
        """Get head-to-head comparison data for two drivers"""
        cursor = self._get_sqlite_cursor()
        if not cursor:
            return []
        
        try:
            # Get driver IDs
            cursor.execute("""
                SELECT d.id, d.abbreviation
                FROM drivers d
                WHERE d.year = ? AND d.abbreviation IN (?, ?)
            """, (year, driver1_abbr, driver2_abbr))
            
            drivers = {}
            for row in cursor.fetchall():
                drivers[row['abbreviation']] = row['id']
                
            if len(drivers) != 2:
                logger.error(f"Could not find both drivers: {driver1_abbr}, {driver2_abbr}")
                return []
            
            # Get race sessions for this year
            cursor.execute("""
                SELECT s.id, e.round_number, e.event_name
                FROM sessions s
                JOIN events e ON s.event_id = e.id
                WHERE e.year = ? AND s.session_type = 'race'
                ORDER BY e.round_number
            """, (year,))
            
            session_info = {}
            for row in cursor.fetchall():
                session_info[row['id']] = {
                    'round': row['round_number'],
                    'name': row['event_name']
                }
            
            # Get results for both drivers
            cursor.execute("""
                SELECT r.driver_id, r.session_id, r.position, r.grid_position, r.points
                FROM results r
                WHERE r.driver_id IN (?, ?) AND r.session_id IN ({})
                ORDER BY r.session_id
            """.format(','.join('?' * len(session_info))),
                [drivers[driver1_abbr], drivers[driver2_abbr]] + list(session_info.keys()))
            
            # Organize results by session
            results_by_session = {}
            for row in cursor.fetchall():
                session_id = row['session_id']
                driver_id = row['driver_id']
                
                if session_id not in results_by_session:
                    results_by_session[session_id] = {}
                    
                driver_abbr = driver1_abbr if driver_id == drivers[driver1_abbr] else driver2_abbr
                results_by_session[session_id][driver_abbr] = {
                    'position': row['position'],
                    'grid_position': row['grid_position'],
                    'points': row['points']
                }
            
            # Build comparison data
            comparison = []
            for session_id, info in session_info.items():
                if session_id in results_by_session and len(results_by_session[session_id]) == 2:
                    comparison.append({
                        'race': f"Round {info['round']} - {info['name']}",
                        'round': info['round'],
                        f"{driver1_abbr}_grid": results_by_session[session_id][driver1_abbr]['grid_position'],
                        f"{driver1_abbr}_finish": results_by_session[session_id][driver1_abbr]['position'],
                        f"{driver1_abbr}_points": results_by_session[session_id][driver1_abbr]['points'],
                        f"{driver2_abbr}_grid": results_by_session[session_id][driver2_abbr]['grid_position'],
                        f"{driver2_abbr}_finish": results_by_session[session_id][driver2_abbr]['position'],
                        f"{driver2_abbr}_points": results_by_session[session_id][driver2_abbr]['points']
                    })
            
            return comparison
        except sqlite3.Error as e:
            logger.error(f"Error getting driver comparison: {e}")
            return []