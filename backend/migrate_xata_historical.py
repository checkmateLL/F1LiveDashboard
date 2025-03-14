import fastf1
import fastf1.plotting
import pandas as pd
import datetime
import argparse
import logging
import time
import os
from tqdm import tqdm
from xata_client import f1_client

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

def migrate_events(year):
    """Migrate events data for a specific year to Xata"""
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
        if not f1_client.event_exists(year, event_data["round_number"]):
            logger.info(f"Adding event: {event_data['event_name']}")
            f1_client.create_event(event_data)
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    
    return schedule

def migrate_sessions(schedule, year):
    """Migrate sessions data for events in a year to Xata"""
    for idx, event in schedule.iterrows():
        # Get event from Xata
        event_record = f1_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
            
        event_id = event_record.id
        
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
            if not f1_client.session_exists(event_id, session_name):
                logger.info(f"Adding session: {session_name} for {event['EventName']}")
                f1_client.create_session(session_data)
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

def migrate_drivers_and_teams(year):
    """Migrate drivers and teams data for a specific year to Xata"""
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
            if not f1_client.team_exists(team_name, year):
                logger.info(f"Adding team: {team_name}")
                f1_client.create_team(team_data)
            else:
                logger.info(f"Team already exists: {team_name}")
                
            teams_processed.add(team_name)
    
    # Process drivers
    for _, driver_data in session.results.iterrows():
        # Get team id reference
        team_record = f1_client.get_team(driver_data['TeamName'], year)
        
        if not team_record:
            logger.warning(f"Team {driver_data['TeamName']} not found, skipping driver {driver_data['FullName']}")
            continue
            
        team_id = team_record.id
        
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
            "year": year  # Add year field for filtering
        }
        
        # Check if driver already exists
        if not f1_client.driver_exists(driver_info["abbreviation"], year):
            logger.info(f"Adding driver: {driver_info['full_name']}")
            f1_client.create_driver(driver_info)
        else:
            logger.info(f"Driver already exists: {driver_info['full_name']}")

def migrate_session_details(schedule, year):
    """Migrate detailed session data including results and laps"""
    # Process each event
    for idx, event in tqdm(schedule.iterrows(), desc="Processing events", total=len(schedule)):
        # Skip if not supported by F1 API
        if not event['F1ApiSupport']:
            logger.info(f"Event {event['EventName']} not supported by F1 API, skipping")
            continue
            
        event_record = f1_client.get_event(year, int(event['RoundNumber']))
        
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping session details")
            continue
            
        # Process each session type
        for session_type in ['FP1', 'FP2', 'FP3', 'Q', 'S', 'SQ', 'SS', 'R']:
            try:
                session = fastf1.get_session(year, event['RoundNumber'], session_type)
                
                # Get session from Xata
                session_record = f1_client.get_session(event_record.id, session.name)
                
                if not session_record:
                    logger.info(f"Session {session.name} not found in database, skipping")
                    continue
                    
                session_id = session_record.id
                
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
                    f1_client.update_session(session_id, session_updates)
                
                # Process results
                migrate_results(session, session_id, year)
                
                # Process laps data
                migrate_laps(session, session_id, year)
                
                # Process weather data
                migrate_weather(session, session_id)
                
                # Throttle to avoid API rate limits
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Failed to process session {session_type} for event {event['EventName']}: {e}")

def migrate_results(session, session_id, year):
    """Migrate results data for a session"""
    if not hasattr(session, 'results') or len(session.results) == 0:
        logger.warning(f"No results available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = f1_client.get_drivers(year)
    driver_map = {d.abbreviation: d.id for d in drivers}
    
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
        if not f1_client.result_exists(session_id, driver_id):
            logger.info(f"Adding result for {result['Abbreviation']} in {session.name}")
            f1_client.create_result(result_data)
        else:
            logger.info(f"Result already exists for {result['Abbreviation']} in {session.name}")

def migrate_laps(session, session_id, year):
    """Migrate lap data for a session"""
    if not hasattr(session, 'laps') or len(session.laps) == 0:
        logger.warning(f"No lap data available for session {session.name}")
        return
        
    # Get all drivers for this year
    drivers = f1_client.get_drivers(year)
    driver_map = {d.abbreviation: d.id for d in drivers}
    
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
        if not f1_client.lap_exists(session_id, driver_id, lap_number):
            logger.info(f"Adding lap {lap_number} for {lap['Driver']}")
            f1_client.create_lap(lap_data)
            
            # For selected interesting laps, add some telemetry data
            if lap_data["is_personal_best"] or (lap_number % 10 == 0):
                migrate_telemetry_for_lap(session, lap, driver_id, year)
                
            lap_count += 1
            
            # Throttle to avoid API rate limits
            if lap_count % batch_size == 0:
                logger.info(f"Processed {lap_count} laps, pausing briefly")
                time.sleep(2)
        else:
            logger.info(f"Lap already exists: {lap_number} for {lap['Driver']}")

def migrate_telemetry_for_lap(session, lap, driver_id, year):
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
        if len(telemetry) > 20:
            telemetry = telemetry.iloc[::len(telemetry)//20]
            
        # Get the session ID from the function parameter
        session_id = session
        
        for idx, tel in telemetry.iterrows():
            tel_data = {
                "driver_id": driver_id,
                "lap_number": lap_number,
                "session_id": session_id,
                "time": str(tel['Time']) if 'Time' in tel and pd.notna(tel['Time']) else None,
                "session_time": str(tel['SessionTime']) if 'SessionTime' in tel and pd.notna(tel['SessionTime']) else None,
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
            
            # Remove None values
            tel_data = {k: v for k, v in tel_data.items() if v is not None}
            
            # Insert telemetry data without checking for duplicates (we're already sampling)
            f1_client.create_telemetry(tel_data)
    except Exception as e:
        logger.error(f"Failed to process telemetry: {e}")