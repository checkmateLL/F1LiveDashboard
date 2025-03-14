import fastf1
import pandas as pd
import logging
import os
import argparse
import time
from tqdm import tqdm
from config import FASTF1_CACHE_DIR
from data_service import F1DataService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Ensure the FastF1 cache directory exists.
if not os.path.exists(FASTF1_CACHE_DIR):
    os.makedirs(FASTF1_CACHE_DIR)
    logger.info(f"Created cache directory: {FASTF1_CACHE_DIR}")

fastf1.Cache.enable_cache(FASTF1_CACHE_DIR)

def determine_session_type(session_name: str) -> str:
    if "Practice" in session_name:
        return "practice"
    elif "Qualifying" in session_name:
        return "qualifying"
    elif "Sprint" in session_name:
        return "sprint"
    elif "Race" in session_name:
        return "race"
    else:
        return "unknown"

def migrate_events(data_service: F1DataService, year: int) -> pd.DataFrame:
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
        existing = data_service.get_event(year, event_data["round_number"])
        if not existing:
            logger.info(f"Adding event: {event_data['event_name']}")
            # Implement data_service.create_event(event_data) if needed.
        else:
            logger.info(f"Event already exists: {event_data['event_name']}")
    return schedule

def migrate_sessions(data_service: F1DataService, schedule: pd.DataFrame, year: int) -> None:
    for idx, event in schedule.iterrows():
        event_record = data_service.get_event(year, int(event['RoundNumber']))
        if not event_record:
            logger.warning(f"Event not found for round {event['RoundNumber']}, skipping sessions")
            continue
        event_id = event_record['id']
        for i in range(1, 6):
            session_name = event.get(f"Session{i}")
            if pd.isna(session_name):
                continue
            session_date_utc = event.get(f"Session{i}DateUtc")
            session_data = {
                "event_id": event_id,
                "name": session_name,
                "date": session_date_utc.isoformat() if pd.notna(session_date_utc) else None,
                "session_type": determine_session_type(session_name)
            }
            # Implement data_service.create_session(session_data) if needed.
            logger.info(f"Processed session: {session_name} for event {event['EventName']}")

def main():
    parser = argparse.ArgumentParser(description="Migrate F1 data to SQLite")
    parser.add_argument("--year", type=int, required=True, help="Year to migrate")
    args = parser.parse_args()
    
    data_service = F1DataService()
    try:
        schedule = migrate_events(data_service, args.year)
        migrate_sessions(data_service, schedule, args.year)
        # Additional migration functions (drivers, teams, results, laps, telemetry, weather) can be added here.
    finally:
        data_service.close()

if __name__ == "__main__":
    main()