from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uvicorn
import os
from data_service import F1DataService

# Initialize application
app = FastAPI(
    title="F1 Data API",
    description="API for accessing Formula 1 data (both historical and live)",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development; restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dependency for data service
def get_data_service():
    service = F1DataService()
    try:
        yield service
    finally:
        service.close()

# Start live data polling on startup
@app.on_event("startup")
async def startup_event():
    service = F1DataService()
    service.start_live_polling()

# Metadata models
class YearModel(BaseModel):
    year: int

class EventModel(BaseModel):
    id: int
    round_number: int
    country: str
    location: str
    official_event_name: str
    event_name: str
    event_date: Optional[str]
    event_format: str
    f1_api_support: bool

class SessionModel(BaseModel):
    id: int
    name: str
    date: Optional[str]
    session_type: str
    total_laps: Optional[int]
    session_start_time: Optional[str]
    t0_date: Optional[str]

class TeamModel(BaseModel):
    id: int
    name: str
    team_id: str
    team_color: str

class DriverModel(BaseModel):
    id: int
    driver_number: str
    broadcast_name: str
    abbreviation: str
    driver_id: str
    first_name: str
    last_name: str
    full_name: str
    headshot_url: Optional[str]
    country_code: str
    team_id: int
    team_name: str
    team_color: str

class StandingModel(BaseModel):
    position: int
    driver_name: str
    abbreviation: str
    team: str
    team_color: str
    points: float

class ResultModel(BaseModel):
    position: Optional[int]
    driver_name: str
    abbreviation: str
    team: str
    grid_position: Optional[int]
    status: Optional[str]
    points: Optional[float]
    race_time: Optional[str]

class LapTimeModel(BaseModel):
    lap_number: int
    lap_time: str
    lap_time_sec: Optional[float]
    driver: str
    team_color: str
    sector1: Optional[str]
    sector2: Optional[str]
    sector3: Optional[str]
    compound: Optional[str]
    tyre_life: Optional[float]
    is_personal_best: bool

class TelemetryModel(BaseModel):
    session_time: str
    speed: Optional[float]
    rpm: Optional[float]
    gear: Optional[int]
    throttle: Optional[float]
    brake: Optional[bool]
    drs: Optional[int]
    x: Optional[float]
    y: Optional[float]

class WeatherModel(BaseModel):
    time: Optional[str]
    air_temp: Optional[float]
    humidity: Optional[float]
    pressure: Optional[float]
    rainfall: Optional[bool]
    track_temp: Optional[float]
    wind_direction: Optional[int]
    wind_speed: Optional[float]

class TireCompoundModel(BaseModel):
    name: str
    color: str

class SessionInfoModel(BaseModel):
    year: int
    round: int
    event_name: str
    session_name: str
    session_type: str
    start_time: Optional[str]
    end_time: Optional[str]
    is_live: bool

# API Routes
@app.get("/")
async def root():
    return {"message": "Welcome to the F1 Data API"}

@app.get("/years", response_model=List[int])
async def get_years(data_service: F1DataService = Depends(get_data_service)):
    years = data_service.get_available_years()
    return years

@app.get("/current", response_model=Optional[SessionInfoModel])
async def get_current_session(data_service: F1DataService = Depends(get_data_service)):
    session = data_service.get_current_session()
    if not session:
        return None
    return session

@app.get("/events/{year}", response_model=List[EventModel])
async def get_events(year: int, data_service: F1DataService = Depends(get_data_service)):
    events = data_service.get_events(year)
    return events

@app.get("/event/{year}/{round}", response_model=EventModel)
async def get_event(year: int, round: int, data_service: F1DataService = Depends(get_data_service)):
    event = data_service.get_event(year, round)
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return event

@app.get("/sessions/{event_id}", response_model=List[SessionModel])
async def get_sessions(event_id: int, data_service: F1DataService = Depends(get_data_service)):
    sessions = data_service.get_sessions(event_id)
    return sessions

@app.get("/teams/{year}", response_model=List[TeamModel])
async def get_teams(year: int, data_service: F1DataService = Depends(get_data_service)):
    teams = data_service.get_teams(year)
    return teams

@app.get("/drivers/{year}", response_model=List[DriverModel])
async def get_drivers(
    year: int, 
    team_id: Optional[int] = None, 
    data_service: F1DataService = Depends(get_data_service)
):
    drivers = data_service.get_drivers(year, team_id)
    return drivers

@app.get("/standings/drivers/{year}", response_model=List[Dict[str, Any]])
async def get_driver_standings(year: int, data_service: F1DataService = Depends(get_data_service)):
    standings = data_service.get_driver_standings(year)
    return standings

@app.get("/standings/constructors/{year}", response_model=List[Dict[str, Any]])
async def get_constructor_standings(year: int, data_service: F1DataService = Depends(get_data_service)):
    standings = data_service.get_constructor_standings(year)
    return standings

@app.get("/results/race/{session_id}", response_model=List[ResultModel])
async def get_race_results(session_id: int, data_service: F1DataService = Depends(get_data_service)):
    results = data_service.get_race_results(session_id)
    return results

@app.get("/results/qualifying/{session_id}", response_model=List[Dict[str, Any]])
async def get_qualifying_results(session_id: int, data_service: F1DataService = Depends(get_data_service)):
    results = data_service.get_qualifying_results(session_id)
    return results

@app.get("/laptimes/{session_id}", response_model=List[LapTimeModel])
async def get_lap_times(
    session_id: int, 
    driver: Optional[str] = None,
    data_service: F1DataService = Depends(get_data_service)
):
    lap_times = data_service.get_driver_lap_times(session_id, driver)
    return lap_times

@app.get("/telemetry/{session_id}/{driver_id}/{lap_number}", response_model=List[TelemetryModel])
async def get_telemetry(
    session_id: int,
    driver_id: int,
    lap_number: int,
    data_service: F1DataService = Depends(get_data_service)
):
    telemetry = data_service.get_telemetry(session_id, driver_id, lap_number)
    return telemetry

@app.get("/weather/{session_id}", response_model=List[WeatherModel])
async def get_weather(session_id: int, data_service: F1DataService = Depends(get_data_service)):
    weather = data_service.get_weather(session_id)
    return weather

@app.get("/tires/{year}", response_model=List[TireCompoundModel])
async def get_tire_compounds(year: int, data_service: F1DataService = Depends(get_data_service)):
    compounds = data_service.get_tire_compounds(year)
    return compounds

@app.get("/live/timing", response_model=List[Dict[str, Any]])
async def get_live_timing(data_service: F1DataService = Depends(get_data_service)):
    timing = data_service.get_live_timing()
    return timing

@app.get("/live/tires", response_model=Dict[str, Any])
async def get_live_tires(data_service: F1DataService = Depends(get_data_service)):
    tires = data_service.get_live_tires()
    return tires

@app.get("/live/status", response_model=Optional[Dict[str, Any]])
async def get_track_status(data_service: F1DataService = Depends(get_data_service)):
    status = data_service.get_track_status()
    return status

@app.get("/compare/{year}/{driver1}/{driver2}", response_model=List[Dict[str, Any]])
async def compare_drivers(
    year: int,
    driver1: str,
    driver2: str,
    data_service: F1DataService = Depends(get_data_service)
):
    comparison = data_service.get_compare_drivers(year, driver1, driver2)
    return comparison

if __name__ == "__main__":
    # Run server
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)