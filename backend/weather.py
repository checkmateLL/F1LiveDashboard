import requests

def get_track_weather(latitude: float, longitude: float):
    """Fetch live track weather using Open-Meteo API."""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    response = requests.get(url)
    return response.json()
