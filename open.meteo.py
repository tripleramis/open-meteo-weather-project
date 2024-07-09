import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from geopy.geocoders import Nominatim
import sqlite3

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

# Function to get coordinates for a given location
def get_coordinates(location):
    geolocator = Nominatim(user_agent="weather_app")
    location = geolocator.geocode(location)
    return location.latitude, location.longitude

# Function to fetch weather data using coordinates
def fetch_weather_data(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": "temperature_2m",
        "hourly": "temperature_2m",
        "daily": "weather_code",
        "timezone": "Europe/Berlin"
    }
    responses = openmeteo.weather_api(url, params=params)
    return responses[0]

# Function to save data to SQLite database
def save_to_database(hourly_df, daily_df):
    conn = sqlite3.connect('weather_data.db')
    hourly_df.to_sql('hourly_weather', conn, if_exists='replace', index=False)
    daily_df.to_sql('daily_weather', conn, if_exists='replace', index=False)
    conn.close()

# Get user input for location
location = input("Enter a location: ")
latitude, longitude = get_coordinates(location)
response = fetch_weather_data(latitude, longitude)

# Print basic location info
print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation {response.Elevation()} m asl")
print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

# Current values
current = response.Current()
current_temperature_2m = current.Variables(0).Value()
print(f"Current time {current.Time()}")
print(f"Current temperature_2m {current_temperature_2m}")

# Process hourly data
hourly = response.Hourly()
hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
hourly_data = {"date": pd.date_range(
    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
    freq=pd.Timedelta(seconds=hourly.Interval()),
    inclusive="left"
)}
hourly_data["temperature_2m"] = hourly_temperature_2m
hourly_dataframe = pd.DataFrame(data=hourly_data)
print(hourly_dataframe)

# Process daily data
daily = response.Daily()
daily_weather_code = daily.Variables(0).ValuesAsNumpy()
daily_data = {"date": pd.date_range(
    start=pd.to_datetime(daily.Time(), unit="s", utc=True),
    end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
    freq=pd.Timedelta(seconds=daily.Interval()),
    inclusive="left"
)}
daily_data["weather_code"] = daily_weather_code
daily_dataframe = pd.DataFrame(data=daily_data)
print(daily_dataframe)

# Save data to database
save_to_database(hourly_dataframe, daily_dataframe)
