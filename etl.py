import pandas as pd
from pandas import json_normalize
import requests as rq
import csv
import json

from pandas import json_normalize

CITY = "Moscow"
API_KEY = "cf57018e4cf19650f623b0afd26ce981"
URL = f'https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric'


def get_weather_data(URL):

    """
    Отправляем get-запрос и получаем json
    """

    try:
        response = rq.get(URL)
        response.raise_for_status()
        return response.json()
    except rq.exceptions.HTTPError as http_err:
        print(f"HTTP ошибка: {http_err}")
        raise
    except rq.exceptions.RequestException as req_err:
        print(f"Ошибка запроса: {req_err}")
        raise
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        raise

def extract_weather_data(data:dict) -> pd.DataFrame:

    row = {}

    data = (get_weather_data(URL))


    # df = json_normalize(data, sep='_')

    row = {
        "coord_lon": data["coord"]["lon"],
        "coord_lat": data["coord"]["lat"],
        "weather_id": data["weather"][0]["id"],          # первый элемент списка weather
        "weather_main": data["weather"][0]["main"],
        "weather_description": data["weather"][0]["description"],
        "temp": data["main"]["temp"],
        "pressure": data["main"]["pressure"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"],
        "wind_deg": data["wind"]["deg"],
        "city_name": data["name"],
        "sunrise": data["sys"]["sunrise"],
        "sunset": data["sys"]["sunset"]
    }

    return pd.DataFrame([row])


data = get_weather_data(URL)
data = extract_weather_data(data)
print(data)