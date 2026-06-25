print("""
╔════════════════════════════╗
║        SCRIPT LANCÉ        ║
╚════════════════════════════╝
""")


# API 
import requests as r
import pandas as pd
from pprint import pprint

# telechargement des mise a jour historique 
from datetime import datetime
from datetime import timedelta

# telechargement des donnes historique
from pathlib import Path

# evite le spam de requete / anti bot
import time
import random

# bar de chargement
from tqdm import tqdm
import threading


cities = [
    # Europe   lat      lon
    ("London", 51.5074, -0.1278),
    ("Paris", 48.8566, 2.3522),
    ("Madrid", 40.4168, -3.7038),
    ("Rome", 41.9028, 12.4964),
    ("Berlin", 52.5200, 13.4050),
    ("Moscow", 55.7558, 37.6176),
    ("Stockholm", 59.3293, 18.0686),

    # North America
    ("New York", 40.7128, -74.0060),
    ("Chicago", 41.8781, -87.6298),
    ("Los Angeles", 34.0522, -118.2437),
    ("Mexico City", 19.4326, -99.1332),
    ("Toronto", 43.6532, -79.3832),
    ("Anchorage", 61.2181, -149.9003),

    # South America
    ("Sao Paulo", -23.5505, -46.6333),
    ("Rio de Janeiro", -22.9068, -43.1729),
    ("Buenos Aires", -34.6037, -58.3816),
    ("Santiago", -33.4489, -70.6693),
    ("Lima", -12.0464, -77.0428),
    ("Bogota", 4.7110, -74.0721),

    # Africa
    ("Cairo", 30.0444, 31.2357),
    ("Casablanca", 33.5731, -7.5898),
    ("Lagos", 6.5244, 3.3792),
    ("Nairobi", -1.2921, 36.8219),
    ("Johannesburg", -26.2041, 28.0473),
    ("Cape Town", -33.9249, 18.4241),

    # Middle East
    ("Dubai", 25.2048, 55.2708),
    ("Riyadh", 24.7136, 46.6753),
    ("Tehran", 35.6892, 51.3890),
    ("Jerusalem", 31.7683, 35.2137),

    # South Asia
    ("Mumbai", 19.0760, 72.8777),
    ("Delhi", 28.6139, 77.2090),
    ("Karachi", 24.8607, 67.0011),
    ("Dhaka", 23.8103, 90.4125),
    ("Kathmandu", 27.7172, 85.3240),

    # East Asia
    ("Beijing", 39.9042, 116.4074),
    ("Shanghai", 31.2304, 121.4737),
    ("Tokyo", 35.6762, 139.6503),
    ("Seoul", 37.5665, 126.9780),
    ("Hong Kong", 22.3193, 114.1694),

    # Southeast Asia
    ("Bangkok", 13.7563, 100.5018),
    ("Singapore", 1.3521, 103.8198),
    ("Jakarta", -6.2088, 106.8456),
    ("Manila", 14.5995, 120.9842),

    # Oceania
    ("Sydney", -33.8688, 151.2093),
    ("Melbourne", -37.8136, 144.9631),
    ("Perth", -31.9505, 115.8605),
    ("Auckland", -36.8509, 174.7645),

    # Extreme climates
    ("Reykjavik", 64.1466, -21.9426),
    ("Ushuaia", -54.8019, -68.3030),
    ("Fairbanks", 64.8378, -147.7164)
]

end_date = (datetime.today()- timedelta(days=1)).strftime("%Y-%m-%d")
start_date =  (datetime.today() - timedelta(days=8)).strftime("%Y-%m-%d")


for city, lat, lon in cities:

    path = Path(f"hist_villes/{city}_hist_1960_2026.parquet")

    url_historique = (f"https://archive-api.open-meteo.com/v1/archive?"
                    f"latitude={lat}"
                    f"&longitude={lon}"
                    f"&start_date={start_date}"
                    f"&end_date={end_date}"
                    "&daily=weather_code,"
                    "temperature_2m_mean,"
                    "temperature_2m_min,"
                    "temperature_2m_max,"
                    "wind_speed_10m_max,"
                    "wind_gusts_10m_max,"
                    "rain_sum,snowfall_sum,"
                    "precipitation_hours,"
                    "apparent_temperature_mean,"
                    "apparent_temperature_max,"
                    "apparent_temperature_min,"
                    "shortwave_radiation_sum,"
                    "et0_fao_evapotranspiration,"
                    "cloud_cover_mean,"
                    "relative_humidity_2m_mean,"
                    "snowfall_water_equivalent_sum,"
                    "pressure_msl_mean,"
                    "surface_pressure_mean,"
                    "soil_temperature_0_to_100cm_mean")
    
    try:
        done = False

        def progress():
            with tqdm(total=5, desc=city, leave=False) as bar:
                while not done and bar.n < 5:
                    time.sleep(1)
                    bar.update(1)
                
                if done:
                    bar.n = bar.total
                    bar.refresh()
                    pprint("")

        
        threading.Thread(target=progress, daemon=True).start()
        
        response = r.get(url_historique, timeout=240)
        response.raise_for_status()

        maj = response.json()

        maj = pd.DataFrame(maj["daily"])
       
        hist = pd.read_parquet(f"hist_villes/{city}_hist_1960_2026.parquet")
        
        final = pd.concat([maj, hist], ignore_index=True)

        final = final.drop_duplicates(subset=["time"], keep="last")
        final = final.sort_values("time")

        final.to_parquet(path, index=False)

        pprint(f"✓ {city} up to date")

    except Exception as erreur:
        pprint(f"✗ {city} : {erreur}/n")
    
    finally:
        done = True

    time.sleep(3 + random.random())

print("""
╔════════════════════════════╗
║       SCRIPT TERMINÉ       ║
╚════════════════════════════╝
""")