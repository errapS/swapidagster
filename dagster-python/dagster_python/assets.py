import pandas as pd
import requests
import time
from sqlalchemy import create_engine
from dagster import Output, asset

@asset
def raw_people():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/people/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_people', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_people', con=engine, if_exists='replace')

@asset
def raw_films():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/films/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_films', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_films', con=engine, if_exists='replace')


@asset
def raw_species():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/species/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_species', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_species', con=engine, if_exists='replace')


@asset
def raw_planets():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/planets/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_planets', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_planets', con=engine, if_exists='replace')


@asset
def raw_vehicles():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/vehicles/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_vehicles', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_vehicles', con=engine, if_exists='replace')

@asset
def raw_starships():

    engine = create_engine('postgresql://nqtjkmnw:RH3fWHJsX7zvQEYIGX_D921HINlHNY59@flora.db.elephantsql.com/nqtjkmnw')    

    response = requests.get(url=f"https://swapi.dev/api/starships/")
    data = response.json()

    if not data['next']:
        return pd.DataFrame(data['results']).to_sql('raw_starships', con=engine, if_exists='replace')

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return pd.DataFrame(results).to_sql('raw_starships', con=engine, if_exists='replace')
