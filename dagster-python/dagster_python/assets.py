import pandas as pd
import requests
import time
from sqlalchemy import create_engine
from dagster import Output, asset

@asset
def raw_people():

    response = requests.get(url=f"https://swapi.dev/api/people/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))

@asset
def raw_films():

    response = requests.get(url=f"https://swapi.dev/api/films/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))


@asset
def raw_species():

    response = requests.get(url=f"https://swapi.dev/api/species/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))


@asset
def raw_planets():

    response = requests.get(url=f"https://swapi.dev/api/planets/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))


@asset
def raw_vehicles():

    response = requests.get(url=f"https://swapi.dev/api/vehicles/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))


@asset
def raw_starships():

    response = requests.get(url=f"https://swapi.dev/api/starships/")
    data = response.json()

    if not data['next']:
        return Output(pd.DataFrame(data['results']))

    results = data['results']
    while True:
        response = requests.get(data['next'])
        data = response.json()
        results.extend(data['results'])
        if not data['next']:
            break
        time.sleep(2)
    
    return Output(pd.DataFrame(results))
