# analysis.py — функции очистки данных и предсказания рейтинга
import re
import pandas as pd
import numpy as np
from math import radians, sin, cos, sqrt, atan2
from xgboost import XGBClassifier


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1 = float(lat1), float(lon1)
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def clean_scrapped_data(df: pd.DataFrame, city_centers: pd.DataFrame):
    categorial = ['city', 'state', 'type']
    for cat in categorial:
        df[cat] = df[cat].astype(str).str.lower()

    df["price"] = pd.to_numeric(df["price"], errors='coerce')
    df["area"] = pd.to_numeric(df["area"], errors='coerce')
    df['city'] = df['city'].str.replace("показать на карте", "").str.strip()

    states = [
        'не новый, но аккуратный ремонт',
        'требует ремонта',
        'черновая отделка',
        'свежий ремонт',
        'свободная планировка'
    ]
    df['state'] = df['state'].str.replace("'", "")

    def normalize_state(text):
        for s in states:
            if s in str(text):
                return s
        return text
    df['state'] = df['state'].apply(normalize_state)

    types = ['кирпичный', 'монолитный', 'панельный', 'иной']

    def normalize_type(text):
        for s in types:
            if s in str(text):
                return s
        return None
    df['type'] = df['type'].apply(normalize_type)

    df['dist'] = df.apply(
        lambda x: haversine(
            x['lat'], x['lon'],
            city_centers.loc[x['city'], 'lat_med'],
            city_centers.loc[x['city'], 'lon_med']
        ) if x['city'] in city_centers.index else 0,
        axis=1
    )
    df["cur_price_m"] = df["price"] / df["area"]
    df = df.drop(columns=[c for c in ["lat", "lon", "id", "price"] if c in df.columns])

    state_map = {
        'не новый, но аккуратный ремонт': 1,
        'требует ремонта': 2,
        'черновая отделка': 3,
        'свежий ремонт': 4,
        'свободная планировка': 5
    }
    df['state'] = df['state'].map(state_map).fillna(2)

    city_map = {
        'астана': 1, 'алматы': 2, 'костанай': 3, 'усть-каменогорск': 4,
        'атырау': 5, 'актобе': 6, 'тараз': 7, 'шымкент': 8, 'караганда': 9,
        'семей': 10, 'талдыкорган': 11, 'кокшетау': 12, 'актау': 13,
        'павлодар': 14, 'уральск': 15, 'петропавловск': 16, 'темиртау': 17
    }
    df['city'] = df['city'].map(city_map)

    types_map = {'кирпичный': 1, 'монолитный': 2, 'панельный': 3, 'иной': 4}
    df['type'] = df['type'].map(types_map).fillna(4)

    df['rating'] = 0

    return df


MODEL_FEATURE_COLS = [
    'city', 'bathrooms', 'height', 'state', 'type',
    'year', 'floor', 'max_floor', 'area', 'views',
    'dist', 'cur_price_m'
]

TYPES_DICT = {
    'cur_price_m': float,
    'area':        int,
    'year':        int,
    'floor':       float,
    'max_floor':   float,
    'views':       int,
    'height':      float,
    'city':        float,
    'bathrooms':   float,
    'state':       float,
    'type':        float,
    'dist':        float,
}


def model_predict(df: pd.DataFrame):
    model = XGBClassifier()
    model.load_model('apartment_rating_model.json')

    feature_df = df[MODEL_FEATURE_COLS].copy()

    for col, dtype in TYPES_DICT.items():
        if col in feature_df.columns:
            feature_df[col] = pd.to_numeric(feature_df[col], errors='coerce').fillna(0).astype(dtype)

    def predict_row(row):
        try:
            return int(model.predict(pd.DataFrame([row]))[0])
        except Exception:
            return 0

    df['rating'] = feature_df.apply(predict_row, axis=1)
    return df
