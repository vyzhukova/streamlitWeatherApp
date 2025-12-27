from concurrent.futures import ProcessPoolExecutor
import pandas as pd
import plotly.graph_objects as go
import requests
import aiohttp
import asyncio
from datetime import datetime
import multiprocessing as mp

def process_city_for_parallel(args):
    """Функция для параллельной обработки одного города"""
    city_data, window_size = args
    city_data = city_data.sort_values('timestamp')
    city_data['moving_avg'] = city_data['temperature'].rolling(window=window_size, center=True).mean()
    city_data['moving_std'] = city_data['temperature'].rolling(window=window_size, center=True).std()
    return city_data

# Функции для анализа данных
def calculate_moving_average_parallel(data, window_size):
    """Вычисление скользящего среднего с использованием параллельных вычислений"""
    cities = data['city'].unique()
    args_list = [(data[data['city'] == city], window_size) for city in cities]
    
    with ProcessPoolExecutor(max_workers=min(len(cities), mp.cpu_count())) as executor:
        results = list(executor.map(process_city_for_parallel, args_list))
    
    return pd.concat(results)

def calculate_moving_average_sequential(data, window_size):
    """Вычисление скользящего среднего последовательно"""
    result_data = data.copy()
    
    for city in result_data['city'].unique():
        city_mask = result_data['city'] == city
        city_data = result_data[city_mask].sort_values('timestamp')
        
        result_data.loc[city_mask, 'moving_avg'] = city_data['temperature'].rolling(
            window=window_size, center=True
        ).mean().values
        
        result_data.loc[city_mask, 'moving_std'] = city_data['temperature'].rolling(
            window=window_size, center=True
        ).std().values
    
    return result_data

def detect_anomalies(data, threshold=2):
    """Обнаружение аномалий в температурных данных"""
    data = data.copy()
    data['anomaly'] = False
    
    for city in data['city'].unique():
        city_mask = data['city'] == city
        city_data = data[city_mask]
        
        if 'moving_avg' in city_data.columns and 'moving_std' in city_data.columns:
            valid_mask = ~city_data['moving_avg'].isna() & ~city_data['moving_std'].isna()
            
            # Аномалии: выходят за пределы moving_avg ± threshold * moving_std
            upper_bound = city_data.loc[valid_mask, 'moving_avg'] + threshold * city_data.loc[valid_mask, 'moving_std']
            lower_bound = city_data.loc[valid_mask, 'moving_avg'] - threshold * city_data.loc[valid_mask, 'moving_std']
            
            is_anomaly = (
                (city_data.loc[valid_mask, 'temperature'] > upper_bound) |
                (city_data.loc[valid_mask, 'temperature'] < lower_bound)
            )
            
            data.loc[city_mask & valid_mask, 'anomaly'] = is_anomaly.values
    
    return data

def calculate_seasonal_stats(data):
    """Вычисление сезонной статистики"""
    seasonal_stats = data.groupby(['city', 'season']).agg({
        'temperature': ['mean', 'std', 'min', 'max', 'count']
    }).round(2)
    
    seasonal_stats.columns = ['_'.join(col).strip() for col in seasonal_stats.columns.values]
    seasonal_stats = seasonal_stats.reset_index()
    
    return seasonal_stats

# Функции для работы с API
def get_current_temperature_sync(api_key, city):
    """Синхронный запрос текущей температуры через OpenWeatherMap API"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'ru'
    }
    
    try:
        response = requests.get(base_url, params=params, timeout=10)
        data = response.json()
        
        if response.status_code == 200:
            return {
                'success': True,
                'city': city,
                'temperature': data['main']['temp'],
                'feels_like': data['main']['feels_like'],
                'humidity': data['main']['humidity'],
                'pressure': data['main']['pressure'],
                'description': data['weather'][0]['description'],
                'icon': data['weather'][0]['icon'],
                'timestamp': datetime.now()
            }
        else:
            return {
                'success': False,
                'error': data.get('message', 'Unknown error'),
                'cod': data.get('cod')
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

async def get_current_temperature_async(session, api_key, city):
    """Асинхронный запрос текущей температуры"""
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    
    params = {
        'q': city,
        'appid': api_key,
        'units': 'metric',
        'lang': 'ru'
    }
    
    try:
        async with session.get(base_url, params=params, timeout=10) as response:
            data = await response.json()
            
            if response.status == 200:
                return {
                    'success': True,
                    'city': city,
                    'temperature': data['main']['temp'],
                    'feels_like': data['main']['feels_like'],
                    'humidity': data['main']['humidity'],
                    'pressure': data['main']['pressure'],
                    'description': data['weather'][0]['description'],
                    'icon': data['weather'][0]['icon'],
                    'timestamp': datetime.now()
                }
            else:
                return {
                    'success': False,
                    'error': data.get('message', 'Unknown error'),
                    'cod': data.get('cod')
                }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }

async def get_multiple_temperatures_async(api_key, cities):
    """Асинхронный запрос температуры для нескольких городов"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for city in cities:
            task = get_current_temperature_async(session, api_key, city)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        return results

# Функции для визуализации
def plot_temperature_timeseries(data, city):
    """Построение временного ряда температур"""
    city_data = data[data['city'] == city].sort_values('timestamp')
    
    fig = go.Figure()
    
    # Основной временной ряд
    fig.add_trace(go.Scatter(
        x=city_data['timestamp'],
        y=city_data['temperature'],
        mode='lines',
        name='Температура',
        line=dict(color='lightblue', width=1),
        opacity=0.7
    ))
    
    # Скользящее среднее
    if 'moving_avg' in city_data.columns:
        fig.add_trace(go.Scatter(
            x=city_data['timestamp'],
            y=city_data['moving_avg'],
            mode='lines',
            name='Скользящее среднее (30 дней)',
            line=dict(color='red', width=2)
        ))
    
    # Аномалии
    if 'anomaly' in city_data.columns:
        anomalies = city_data[city_data['anomaly']]
        if not anomalies.empty:
            fig.add_trace(go.Scatter(
                x=anomalies['timestamp'],
                y=anomalies['temperature'],
                mode='markers',
                name='Аномалии',
                marker=dict(
                    color='red',
                    size=8,
                    symbol='circle-open'
                )
            ))
    
    fig.update_layout(
        title=f'Временной ряд температур для {city}',
        xaxis_title='Дата',
        yaxis_title='Температура (°C)',
        hovermode='x unified',
        template='plotly_white',
        height=500
    )
    
    return fig

def plot_seasonal_profile(seasonal_stats, city):
    """Построение сезонного профиля"""
    city_stats = seasonal_stats[seasonal_stats['city'] == city]
    
    fig = go.Figure()
    
    # Определение порядка сезонов
    season_order = ['winter', 'spring', 'summer', 'autumn']
    city_stats['season'] = pd.Categorical(city_stats['season'], categories=season_order, ordered=True)
    city_stats = city_stats.sort_values('season')
    
    # Средние температуры
    fig.add_trace(go.Bar(
        x=city_stats['season'],
        y=city_stats['temperature_mean'],
        name='Средняя температура',
        marker_color='skyblue',
        text=city_stats['temperature_mean'].round(1),
        textposition='auto'
    ))
    
    # Ошибки (стандартное отклонение)
    fig.add_trace(go.Scatter(
        x=city_stats['season'],
        y=city_stats['temperature_mean'] + city_stats['temperature_std'],
        mode='lines',
        line=dict(width=0),
        showlegend=False,
        hoverinfo='skip'
    ))
    
    fig.add_trace(go.Scatter(
        x=city_stats['season'],
        y=city_stats['temperature_mean'] - city_stats['temperature_std'],
        mode='lines',
        line=dict(width=0),
        fill='tonexty',
        fillcolor='rgba(135, 206, 235, 0.3)',
        name='±1 стандартное отклонение',
        hoverinfo='skip'
    ))
    
    fig.update_layout(
        title=f'📊 Сезонный профиль температур для {city}',
        xaxis_title='Сезон',
        yaxis_title='Температура (°C)',
        template='plotly_white',
        height=400
    )
    
    return fig

def plot_anomaly_distribution(data, city):
    """Распределение аномалий по годам"""
    city_data = data[data['city'] == city].copy()
    city_data['year'] = city_data['timestamp'].dt.year
    
    if 'anomaly' not in city_data.columns:
        return None
    
    anomaly_counts = city_data[city_data['anomaly']].groupby('year').size()
    total_counts = city_data.groupby('year').size()
    anomaly_percentages = (anomaly_counts / total_counts * 100).fillna(0)
    
    fig = go.Figure(data=[
        go.Bar(
            x=anomaly_percentages.index,
            y=anomaly_percentages.values,
            text=[f'{p:.1f}%' for p in anomaly_percentages.values],
            textposition='auto',
            marker_color='coral'
        )
    ])
    
    fig.update_layout(
        title=f'📊 Процент аномалий по годам для {city}',
        xaxis_title='Год',
        yaxis_title='Процент аномальных дней (%)',
        template='plotly_white',
        height=400
    )
    
    return fig
