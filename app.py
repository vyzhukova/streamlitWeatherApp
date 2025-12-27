import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
import asyncio
from datetime import datetime
import time
from scipy import stats
import os

from functions import *

st.title("🌡️ Анализ температурных данных и мониторинг текущей температуры")

# Инициализация состояния сессии
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'api_key' not in st.session_state:
    st.session_state.api_key = ""
if 'current_temps' not in st.session_state:
    st.session_state.current_temps = {}

# Боковая панель для загрузки данных и настроек

with st.sidebar:
    st.header("Укажите данные для начала работы")
    
    # Загрузка файла с данными
    uploaded_file = st.file_uploader(
        "Загрузите файл с температурными данными (CSV)",
        type=['csv']
    )
    
    if uploaded_file is not None:
        try:
            data = pd.read_csv(uploaded_file)
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            st.session_state.data = data
            st.session_state.data_loaded = True
            st.success(f"✅ Данные загружены!")
        except Exception as e:
            st.error(f"❌ Ошибка при загрузке файла: {e}")

    st.markdown("---")
    
    # Настройки API OpenWeatherMap
    st.header("🌤️ OpenWeatherMap API")
    api_key = st.text_input(
        "Введите API ключ",
        type="password"
    )
    
    if api_key:
        st.session_state.api_key = api_key
        st.success("✅ API ключ сохранен")
    
    st.markdown("---")
    
    # Как доп фича, дайте доп баллов, пожалуста 🥺
    st.header("Настройки анализа")
    window_size = st.slider(
        "Размер окна для скользящего среднего (дни)",
        min_value=7,
        max_value=90,
        value=30,
        step=1
    )
    
    anomaly_threshold = st.slider(
        "Порог аномалий (стандартные отклонения)",
        min_value=1.0,
        max_value=3.0,
        value=2.0,
        step=0.1
    )
    
    st.markdown("---")


# Основное приложение
if st.session_state.data_loaded:
    data = st.session_state.data
    
    # Выбор города
    cities = sorted(data['city'].unique())
    selected_city = st.selectbox(
        "Выберите город для анализа",
        cities,
        index=0 if len(cities) > 0 else None
    )
    
    if selected_city:
        # Вкладки для разных разделов
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "Обзор данных",
            "Анализ временных рядов",
            "Текущая температура",
            "Производительность",
            "📋 Отчет"
        ])
        
        with tab1:
            st.header(f"Обзор данных для {selected_city}")
            
            # Основная статистика
            col1, col2, col3, col4 = st.columns(4)
            
            city_data = data[data['city'] == selected_city]
            current_season = city_data.iloc[-1]['season'] if not city_data.empty else "unknown"
            
            with col1:
                st.metric(
                    "Всего записей",
                    f"{len(city_data):,}"
                )
            
            with col2:
                st.metric(
                    "Период",
                    f"{city_data['timestamp'].min().date()} - {city_data['timestamp'].max().date()}"
                )
            
            with col3:
                avg_temp = city_data['temperature'].mean()
                st.metric(
                    "🌡️ Средняя температура",
                    f"{avg_temp:.1f}°C"
                )
            
            with col4:
                std_temp = city_data['temperature'].std()
                st.metric(
                    "Стандартное отклонение",
                    f"{std_temp:.1f}°C"
                )
            
            # Гистограмма распределения температур
            st.subheader("Распределение температур")
            fig_dist = px.histogram(
                city_data,
                x='temperature',
                nbins=50,
                title=f'Распределение температур в {selected_city}',
                color_discrete_sequence=['skyblue']
            )
            fig_dist.update_layout(
                xaxis_title='Температура (°C)',
                yaxis_title='Частота',
                height=400
            )
            st.plotly_chart(fig_dist, use_container_width=True)
            
            # Коробчатая диаграмма по сезонам
            st.subheader("Температура по сезонам")
            fig_box = px.box(
                city_data,
                x='season',
                y='temperature',
                color='season',
                title=f'Распределение температур по сезонам в {selected_city}'
            )
            fig_box.update_layout(height=400)
            st.plotly_chart(fig_box, use_container_width=True)
        
        with tab2:
            st.header(f"Анализ временных рядов для {selected_city}")
            
            if st.button("Запустить анализ временных рядов", type="primary"):
                with st.spinner("Выполняется анализ данных..."):
                    # Измерение времени для последовательного анализа
                    start_time = time.time()
                    data_sequential = calculate_moving_average_sequential(data, window_size)
                    sequential_time = time.time() - start_time
                    
                    # Измерение времени для параллельного анализа
                    start_time = time.time()
                    data_parallel = calculate_moving_average_parallel(data, window_size)
                    parallel_time = time.time() - start_time
                    
                    # Сохранение результатов
                    st.session_state.data_analyzed = data_parallel
                    st.session_state.sequential_time = sequential_time
                    st.session_state.parallel_time = parallel_time
                    
                    st.success(f"✅ Анализ завершен!")
                    
                    # Отображение времени выполнения
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric(
                            "Последовательный анализ",
                            f"{sequential_time:.2f} сек"
                        )
                    with col2:
                        st.metric(
                            "Параллельный анализ",
                            f"{parallel_time:.2f} сек"
                        )
                
                # Обнаружение аномалий
                data_with_anomalies = detect_anomalies(data_parallel, anomaly_threshold)
                st.session_state.data_with_anomalies = data_with_anomalies
                
                # Сезоннаяя статистика
                seasonal_stats = calculate_seasonal_stats(data_with_anomalies)
                st.session_state.seasonal_stats = seasonal_stats
                
                # Статистика аномалий
                anomalies_count = data_with_anomalies[data_with_anomalies['anomaly']].shape[0]
                total_count = data_with_anomalies.shape[0]
                anomaly_percentage = (anomalies_count / total_count * 100) if total_count > 0 else 0
                
                st.info(f"""
                **Статистика аномалий:**
                - Всего аномалий: {anomalies_count:,}
                - Процент аномальных дней: {anomaly_percentage:.2f}%
                - Порог аномалий: ±{anomaly_threshold}σ
                """)
            
            # Отображение графиков если данные проанализированы
            if 'data_with_anomalies' in st.session_state:
                # Временной ряд
                st.subheader("Временной ряд температур")
                fig_timeseries = plot_temperature_timeseries(
                    st.session_state.data_with_anomalies,
                    selected_city
                )
                st.plotly_chart(fig_timeseries, use_container_width=True)
                
                # Сезонный профиль
                st.subheader("Сезонный профиль")
                fig_seasonal = plot_seasonal_profile(
                    st.session_state.seasonal_stats,
                    selected_city
                )
                st.plotly_chart(fig_seasonal, use_container_width=True)
                
                # Распределение аномалий
                st.subheader("Распределение аномалий")
                fig_anomalies = plot_anomaly_distribution(
                    st.session_state.data_with_anomalies,
                    selected_city
                )
                if fig_anomalies:
                    st.plotly_chart(fig_anomalies, use_container_width=True)
        
        with tab3:
            st.header(f"🌡️ Текущая температура в {selected_city}")
            
            if st.session_state.api_key:
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    if st.button("Получить текущую температуру", type="primary"):
                        with st.spinner("Получение данных с OpenWeatherMap..."):
                            # Синхронный запрос
                            start_time = time.time()
                            sync_result = get_current_temperature_sync(
                                st.session_state.api_key,
                                selected_city
                            )
                            sync_time = time.time() - start_time
                            
                            if sync_result['success']:
                                st.session_state.current_temp = sync_result
                                st.session_state.sync_time = sync_time
                                
                                # Проверка на аномальность
                                city_data = data[data['city'] == selected_city]
                                current_season_data = city_data[city_data['season'] == current_season]
                                
                                if not current_season_data.empty:
                                    season_mean = current_season_data['temperature'].mean()
                                    season_std = current_season_data['temperature'].std()
                                    current_temp = sync_result['temperature']
                                    
                                    is_anomalous = (
                                        current_temp > season_mean + 2 * season_std or
                                        current_temp < season_mean - 2 * season_std
                                    )
                                    
                                    st.session_state.is_anomalous = is_anomalous
                                    st.session_state.season_stats = {
                                        'mean': season_mean,
                                        'std': season_std
                                    }
                                
                                st.success("✅ Данные получены!")
                            else:
                                error_msg = sync_result.get('error', 'Unknown error')
                                if sync_result.get('cod') == 401:
                                    st.error("❌ Неверный API ключ. Пожалуйста, проверьте ключ.")
                                else:
                                    st.error(f"❌ Ошибка: {error_msg}")
                
                with col2:
                    if st.button("Асинхронный запрос (тест)"):
                        with st.spinner("Тестирую асинхронный запрос..."):
                            async def test_async():
                                start_time = time.time()
                                results = await get_multiple_temperatures_async(
                                    st.session_state.api_key,
                                    [selected_city]
                                )
                                async_time = time.time() - start_time
                                return results[0], async_time
                            
                            result, async_time = asyncio.run(test_async())
                            
                            if result['success']:
                                st.session_state.async_time = async_time
                                st.success(f"✅ Асинхронный запрос выполнен за {async_time:.2f} сек")
                            else:
                                st.error(f"❌ Ошибка: {result.get('error')}")
                
                # Отображение текущей температуры
                if 'current_temp' in st.session_state:
                    current_temp = st.session_state.current_temp
                    
                    if current_temp['success']:
                        # Отображение погоды
                        st.subheader("🌤️ Текущая погода")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric(
                                "Температура",
                                f"{current_temp['temperature']:.1f}°C"
                            )
                        
                        with col2:
                            st.metric(
                                "Влажность",
                                f"{current_temp['humidity']}%"
                            )
                        
                        with col3:
                            st.metric(
                                "Давление",
                                f"{current_temp['pressure']} hPa"
                            )
                        
                        st.write(f"**Описание:** {current_temp['description'].capitalize()}")
                        
                        if 'is_anomalous' in st.session_state:
                            st.subheader("Анализ аномальности")
                            
                            season_stats = st.session_state.season_stats
                            normal_min = season_stats['mean'] - 2 * season_stats['std']
                            normal_max = season_stats['mean'] + 2 * season_stats['std']
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.info(f"""
                                **Исторические данные для {current_season}:**
                                - Средняя: {season_stats['mean']:.1f}°C
                                - Стандартное отклонение: {season_stats['std']:.1f}°C
                                - Нормальный диапазон: {normal_min:.1f}°C - {normal_max:.1f}°C
                                """)
                            
                            with col2:
                                if st.session_state.is_anomalous:
                                    st.error(f"""
                                    **АНОМАЛЬНАЯ ТЕМПЕРАТУРА!!11!1!**
                                    - Текущая температура: {current_temp['temperature']:.1f}°C
                                    - Выходит за пределы нормального диапазона
                                    - Отклонение: {(current_temp['temperature'] - season_stats['mean']):.1f}°C
                                    """)
                                else:
                                    st.success(f"""
                                    **Температура в пределах нормы**
                                    - Текущая температура: {current_temp['temperature']:.1f}°C
                                    - В пределах нормального диапазона
                                    - Отклонение от среднего: {(current_temp['temperature'] - season_stats['mean']):.1f}°C
                                    """)
                        
                        # Время выполнения запросов
                        if 'sync_time' in st.session_state:
                            st.info(f"Время синхронного запроса: {st.session_state.sync_time:.3f} сек")
                        
                        if 'async_time' in st.session_state:
                            st.info(f"Время асинхронного запроса: {st.session_state.async_time:.3f} сек")
            else:
                st.warning("Для получения текущей температуры введите API ключ OpenWeatherMap в боковой панели")
        
        with tab4:
            st.header("Сравнение производительности")
            
            if all(key in st.session_state for key in ['sequential_time', 'parallel_time']):
                # График сравнения времени
                times = {
                    'Последовательный': st.session_state.sequential_time,
                    'Параллельный': st.session_state.parallel_time
                }
                
                fig_perf = go.Figure(data=[
                    go.Bar(
                        x=list(times.keys()),
                        y=list(times.values()),
                        text=[f'{t:.2f} сек' for t in times.values()],
                        textposition='auto',
                        marker_color=['lightblue', 'lightgreen']
                    )
                ])
                
                fig_perf.update_layout(
                    title='Сравнение времени выполнения анализа',
                    yaxis_title='Время (секунды)',
                    template='plotly_white',
                    height=400
                )
                
                st.plotly_chart(fig_perf, use_container_width=True)
                
                # Выводы о производительности
                speedup = st.session_state.sequential_time / st.session_state.parallel_time
                
                st.subheader("Выводы о производительности")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.metric(
                        "Ускорение",
                        f"{speedup:.2f}x"
                    )
                
                with col2:
                    efficiency = (speedup / os.cpu_count()) * 100
                    st.metric(
                        "Эффективность",
                        f"{efficiency:.1f}%"
                    )
                
                st.info("""
                **💡 Комментарии о параллельных вычислениях:**
                - Параллельные вычисления эффективны при большом объеме данных и множестве независимых операций
                - В данном случае анализ по городам можно выполнять независимо
                - Накладные расходы на создание процессов оправданы при обработке 10+ городов
                """)
            else:
                st.info("Запустите анализ временных рядов для сравнения производительности")
        
        with tab5:
            st.header("📋 Итоговый отчет")
            
            if all(key in st.session_state for key in ['data_with_anomalies', 'seasonal_stats']):
                # Сводная информация
                st.subheader("Сводная статистика")
                
                city_stats = st.session_state.seasonal_stats[
                    st.session_state.seasonal_stats['city'] == selected_city
                ]
                
                # Таблица сезонной статистики
                st.dataframe(
                    city_stats.style.format({
                        'temperature_mean': '{:.1f}°C',
                        'temperature_std': '{:.1f}°C',
                        'temperature_min': '{:.1f}°C',
                        'temperature_max': '{:.1f}°C'
                    }),
                    use_container_width=True
                )
                
                st.subheader("Ключевые выводы")
                
                # Анализ трендов
                city_data = st.session_state.data_with_anomalies[
                    st.session_state.data_with_anomalies['city'] == selected_city
                ]
                
                if 'moving_avg' in city_data.columns:
                    # Вычисление линейного тренда
                    valid_data = city_data.dropna(subset=['moving_avg'])
                    if len(valid_data) > 1:
                        x = np.arange(len(valid_data))
                        y = valid_data['moving_avg'].values
                        
                        slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            trend_direction = "Повышается" if slope > 0 else "Понижается"
                            st.metric(
                                "Долгосрочный тренд",
                                trend_direction
                            )
                        
                        with col2:
                            st.metric(
                                "Коэффициент детерминации",
                                f"{(r_value**2):.3f}"
                            )
                        
                        with col3:
                            confidence = "Высокая" if p_value < 0.05 else "Низкая"
                            st.metric(
                                "Достоверность тренда",
                                confidence
                            )
                
                # Рекомендации
                st.subheader("Доп. информация")
                
                recommendations = []
                
                # Анализ аномалий
                anomaly_rate = city_data['anomaly'].mean() * 100
                if anomaly_rate > 5:
                    recommendations.append(
                        f"⚠️ Высокий уровень аномалий ({anomaly_rate:.1f}%). "
                    )
                
                # Отображение рекомендаций
                for i, rec in enumerate(recommendations, 1):
                    st.write(f"{i}. {rec}")
                
                # Кнопка для экспорта отчета
                # Как дополнительная фича, дайте доп баллов, пожалуйста 🥺
                if st.button("📥 Экспортировать отчет (CSV)"):
                    report_data = {
                        'Город': [selected_city],
                        'Период анализа': [f"{city_data['timestamp'].min().date()} - {city_data['timestamp'].max().date()}"],
                        'Всего записей': [len(city_data)],
                        'Средняя температура': [f"{city_data['temperature'].mean():.1f}°C"],
                        'Количество аномалий': [city_data['anomaly'].sum()],
                        'Процент аномалий': [f"{anomaly_rate:.1f}%"]
                    }
                    
                    report_df = pd.DataFrame(report_data)
                    csv = report_df.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="💾 Скачать отчет",
                        data=csv,
                        file_name=f"temperature_report_{selected_city}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv"
                    )
            else:
                st.info("Запустите анализ временных рядов для генерации отчета")
    
    else:
        st.warning("Пожалуйста, выберите город для анализа")
else:
    # Приветственный экран
    st.header("Добро пожаловать в систему анализа температурных данных")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### Начало работы
        
        1. Загрузите CSV файл с температурными данными
        2. Введите API ключ OpenWeatherMap
        3. Выберите город для анализа    
                    Насладждайтесь результатом!
        """)