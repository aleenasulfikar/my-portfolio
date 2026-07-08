# kafka_producer
# UAE Weather x Flight Delay Correlation Pipeline
# To replace DummyProducer with real Kafka infrastructure
   
import pandas as pd
import numpy as np
import json
import time
import threading
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
   
# Configure logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-12s] [%(levelname)-5s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('UAE-Producer')
   
# Airport codes (Kafka partition keys) 
# OMDB = Dubai International (DXB)
# OMAA = Abu Dhabi International (AUH)
# OMSJ = Sharjah International (SHJ)
UAE_AIRPORTS = {'OMDB', 'OMAA', 'OMSJ'}
   
# Serializers 
def json_serializer(data):
    """Serialize dict to JSON bytes for Kafka."""
    return json.dumps(data, default=str).encode('utf-8')
   
def key_serializer(key):
    """Serialize string key to bytes for Kafka."""
    return key.encode('utf-8') if key else b'UNKNOWN'
   
# Type cleaner 
def clean_record(record: dict) -> dict:
    """
    Converting numpy/pandas types to plain Python types.
    Kafka's JSON serializer cannot handle numpy.int64, numpy.float64,
    or pandas.Timestamp — this function converts them.
    """
    cleaned = {}
    for key, value in record.items():
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except (TypeError, ValueError):
            pass  
        if isinstance(value, np.integer):
            cleaned[key] = int(value)
        elif isinstance(value, np.floating):
            cleaned[key] = float(value)
        elif isinstance(value, pd.Timestamp):
            cleaned[key] = value.isoformat()
        elif isinstance(value, datetime):
            cleaned[key] = value.isoformat()
        else:
            cleaned[key] = value
    return cleaned
   
# Creating the Kafka producer
def create_producer(max_retries: int = 5) -> KafkaProducer:
    """
    Creating a KafkaProducer that's connected to localhost:9092.
    It uses localhost:9092 (external mapped port) not kafka:29092 (internal).
    Retries up to max_retries times with a 5-second delay.
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(f'Connecting to Kafka (attempt {attempt}/{max_retries})...')
            producer = KafkaProducer(
                bootstrap_servers=['127.0.0.1:9092'],
                value_serializer=json_serializer,
                key_serializer=key_serializer,
                acks='all',               # wait for full write acknowledgment
                retries=5,                # retry on transient failures
                linger_ms=20,             # batch small messages (20ms window)
                request_timeout_ms=30000,
                max_block_ms=60000,
            )
            log.info('Kafka producer connected successfully.')
            return producer
        except NoBrokersAvailable:
            log.warning(f'Kafka not available yet. Retrying in 5s...')
            time.sleep(5)
    raise RuntimeError('Could not connect to Kafka after max retries.')
   
# Load flight data
def load_flight_data() -> pd.DataFrame:
    """
    Loading and preparing the flight dataframe.
    Used only the 2026 JAN-FEB data for streaming.
    """
    log.info('Loading flight data from data/uae_flights_2026_JAN_FEB.csv...')
    df = pd.read_csv('data/uae_flights_2026_JAN_FEB.csv')
    log.info(f'Loaded {len(df):,} raw flight records')
   
    # 1: Removing timezone text 
    for col in ['firstseen_datetime', 'lastseen_datetime', 'flight_date']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace(' Asia/Muscat', '', regex=False)
            df[col] = pd.to_datetime(df[col], errors='coerce')
   
    # 2: Filling in missing callsign
    df['callsign'] = df['callsign'].fillna('UNKNOWN').str.strip()
   
    # 3: Filling missing origin/destination 
    for col in ['origin', 'destination']:
        df[col] = df[col].fillna(
            df[col].mode()[0] if not df[col].mode().empty else 'UNKNOWN'
        )
   
    # 4: Calculating the delay to compare actual duration to route median
    route_med = df.groupby(['origin', 'destination'])['duration_hours'].median()
    df['expected_duration'] = (
        df.set_index(['origin', 'destination']).index.map(route_med)
    )
    df['delay_minutes'] = (
        (df['duration_hours'] - df['expected_duration']) * 60
    ).fillna(0).round(2)
    df['delay_status'] = df['delay_minutes'].apply(
        lambda x: 'delayed' if x > 15 else 'on_time'
    )
   
    # 5: Feature engineering 
    df['flight_type'] = df['duration_hours'].apply(
        lambda x: 'long' if x > 6 else 'medium' if x > 3 else 'short'
    )
    df['day_of_week'] = df['flight_date'].dt.day_name()
    df['flight_month'] = df['flight_date'].dt.month_name()
   
    # 6: Sorting chronologically for realistic time-paced streaming
    df = df.sort_values('firstseen_datetime').reset_index(drop=True)
    log.info(f'Flight data ready: {len(df):,} records | Date range: {df["flight_date"].min()} to {df["flight_date"].max()}')
    return df
   
# Loading the weather data 
def load_weather_data() -> pd.DataFrame:
    """
    Loading and combining all weather sources:
    1. Open-Meteo hourly weather JSON (temperature, wind, cloud cover)
    2. Open-Meteo AQI JSON (pm10, pm2.5, dust, carbon monoxide, etc.)
    3. Iowa State ASOS METAR CSV (visibility, temp_f, relative humidity)
    Computing the Sandstorm Resilience Index 
    """
    log.info('Loading weather data...')
   
    # Helper to load Open-Meteo JSON files
    def load_meteo_json(path: str, airport: str) -> pd.DataFrame:
        with open(path, 'r') as f:
            raw = json.load(f)
        df = pd.DataFrame(raw['hourly'])
        df['airport'] = airport
        df['time'] = pd.to_datetime(df['time'])
        return df
   
    # Loading the hourly weather (temperature, wind, precipitation, cloud cover)
    weather = pd.concat([
        load_meteo_json('data/dubai hourly weather.json',       'OMDB'),
        load_meteo_json('data/sharjah hourly weather.json',     'OMSJ'),
        load_meteo_json('data/abu dhabi hourly weather.json',   'OMAA'),
    ], ignore_index=True)
    weather.drop(columns=['snowfall', 'uv_index'], errors='ignore', inplace=True)
    log.info(f'Weather records: {len(weather):,}')
   
    # Loading the AQI data (pm10, pm2.5, dust, nitrogen dioxide, ozone, etc.)
    aqi = pd.concat([
        load_meteo_json('data/dubai aqi.json',       'OMDB'),
        load_meteo_json('data/sharjah aqi.json',     'OMSJ'),
        load_meteo_json('data/abu dhabi aqi.json',   'OMAA'),
    ], ignore_index=True)
    aqi.drop(columns=['uv_index', 'uv_index_clear_sky'], errors='ignore', inplace=True)
    log.info(f'AQI records: {len(aqi):,}')
   
    # Helper to load METAR CSV files
    def load_metar(path: str, airport: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        df = df.rename(columns={
            'station': 'airport_raw',
            'valid':   'time',
            'tmpf':    'temp_f',
            'vsby':    'visibility',
            'relh':    'rh',
        })
        df['airport'] = airport
        df['time'] = pd.to_datetime(df['time'], errors='coerce')
        # Dropping the irrelevant METAR columns
        drop_cols = [
            'p01i','mslp','gust','skyc1','skyc2','skyc3','skyc4','sknt',
            'skyl1','skyl2','skyl3','skyl4','drct','ice_accretion_1hr',
            'ice_accretion_3hr','ice_accretion_6hr','peak_wind_gust',
            'peak_wind_drct','peak_wind_time','feel','snowdepth',
            'wxcodes','metar','dwpf','alti','dew_f','alt_in','airport_raw',
        ]
        df.drop(columns=drop_cols, errors='ignore', inplace=True)
        # Converting string columns (METAR uses 'M' for missing)
        for col in ['temp_f', 'visibility', 'rh']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                df[col] = df[col].fillna(df[col].median())
        return df
   
    metar = pd.concat([
        load_metar('data/DXB_METAR_2023_2026.csv', 'OMDB'),
        load_metar('data/SHJ_METAR_2023_2026.csv', 'OMSJ'),
        load_metar('data/AUH_METAR_2023_2026.csv', 'OMAA'),
    ], ignore_index=True)
    log.info(f'METAR records: {len(metar):,}')
   
    # Merging all three sources on time + airport
    combined = weather.merge(aqi, on=['time', 'airport'], how='inner')
    combined = combined.merge(metar, on=['time', 'airport'], how='inner')
    log.info(f'Combined weather records after merge: {len(combined):,}')
   
    # SANDSTORM RESILIENCE INDEX 
    # Formula weights (for UAE conditions):
    #  40% wind_speed  — high UAE wind carries and sustains dust clouds
    #  40% pm10        — dust particle concentration, primary visibility killer
    #  20% visibility  — direct flight safety metric
    #
    # Clipping: Wind capped at 60 km/h - (beyond which risk is maximal),
    #           pm10 capped at 200 ug/m3 - (WHO hazardous level),
    #           visibility capped at 10 km - (beyond which weather is clear)
    #
    # Score range: 0 (perfect conditions) to 100 (extreme storm)
    # Thresholds:  >60 = HIGH_RISK, >30 = MODERATE, <=30 = NORMAL
    combined['sandstorm_index'] = (
        (combined['wind_speed_10m'].clip(0, 60) / 60 * 40) +
        (combined['pm10'].clip(0, 200) / 200 * 40) +
        ((1 - combined['visibility'].clip(0, 10) / 10) * 20)
    ).round(2)
   
    combined['sandstorm_alert'] = combined['sandstorm_index'].apply(
        lambda x: 'HIGH_RISK' if x > 60 else 'MODERATE' if x > 30 else 'NORMAL'
    )
   
    # Counting and logging how many high-risk periods exist
    high_risk_count = (combined['sandstorm_alert'] == 'HIGH_RISK').sum()
    log.info(f'Sandstorm index computed. HIGH_RISK periods: {high_risk_count:,}')
   
    combined = combined.sort_values('time').reset_index(drop=True)
    return combined
   
# Flight streaming thread
def stream_flights(
    producer: KafkaProducer,
    df: pd.DataFrame,
    stop_event: threading.Event,
    speed_multiplier: int = 1800
) -> None:
    """
    Streaming flight records to 'uae-flights' Kafka topic.
   
    Time-paced streaming:
    speed_multiplier = 1800, means 1 real second = 1800 data seconds = 30 minutes.
    With 91,290 records spanning 2 months, the full dataset streams in ~2 hours. 
   
    Kafka KEY = destination airport ICAO code.
    Routeing all Dubai-bound flights to Partition 0,
    Abu Dhabi-bound to Partition 1, Sharjah-bound to Partition 2.
    """
    sent = 0
    errors = 0
    t0_real = time.time()
    first_valid_time = df['firstseen_datetime'].dropna().iloc[0]
    t0_data = first_valid_time.timestamp()
   
    log.info(f'[Flights] Starting. {len(df):,} records. Speed: {speed_multiplier}x')
    log.info(f'[Flights] First record: {first_valid_time}')
   
    for idx, row in df.iterrows():
        if stop_event.is_set():
            log.info('[Flights] Stop requested. Exiting thread.')
            break
   
        # Time-paced: calculating how long to wait before sending the record
        row_time = row['firstseen_datetime']
        if pd.notna(row_time):
            data_elapsed = row_time.timestamp() - t0_data
            real_elapsed = time.time() - t0_real
            wait = (data_elapsed / speed_multiplier) - real_elapsed
            if wait > 0:
                # Sleep in small increments so we can respond to stop_event
                for _ in range(int(wait / 0.1)):
                    if stop_event.is_set(): break
                    time.sleep(0.1)
   
        # Building the message
        record = clean_record(row.to_dict())
        record['stream_timestamp'] = datetime.now().isoformat()
        record['source'] = 'opensky_network'
   
        # KEY = destination airport (falls back to origin if missing)
        key = str(record.get('destination', record.get('origin', 'UNKNOWN'))).strip()
   
        try:
            producer.send('uae-flights', key=key, value=record)
            sent += 1
            if sent % 1000 == 0:
                delayed = record.get('delay_status', 'unknown')
                log.info(f'[Flights] Sent {sent:,} | Latest: {record.get("callsign","?")}'
                         f' {record.get("origin","?")}→{key} | Status: {delayed}'
                         f' | Delay: {record.get("delay_minutes",0):.1f} min')
        except KafkaError as e:
            errors += 1
            if errors <= 10:
                log.warning(f'[Flights] Send error on record {idx}: {e}')
   
    log.info(f'[Flights] Cycle complete. Sent: {sent:,}, Errors: {errors}. Restarting loop...')
    if not stop_event.is_set():
        stream_flights(producer, df, stop_event, speed_multiplier)
   
# Weather streaming thread 
def stream_weather(
    producer: KafkaProducer,
    df: pd.DataFrame,
    stop_event: threading.Event,
    speed_multiplier: int = 1800
) -> None:
    """
    Streaming weather+AQI+METAR records to 'uae-weather' Kafka topic.
    HIGH_RISK sandstorm records are also then published to 'delay-alerts' topic.
    Kafka KEY = airport ICAO code.
    """
    sent = 0
    alerts_sent = 0
    t0_real = time.time()
    t0_data = df['time'].iloc[0].timestamp()
   
    log.info(f'[Weather] Starting. {len(df):,} records. Speed: {speed_multiplier}x')
   
    for idx, row in df.iterrows():
        if stop_event.is_set():
            log.info('[Weather] Stop requested. Exiting thread.')
            break
   
        # Time-paced
        row_time = row['time']
        if pd.notna(row_time):
            wait = (row_time.timestamp()-t0_data)/speed_multiplier - (time.time()-t0_real)
            if wait > 0:
                for _ in range(int(wait / 0.1)):
                    if stop_event.is_set(): break
                    time.sleep(0.1)
   
        record = clean_record(row.to_dict())
        record['stream_timestamp'] = datetime.now().isoformat()
        key = str(record.get('airport', 'UNKNOWN'))
   
        # Sending it to the main weather topic
        try:
            producer.send('uae-weather', key=key, value=record)
            sent += 1
            if sent % 1000 == 0:
                log.info(f'[Weather] Sent {sent:,} | {key}'
                         f' | sandstorm_index: {record.get("sandstorm_index",0)}'
                         f' | alert: {record.get("sandstorm_alert","?")}')  
        except KafkaError as e:
            log.warning(f'[Weather] Send error on record {idx}: {e}')
   
        # Auto-publishing the HIGH_RISK events to delay-alerts topic
        if record.get('sandstorm_alert') == 'HIGH_RISK':
            alert = {
                'alert_type':      'SANDSTORM_RISK',
                'airport':          key,
                'sandstorm_index':  record.get('sandstorm_index'),
                'wind_speed_10m':   record.get('wind_speed_10m'),
                'pm10':             record.get('pm10'),
                'pm2_5':            record.get('pm2_5'),
                'visibility':       record.get('visibility'),
                'temperature_2m':   record.get('temperature_2m'),
                'time':             record.get('time'),
                'alert_timestamp':  datetime.now().isoformat(),
            }
            try:
                producer.send('delay-alerts', key=key, value=alert)
                alerts_sent += 1
                log.warning(f'[ALERT!] HIGH_RISK at {key}'
                            f' | index={alert["sandstorm_index"]}'
                            f' | wind={alert["wind_speed_10m"]} km/h'
                            f' | PM10={alert["pm10"]} ug/m3'
                            f' | vis={alert["visibility"]} km')
            except KafkaError as e:
                log.warning(f'[ALERT] Send error: {e}')
   
    log.info(f'[Weather] Cycle complete. Sent: {sent:,}, Alerts: {alerts_sent}. Restarting...')
    if not stop_event.is_set():
        stream_weather(producer, df, stop_event, speed_multiplier)
   
# Main entry point 
if __name__ == '__main__':
    log.info('=' * 65)
    log.info(' UAE FLIGHT DELAY PIPELINE — Kafka Producer')
    log.info(' Member 2: Streaming Pipeline Engineer')
    log.info(' Topics: uae-flights | uae-weather | delay-alerts')
    log.info('=' * 65)
   
    # Loading the data from disk
    flight_df = load_flight_data()
    weather_df = load_weather_data()
   
    # Connecting to Kafka
    producer = create_producer()
   
    # Creating a stop event for shutdown
    stop_event = threading.Event()
   
    # Starting both streams as background daemon threads
    flight_thread = threading.Thread(
        target=stream_flights,
        args=(producer, flight_df, stop_event),
        name='FlightStream',
        daemon=True
    )
    weather_thread = threading.Thread(
        target=stream_weather,
        args=(producer, weather_df, stop_event),
        name='WeatherStream',
        daemon=True
    )
   
    flight_thread.start()
    weather_thread.start()
    log.info('Both streams running. Press Ctrl+C to stop gracefully.')
   
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        log.info('Shutdown requested...')
        stop_event.set()
        producer.flush(timeout=15)
        producer.close()
        log.info('Producer closed cleanly. All done.')
