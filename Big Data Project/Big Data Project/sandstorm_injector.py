# sandstorm_injector.py
# To inject a simulated April 17 2024 UAE megastorm into the live Kafka stream.
   
from kafka import KafkaProducer
import json
import time
import logging
from datetime import datetime
   
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('SandstormInjector')
   
PRODUCER = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],
    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all',
)
   
# Real-world inspired conditions from the April 17 2024 UAE storm.
# (airport_icao, wind_km_h, pm10_ug_m3, visibility_km, description)
STORM_CONDITIONS = [
    ('OMDB', 62.4, 924.0, 0.28, 'Dubai DXB — extreme dust storm, flights diverted'),
    ('OMSJ', 58.7, 1180.0, 0.19, 'Sharjah SHJ — severe sandstorm, airport closed'),
    ('OMAA', 47.2, 682.0, 0.51, 'Abu Dhabi AUH — dust haze, delays expected'),
]
   
def compute_sandstorm_index(wind: float, pm10: float, vis: float) -> float:
    """Used the formula as in kafka_producer.py for consistency."""
    return round(
        (min(wind, 60) / 60 * 40) +
        (min(pm10, 200) / 200 * 40) +
        ((1 - min(vis, 10) / 10) * 20),
        2
    )
   
def inject_storm() -> None:
    """Injecting all the 3 airport storm records into Kafka."""
    log.info('=' * 60)
    log.info('  INJECTING SIMULATED STORM EVENT')
    log.info('  Source: UAE April 17 2024 Megastorm Data')
    log.info('=' * 60)
   
    for airport, wind, pm10, vis, desc in STORM_CONDITIONS:
        sandstorm_index = compute_sandstorm_index(wind, pm10, vis)
   
        # Full weather record
        weather_record = {
            'time':                 datetime.now().isoformat(),
            'airport':              airport,
            'temperature_2m':       38.5,
            'relative_humidity_2m': 13.0,
            'wind_speed_10m':       wind,
            'wind_gusts_10m':       round(wind * 1.45, 1),
            'wind_direction_10m':   218.0,
            'cloud_cover':          92.0,
            'precipitation':        0.0,
            'pm10':                 pm10,
            'pm2_5':                round(pm10 * 0.58, 1),
            'dust':                 round(pm10 * 0.82, 1),
            'carbon_monoxide':      280.0,
            'nitrogen_dioxide':     18.4,
            'visibility':           vis,
            'rh':                   13.0,
            'sandstorm_index':      sandstorm_index,
            'sandstorm_alert':      'HIGH_RISK',
            'stream_timestamp':     datetime.now().isoformat(),
            'source':               'INJECTED_STORM_EVENT',
        }
   
        # Alert record
        alert_record = {
            'alert_type':       'SANDSTORM_RISK',
            'airport':          airport,
            'sandstorm_index':  sandstorm_index,
            'wind_speed_10m':   wind,
            'pm10':             pm10,
            'pm2_5':            round(pm10 * 0.58, 1),
            'visibility':       vis,
            'time':             datetime.now().isoformat(),
            'alert_timestamp':  datetime.now().isoformat(),
            'description':      desc,
            'event_name':       'UAE_MEGASTORM_APR_2024',
        }
   
        # Publishing it to both the topics
        PRODUCER.send('uae-weather', key=airport, value=weather_record)
        PRODUCER.send('delay-alerts', key=airport, value=alert_record)
   
        log.warning(f'INJECTED: {desc}')
        log.warning(f'  sandstorm_index = {sandstorm_index}')
        log.warning(f'  wind = {wind} km/h | PM10 = {pm10} ug/m3 | visibility = {vis} km')
        time.sleep(0.3)
   
    PRODUCER.flush()
    log.info('Storm injection complete!')
    log.info('Check: Kafka UI → delay-alerts topic → Messages tab')
    log.info('Check: kafka_consumer_verify.py → should show 3 new HIGH_RISK alerts')
    log.info('Check: Grafana dashboard (once Member 4 configures it)')
   
if __name__ == '__main__':
    inject_storm()