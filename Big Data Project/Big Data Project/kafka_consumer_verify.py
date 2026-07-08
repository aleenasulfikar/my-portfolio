# kafka_consumer_verify.py
# To verify the Kafka consumer is working correctly by reading from all 3 Kafka topics simultaneously and prints live statistics.

from kafka import KafkaConsumer
import json
import time
import threading
from collections import defaultdict
from datetime import datetime
   
# Shared stats — updated by consumer threads, read by print_stats thread
stats = defaultdict(lambda: {
    'count':       0,
    'last_key':    'N/A',
    'last_record': None,
    'start_time':  time.time(),
})
stats_lock = threading.Lock()
   
def consume_topic(topic_name: str) -> None:
    """Subscribing to a single Kafka topic and counting the messages."""
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['127.0.0.1:9092'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else 'UNKNOWN',
        auto_offset_reset='latest',    # only read NEW messages from now
        enable_auto_commit=True,
        group_id=f'verify-{topic_name}-{int(time.time())}',
        consumer_timeout_ms=300000,    # 5 mins
    )
    print(f'[Consumer] Connected to topic: {topic_name}')
    for msg in consumer:
        with stats_lock:
            stats[topic_name]['count'] += 1
            stats[topic_name]['last_key'] = msg.key or 'UNKNOWN'
            stats[topic_name]['last_record'] = msg.value
   
def print_stats() -> None:
    """To print a formatted stats table every 15 seconds."""
    while True:
        time.sleep(15)
        print()
        print('=' * 70)
        print(f'  LIVE PIPELINE STATS  —  {datetime.now().strftime("%H:%M:%S")}')
        print('=' * 70)
        with stats_lock:
            for topic in ['uae-flights', 'uae-weather', 'delay-alerts']:
                d = stats[topic]
                elapsed = time.time() - d['start_time']
                rate = d['count'] / max(elapsed, 1)
                print(f'  Topic: {topic}')
                print(f'    Messages received : {d["count"]:>8,}')
                print(f'    Rate              : {rate:>8.1f} msg/sec')
                print(f'    Last airport key  : {d["last_key"]}')
                r = d['last_record']
                if r:
                    if 'callsign' in r:
                        delay = r.get('delay_minutes', 0)
                        print(f'    Last flight       : {r.get("callsign","?")} '
                              f'{r.get("origin","?")} → {r.get("destination","?")} | '
                              f'delay: {delay:.1f} min | status: {r.get("delay_status","?")}')
                    elif 'sandstorm_index' in r:
                        print(f'    Last weather      : airport={r.get("airport","?")} | '
                              f'temp={r.get("temperature_2m","?")}°C | '
                              f'wind={r.get("wind_speed_10m","?")} km/h | '
                              f'sandstorm_index={r.get("sandstorm_index","?")} | '
                              f'alert={r.get("sandstorm_alert","?")}')
                    elif 'alert_type' in r:
                        print(f'    Last alert        : {r.get("alert_type")} at '
                              f'{r.get("airport","?")} | index={r.get("sandstorm_index")} | '
                              f'wind={r.get("wind_speed_10m")} km/h | PM10={r.get("pm10")} ug/m3')
                else:
                    print(f'    Waiting for messages...')
                print()
        print('=' * 70)
   
if __name__ == '__main__':
    topics = ['uae-flights', 'uae-weather', 'delay-alerts']
    # To start one consumer thread per topic
    for t in topics:
        thread = threading.Thread(
            target=consume_topic,
            args=(t,),
            name=f'Consumer-{t}',
            daemon=True
        )
        thread.start()
    # To start stats printer in background
    stats_thread = threading.Thread(target=print_stats, name='StatsPrinter', daemon=True)
    stats_thread.start()
    print(f'3 consumers started. Stats print every 15 sec. Ctrl+C to stop.')
    print(f'Tip: Open http://localhost:8090 to see messages in Kafka UI')
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print('\nVerification stopped.')