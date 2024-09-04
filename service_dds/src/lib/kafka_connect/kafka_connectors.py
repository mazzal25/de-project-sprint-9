import json
import logging
from typing import Dict, Optional
from confluent_kafka import Consumer, Producer


def error_callback(err):
    print('Something went wrong: {}'.format(err))


# Настройка базового логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaProducer:
    def __init__(self, host: str, port: int, user: str, password: str, topic: str, cert_path: str) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': error_callback
        }

        # Логирование параметров подключения, исключая элементы, которые нельзя сериализовать
        serializable_params = {k: v if not callable(v) else str(v) for k, v in params.items()}
        logging.info(f'KafkaProducer initializing with params: {json.dumps(serializable_params, indent=2)}')

        self.topic = topic
        self.p = Producer(params)
        print(topic)
        logging.info(f'KafkaProducer topic: {topic}')

    def produce(self, payload: Dict) -> None:
        self.p.produce(self.topic, json.dumps(payload))
        self.p.flush(10)


class KafkaConsumer:
    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str
                 ) -> None:
        params = {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'group.id': group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
            'error_cb': error_callback,
            'debug': 'all',
            'client.id': 'someclientkey'
        }

        # Логирование параметров подключения, исключая элементы, которые нельзя сериализовать
        serializable_params = {k: v if not callable(v) else str(v) for k, v in params.items()}
        logging.info(f'KafkaConsumer initializing with params: {json.dumps(serializable_params, indent=2)}')

        self.topic = topic
        self.c = Consumer(params)
        self.c.subscribe([topic])
        print(topic)
        logging.info(f'KafkaConsumer topic: {topic}')

    def consume(self, timeout: float = 3.0) -> Optional[Dict]:
        msg = self.c.poll(timeout=timeout)
        if not msg:
            return None
        if msg.error():
            raise Exception(msg.error())
        val = msg.value().decode()
        return json.loads(val)
