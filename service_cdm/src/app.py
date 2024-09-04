import logging
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor
from cdm_loader.repository import CdmRepository

app = Flask(__name__)

# Заводим endpoint для проверки, поднялся ли сервис.
# Обратиться к нему можно будет GET-запросом по адресу localhost:5000/health.
# Если в ответе будет healthy - сервис поднялся и работает.
@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    # Устанавливаем уровень логгирования в Debug, чтобы иметь возможность просматривать отладочные логи.
    app.logger.setLevel(logging.DEBUG)

    # Инициализируем конфиг. Для удобства, вынесли логику получения значений переменных окружения в отдельный класс.
    config = AppConfig()

    # Создаем необходимые зависимости
    kafka_consumer = config.kafka_consumer()
    #kafka_producer = config.kafka_producer()
    pg_connection = config.pg_warehouse_db()

    # Создаем репозиторий
    cdm_repository = CdmRepository(pg_connection)

    # Инициализируем процессор сообщений, передавая все необходимые параметры.
    proc = CdmMessageProcessor(
        kafka_consumer,
    #    kafka_producer,
        cdm_repository,
        100,  # размер батча
        app.logger
    )

    # Запускаем процессор в бэкграунде.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    # стартуем Flask-приложение.
    app.run(debug=True, host='0.0.0.0', use_reloader=False)
