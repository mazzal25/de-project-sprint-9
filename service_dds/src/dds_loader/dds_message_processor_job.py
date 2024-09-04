import json
from logging import Logger, getLogger, StreamHandler, Formatter
from typing import List, Dict
from datetime import datetime
from uuid import UUID, uuid5
import time
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        # Цикл обработки сообщений
        for _ in range(self._batch_size):
            try:
                msg = self._consumer.consume()
                if not msg:
                    self._logger.warning(f"{datetime.utcnow()}: Empty message received, skipping...")
                    continue

                # Разобор сообщение как JSON
                try:
                    order = msg['payload']
                except json.decoder.JSONDecodeError as e:
                    self._logger.error(f"{datetime.utcnow()}: Invalid JSON message received: {e}, skipping...")
                    continue

                self._logger.info(f"{datetime.utcnow()}: Message received")

                load_src_var = "orders-stg-kf"



                self._dds_repository.dds_h_user_insert(
                    self._uuid_gen(order["user"]["id"]),
                    order["user"]["id"],
                    datetime.utcnow(),
                    load_src_var
                )

                # h_product
                for product in order['products']:
                    self._dds_repository.dds_h_product_insert(
                        self._uuid_gen(product["id"]),
                        product["id"],
                        datetime.utcnow(),
                        load_src_var
                    )

                # h_category
                for product in order['products']:
                    self._dds_repository.dds_h_category_insert(
                        self._uuid_gen(product["category"]),
                        product["category"],
                        datetime.utcnow(),
                        load_src_var
                    )

                # h_restaurant
                self._dds_repository.dds_h_restaurant_insert(
                    self._uuid_gen(order["restaurant"]["id"]),
                    order["restaurant"]["id"],
                    datetime.utcnow(),
                    load_src_var
                )

                # h_order
                self._dds_repository.dds_h_order_insert(
                    self._uuid_gen(str(order["id"])),
                    order["id"],
                    order["date"],
                    datetime.utcnow(),
                    load_src_var
                )



                # l_order_product
                for product in order['products']:
                    self._dds_repository.dds_l_order_product_insert(
                        self._uuid_gen(f'{order["id"]}{product["id"]}'),
                        self._uuid_gen(str(order["id"])),
                        self._uuid_gen(product["id"]),
                        datetime.utcnow(),
                        load_src_var
                    )

                # l_product_restaurant
                for product in order['products']:
                    self._dds_repository.dds_l_product_restaurant_insert(
                        self._uuid_gen(f'{product["id"]}{order["restaurant"]["id"]}'),
                        self._uuid_gen(product["id"]),
                        self._uuid_gen(order["restaurant"]["id"]),
                        datetime.utcnow(),
                        load_src_var
                    )

                # l_product_category
                for product in order['products']:
                    self._dds_repository.dds_l_product_category_insert(
                        self._uuid_gen(f'{product["id"]}{product["category"]}'),
                        self._uuid_gen(product["id"]),
                        self._uuid_gen(product["category"]),
                        datetime.utcnow(),
                        load_src_var
                    )

                # l_order_user
                self._dds_repository.dds_l_order_user_insert(
                    self._uuid_gen(f'{order["id"]}{order["user"]["id"]}'),
                    self._uuid_gen(str(order["id"])),
                    self._uuid_gen(order["user"]["id"]),
                    datetime.utcnow(),
                    load_src_var
                )

                # s_user_names
                self._dds_repository.dds_s_user_names_insert(
                    self._uuid_gen(order["user"]["id"]),
                    order["user"]["name"],
                    order["user"]["login"],
                    datetime.utcnow(),
                    load_src_var,
                    self._uuid_gen(order["user"]["name"])
                )

                # s_product_names
                for product in order['products']:
                    self._dds_repository.dds_s_product_names_insert(
                        self._uuid_gen(product["id"]),
                        product["name"],
                        datetime.utcnow(),
                        load_src_var,
                        self._uuid_gen(product["name"])
                    )

                # s_restaurant_names
                self._dds_repository.dds_s_restaurant_names_insert(
                    self._uuid_gen(order["restaurant"]["id"]),
                    order["restaurant"]["name"],
                    datetime.utcnow(),
                    load_src_var,
                    self._uuid_gen(order["restaurant"]["name"])
                )

                # s_order_cost
                self._dds_repository.dds_s_order_cost_insert(
                    self._uuid_gen(str(order["id"])),
                    order["cost"],
                    order["payment"],
                    datetime.utcnow(),
                    load_src_var,
                    self._uuid_gen(str(order["cost"]))
                )

                # s_order_status
                self._dds_repository.dds_s_order_status_insert(
                    self._uuid_gen(str(order["id"])),
                    order["status"],
                    datetime.utcnow(),
                    load_src_var,
                    self._uuid_gen(order["status"])
                )

                # Создание сообщения для топика dds-service_orders
                dst_msg = {
                    "object_id": order["user"]["id"],
                    "object_type": "order",
                    "payload": {
                        "user": order['user'],
                        "products": order['products']
                    }
                }

                # Отправка сообщения в топик
                self._producer.produce(dst_msg)

                self._logger.info(f"{datetime.utcnow()}. Message Sent")

            except Exception as e:
                self._logger.error(f"{datetime.utcnow()}: Error during message processing: {e}")

        self._logger.info(f"{datetime.utcnow()}: FINISH")


    def _uuid_gen(self, keygen):
        return uuid5(UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307'), keygen)
