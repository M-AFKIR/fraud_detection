import os
import sys
import json
from dotenv import load_dotenv
from confluent_kafka import Consumer
from PySide6.QtCore import QThread, Signal
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QTextEdit, QPushButton, QLineEdit
)

load_dotenv()


class KafkaWorker(QThread):
    """Background thread polling Confluent Cloud Kafka."""
    event_received = Signal(dict)
    status_changed = Signal(str)

    def __init__(self, topic: str):
        super().__init__()
        self.topic = topic
        self._running = True

    def run(self):
        conf = {
            "bootstrap.servers": os.getenv("BOOTSTRAP_SERVER"),
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": os.getenv("CONFLUENT_API_KEY"),
            "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
            "group.id": f"qt-raw-monitor-{os.getpid()}",
            "auto.offset.reset": "latest",
        }

        if not conf["bootstrap.servers"] or not conf["sasl.username"]:
            self.status_changed.emit("[ERROR] Missing KAFKA_BOOTSTRAP_SERVERS or KAFKA_API_KEY in .env")
            return

        consumer = Consumer(conf)
        consumer.subscribe([self.topic])
        self.status_changed.emit(f"[KAFKA] Subscribed to topic '{self.topic}'. Waiting for messages...")

        try:
            while self._running:
                msg = consumer.poll(timeout=0.5)
                if msg is None:
                    continue
                if msg.error():
                    self.status_changed.emit(f"[KAFKA ERROR] {msg.error()}")
                    continue

                raw_str = msg.value().decode("utf-8")
                try:
                    data = json.loads(raw_str)
                    self.event_received.emit(data)
                except json.JSONDecodeError:
                    self.status_changed.emit(f"[WARN] Non-JSON message: {raw_str}")
        finally:
            consumer.close()
            self.status_changed.emit("[KAFKA] Consumer closed.")

    def stop(self):
        self._running = False
        self.wait()


class RawKafkaDashboard(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Raw Qt Kafka Stream Monitor")
        self.resize(750, 520)

        self.bronze_count = 0
        self.silver_count = 0
        self.gold_count = 0

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Topic input + Start / Stop controls
        top_row = QHBoxLayout()
        top_row.addWidget(QLabel("Kafka Topic:"))
        self.topic_input = QLineEdit(os.getenv("SCORE_TOPIC", "scored-transactions"))
        self.start_btn = QPushButton("Connect")
        self.stop_btn = QPushButton("Disconnect")
        self.start_btn.clicked.connect(self.start_worker)
        self.stop_btn.clicked.connect(self.stop_worker)

        top_row.addWidget(self.topic_input)
        top_row.addWidget(self.start_btn)
        top_row.addWidget(self.stop_btn)
        layout.addLayout(top_row)

        # Alert status label
        self.alert_label = QLabel("ALERT STATUS: Disconnected")
        layout.addWidget(self.alert_label)

        # Bronze -> Silver -> Gold counters
        stage_row = QHBoxLayout()
        self.bronze_label = QLabel("Bronze: 0")
        self.silver_label = QLabel("Silver: 0")
        self.gold_label = QLabel("Gold: 0")

        stage_row.addWidget(self.bronze_label)
        stage_row.addWidget(QLabel("-->"))
        stage_row.addWidget(self.silver_label)
        stage_row.addWidget(QLabel("-->"))
        stage_row.addWidget(self.gold_label)
        layout.addLayout(stage_row)

        # Raw message log box
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        layout.addWidget(self.log_box)

        self.worker = None

    def start_worker(self):
        if self.worker is not None and self.worker.isRunning():
            return
        topic = self.topic_input.text().strip()
        if not topic:
            self.log_box.append("[ERROR] Please enter a topic name.")
            return

        self.worker = KafkaWorker(topic=topic)
        self.worker.event_received.connect(self.handle_event)
        self.worker.status_changed.connect(self.log_box.append)
        self.worker.start()

    def stop_worker(self):
        if self.worker is not None:
            self.worker.stop()
            self.worker = None

    def handle_event(self, data: dict):
        self.bronze_count += 1
        self.silver_count += 1
        self.gold_count += 1

        self.bronze_label.setText(f"Bronze: {self.bronze_count}")
        self.silver_label.setText(f"Silver: {self.silver_count}")
        self.gold_label.setText(f"Gold: {self.gold_count}")

        txn_id = data.get("id", "N/A")
        amount = data.get("amount_usd", 0.0)
        category = data.get("merchant_category", "N/A")
        prob = float(data.get("fraud_probability", 0.0))
        is_fraud = data.get("is_fraud_alert", prob >= 0.70)

        if is_fraud:
            self.alert_label.setText(
                f"!!! FRAUD ALERT !!! TXN #{txn_id} | ${amount} ({category}) | Prob: {prob:.4f}"
            )
            self.log_box.append(
                f"[FRAUD]  id={txn_id} | category={category} | amount=${amount} | prob={prob:.4f}"
            )
        else:
            self.alert_label.setText(f"ALERT STATUS: Normal (last TXN #{txn_id} OK)")
            self.log_box.append(
                f"[NORMAL] id={txn_id} | category={category} | amount=${amount} | prob={prob:.4f}"
            )

    def closeEvent(self, event):
        self.stop_worker()
        event.accept()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = RawKafkaDashboard()
    window.show()
    sys.exit(app.exec())