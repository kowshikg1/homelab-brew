from pathlib import Path

import paho.mqtt.client as mqtt

from src.handlers.env_manager import EnvManager
from src.handlers.telegram import send_message
from src.utils.log_util import get_logger
from src.utils.path_variables import ENV_FILE_HANDLERS

log = get_logger(Path(__file__).stem)

subscribed_topics = ["home/alerts", "jellyfin/webhooks"]


def _build_mqtt_client() -> mqtt.Client:
	env_manager = EnvManager(ENV_FILE_HANDLERS)

	broker = env_manager.get("MQTT_BROKER", "localhost")
	port = int(env_manager.get("MQTT_PORT", "1883"))
	username = env_manager.get("MQTT_USERNAME")
	password = env_manager.get("MQTT_PASSWORD")

	client = mqtt.Client()

	if username:
		client.username_pw_set(username=username, password=password)

	def on_connect(mqtt_client, _userdata, _flags, rc):
		if rc != 0:
			log.error("Failed to connect to MQTT broker with result code %s", rc)
			return

		for topic in subscribed_topics:
			mqtt_client.subscribe(topic)
			log.info("Subscribed to topic: %s", topic)

	def on_message(_mqtt_client, _userdata, msg):
		payload = msg.payload.decode("utf-8", errors="replace")
		telegram_message = f"📩 *{msg.topic}*\n └─ {payload}"
		log.info("Received MQTT message on %s, payload: %s", msg.topic, payload)
		send_message(telegram_message)

	def on_disconnect(_mqtt_client, _userdata, rc):
		if rc != 0:
			log.warning("Unexpected MQTT disconnection (rc=%s)", rc)
		else:
			log.info("Disconnected from MQTT broker")

	client.on_connect = on_connect
	client.on_message = on_message
	client.on_disconnect = on_disconnect

	client.connect(broker, port, 60)
	log.info("Connecting to MQTT broker %s:%s", broker, port)
	return client


def run() -> None:
	client = _build_mqtt_client()
    # client.loop_start()
	client.loop_forever()


if __name__ == "__main__":
	run()




