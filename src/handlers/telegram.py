import requests

from pathlib import Path

from src.handlers.env_manager import EnvManager
from src.utils.path_variables import ENV_FILE_HANDLERS
from src.utils.log_util import get_logger

log = get_logger(Path(__file__).stem)
BASE_URL = "https://api.telegram.org"

def send_message(message: str, chat_id: str = None) -> None:
    env_manager = EnvManager(ENV_FILE_HANDLERS)
    bot_token = env_manager.get("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or env_manager.get("TELEGRAM_CHAT_ID")

    url = f"{BASE_URL}/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        'parse_mode': "Markdown",
    }

    response = requests.post(url, json=payload)
    if response.status_code != 200:
        log.error(f"Failed to send telegram message: {response.text}")


def get_chat_id() -> str:
    env_manager = EnvManager(ENV_FILE_HANDLERS)
    bot_token = env_manager.get("TELEGRAM_BOT_TOKEN")
    url = f"{BASE_URL}/bot{bot_token}/getUpdates"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data["ok"] and data["result"]:
            chat_id = data["result"][0]["message"]["chat"]["id"]
            return str(chat_id)
    else:
        log.error(f"Failed to get telegram chat id: {response.text}")
    return None

if __name__ == "__main__":
    # chat_id = get_chat_id()
    # print(f"Chat ID: {chat_id}")
    send_message("Hello from the bot!")