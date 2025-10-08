# -*- coding: euc-kr -*-
import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict

from agent import run_agent

CONFIG_PATH = Path(__file__).resolve().parent / "settings.json"


def clear_screen() -> None:
    command = "cls" if os.name == "nt" else "clear"
    os.system(command)


DEFAULT_CONFIG: Dict[str, Any] = {
    "server_url": "http://127.0.0.1:8000",
    "device_name": "TermuxPhone",
    "device_key": "",
    "interval": 5,
    "timeout": 15,
}


def generate_device_key(name: str) -> str:
    base = (name or '').strip()
    if not base:
        base = 'device'
    sanitized = ''.join(ch if ch.isalnum() else '-' for ch in base)
    sanitized = sanitized.replace('_', '-')
    parts = [part.lower() for part in sanitized.split('-') if part]
    core = '-'.join(parts) or 'device'
    suffix = uuid.uuid4().hex[:8]
    return f"{core}-{suffix}"


def load_config() -> Dict[str, Any]:
    if CONFIG_PATH.exists():
        try:
            text = CONFIG_PATH.read_text(encoding="euc-kr")
            data = json.loads(text or "{}")
            base = DEFAULT_CONFIG.copy()
            base.update(data)
            if not base.get("device_key") and base.get("device_id"):
                base["device_key"] = str(base.get("device_id"))
            base.pop("device_id", None)
            base.setdefault("device_key", "")
            return base
        except (OSError, ValueError):
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> None:
    payload = config.copy()
    payload.pop("device_id", None)
    CONFIG_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="euc-kr",
    )


def input_with_default(prompt: str, current: Any) -> str:
    base = f"{prompt} (현재: {current})> "
    return input(base).strip()


def ensure_required(config: Dict[str, Any]) -> Dict[str, Any]:
    updated = False
    if not config.get("server_url"):
        config["server_url"] = input("서버 주소를 입력하세요 (예: http://127.0.0.1:8000)> ").strip()
        updated = True
    if not config.get("device_name"):
        config["device_name"] = input("디바이스 이름을 입력하세요> ").strip()
        updated = True
    if not config.get("device_key") and config.get("device_name"):
        config["device_key"] = generate_device_key(config["device_name"])
        updated = True
    if updated:
        save_config(config)
    return config


def show_menu(config: Dict[str, Any]) -> None:
    clear_screen()
    print("======================")
    print(" 다중 디바이스 메일 에이전트")
    print("======================")
    print(f"1. 연결 시작 (현재 서버: {config['server_url']})")
    print("2. 서버 주소 설정")
    print(f"3. 디바이스 이름 변경 (현재: {config['device_name']})")
    print("0. 종료")


def main() -> None:
    config = load_config()
    while True:
        show_menu(config)
        choice = input("선택> ").strip()
        if choice == "1":
            ensure_required(config)
            save_config(config)
            try:
                run_agent(
                    server_url=config["server_url"],
                    device_key=config.get("device_key") or generate_device_key(config["device_name"]),
                    device_name=config["device_name"],
                    interval=int(config.get("interval", 5)),
                    timeout=int(config.get("timeout", 15)),
                )
            except KeyboardInterrupt:
                print("\n연결을 종료했습니다.")
            input("계속하려면 Enter 키를 누르세요...")
        elif choice == "2":
            new_value = input_with_default("서버 주소를 입력하세요", config["server_url"])
            if new_value:
                config["server_url"] = new_value
                save_config(config)
        elif choice == "3":
            new_name = input_with_default("디바이스 이름을 입력하세요", config["device_name"])
            if new_name:
                config["device_name"] = new_name
                if not config.get("device_key"):
                    config["device_key"] = generate_device_key(new_name)
                save_config(config)
        elif choice in {"0", "q", "Q"}:
            print("종료합니다.")
            break
        else:
            print("알 수 없는 선택입니다. 다시 입력하세요.")
            input("계속하려면 Enter 키를 누르세요...")


if __name__ == "__main__":
    main()
