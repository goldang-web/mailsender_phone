import subprocess
import time
import requests
import os
import json

def get_public_ipv4():
    """공인 IPv4 주소를 반환합니다."""
    try:
        response = requests.get("https://ipv4.icanhazip.com", timeout=10)
        return response.text.strip()
    except requests.RequestException:
        return None

def change_mobile_ip_at_phone(max_cycles=None):
    """모바일 아이피를 변경하는 함수

    Args:
        max_cycles (int, optional): 비행기 모드 토글을 반복할 최대 횟수.
            지정하지 않으면 성공할 때까지 계속 시도합니다.

    Returns:
        str | None: 성공 시 변경된 IP 주소, 제한 횟수를 초과하면 None
    """
    def toggle_airplane_mode(state):
        try:
            if state == 'on':
                subprocess.run(['su', '-c', 'settings put global airplane_mode_on 1'], 
                              check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.run(['su', '-c', 'am broadcast -a android.intent.action.AIRPLANE_MODE --ez state true'], 
                              check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif state == 'off':
                subprocess.run(['su', '-c', 'settings put global airplane_mode_on 0'], 
                              check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                subprocess.run(['su', '-c', 'am broadcast -a android.intent.action.AIRPLANE_MODE --ez state false'], 
                              check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"❌ 비행기 모드 전환 실패: {e}")

    def reset_data():
        try:
            print("🔄 모바일 데이터 리셋 중...")
            subprocess.run(["su", "-c", "svc data disable"], 
                          check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1)
            subprocess.run(["su", "-c", "svc data enable"], 
                          check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(1)
        except subprocess.CalledProcessError as e:
            print(f"❌ 모바일 데이터 리셋 실패: {e}")

    # 알박기 간격과 디바이스 이름 정보 가져오기
    albakgi_interval = 0
    device_name = "Unknown"
    
    try:
        # config.json에서 정보 가져오기
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "settings", "config.json")
        if os.path.exists(config_path):
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
                device_name = config.get('device_name', 'Unknown')
                albakgi_interval = config.get('albakgi_interval', 300)  # 알박기 간격 설정값 가져오기
    except Exception as e:
        print(f"정보 로드 오류: {e}")
    
    max_attempts = 15
    cycle = 0

    while True:
        cycle += 1
        print("🔄 비행기모드 전환중...")
        if cycle == 1:
            print(f"   📱 디바이스: {device_name}")
            print(f"   ⏱ 알박기 간격: {albakgi_interval}")
        else:
            print(f"   🔁 재시도 사이클: {cycle}")

        toggle_airplane_mode('on')
        time.sleep(3)
        toggle_airplane_mode('off')
        time.sleep(4)
        reset_data()
        print("🔄 모바일 IP 변경 시도 중...")

        for attempt in range(1, max_attempts + 1):
            new_ip = get_public_ipv4()
            if new_ip:
                print(f"변경된 IP: {new_ip}")
                return new_ip

            print(f"IP 확인 대기 중... ({attempt}/{max_attempts})")
            time.sleep(2)

        if max_cycles is not None and cycle >= max_cycles:
            print("❌ IP 확인 실패: 최대 비행기 모드 재시도 횟수를 초과했습니다.")
            return None

        print("⚠️ IP 확인 실패: 비행기 모드를 다시 토글합니다.")

if __name__ == "__main__":
    change_mobile_ip_at_phone()
    
    
