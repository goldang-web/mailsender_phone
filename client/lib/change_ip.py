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

def change_mobile_ip_at_phone():
    """모바일 아이피를 변경하는 함수

    Returns:
        str: 변경된 IP 주소
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
    
    # 비행기 모드 활성화 → 비활성화 → 데이터 리셋
    print("🔄 비행기모드 전환중...")
    print(f"   📱 디바이스: {device_name}")
    print(f"   ⏱ 알박기 간격: {albakgi_interval}")
    toggle_airplane_mode('on')
    time.sleep(3)
    toggle_airplane_mode('off')
    time.sleep(4)
    reset_data()
    print("🔄 모바일 IP 변경 시도 중...")

    # IP를 받아올 수 있을 때까지 최대 30초 대기
    max_attempts = 15
    attempt = 0
    while attempt < max_attempts:
        new_ip = get_public_ipv4()
        if new_ip:
            print(f"변경된 IP: {new_ip}")
            return new_ip
            
        print(f"IP 확인 대기 중... ({attempt + 1}/{max_attempts})")
        time.sleep(2)
        attempt += 1
    
    print("❌ IP 확인 실패: 시간 초과")
    return None

if __name__ == "__main__":
    change_mobile_ip_at_phone()
    
    
