# Dockerfile: MailSender 서버 FastAPI 앱을 컨테이너로 패키징하기 위한 설정입니다.
# 공부를 위해 각 지시어가 무슨 의미인지 주석으로 자세히 남겼습니다.

# 1. 애플리케이션이 요구하는 Python 3.11 환경을 제공하는 슬림 이미지로 시작합니다.
FROM python:3.11-slim AS base

# 2. 운영 중 디버깅을 쉽게 하기 위해 표준 출력 버퍼링을 끄고,
#    pip 업데이트 체크를 비활성화해 빌드 속도를 높입니다.
ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 3. 컨테이너 내부에서 작업할 디렉터리를 /app으로 고정합니다.
WORKDIR /app

# 4. 빌드 캐시를 최대한 활용하기 위해 의존성 스펙 파일부터 복사합니다.
#    현재 프로젝트는 uv 프로젝트 구조이지만 pyproject.toml과 uv.lock에
#    패키지 목록이 들어 있으므로 두 파일을 먼저 복사합니다.
COPY pyproject.toml uv.lock ./ 

# 5. 시스템 패키지를 최소한 설치하고, 프로젝트 실행에 필요한 Python 패키지를 설치합니다.
#    - 빌드 도구 설치 후 즉시 제거하여 이미지 크기를 줄이는 단계를 포함합니다.
#    - requirements.txt 대신 pyproject.toml에 명시된 FastAPI, uvicorn 등을 직접 설치합니다.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && pip install --no-cache-dir fastapi>=0.118.2 jinja2>=3.1.6 python-multipart>=0.0.20 uvicorn>=0.37.0 \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

# 6. 나머지 애플리케이션 소스를 한 번에 복사합니다.
COPY . .

# 7. FastAPI 앱이 외부에서 접근할 포트를 문서화합니다.
EXPOSE 8000

# 8. 컨테이너가 시작되면 uvicorn을 통해 FastAPI 애플리케이션을 실행합니다.
#    --host 0.0.0.0: 모든 인터페이스 수신, --port 8000: 기본 포트.
CMD ["uvicorn", "server.main:app", "--host", "0.0.0.0", "--port", "8000"]
