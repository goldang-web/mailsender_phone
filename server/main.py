# -*- coding: euc-kr -*-
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, model_validator


class EUCKRJSONResponse(Response):
    media_type = "application/json; charset=euc-kr"

    def render(self, content) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            default=self._serialize_helper,
        ).encode("euc-kr", errors="ignore")

    @staticmethod
    def _serialize_helper(value):
        if isinstance(value, datetime):
            return value.isoformat()
        raise TypeError(f"지원되지 않는 타입: {type(value)}")


class EUCKRHTMLResponse(Response):
    media_type = "text/html; charset=euc-kr"

    def render(self, content) -> bytes:
        if isinstance(content, bytes):
            return content
        if isinstance(content, str):
            return content.encode("euc-kr", errors="ignore")
        raise TypeError("HTML 응답에는 문자열이 필요합니다.")


app = FastAPI(default_response_class=EUCKRJSONResponse)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "email_logs.db"
_db_lock = threading.Lock()


STALE_SECONDS = 10


def _now_local() -> datetime:
    return datetime.now(timezone.utc).astimezone()



def _format_timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")



def _now_string() -> str:
    return _format_timestamp(_now_local())


def _normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ''
    return str(value).strip()


def _get_last_seen_dt(info: Dict[str, Optional[str]]) -> Optional[datetime]:
    value = info.get("last_seen_ts")
    if isinstance(value, datetime):
        return value
    raw = info.get("last_seen")
    if raw:
        try:
            naive = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
        tz = _now_local().tzinfo
        return naive.replace(tzinfo=tz) if tz else naive
    return None



def _update_last_seen(info: Dict[str, Optional[str]]) -> None:
    now = _now_local()
    info["last_seen"] = _format_timestamp(now)
    info["last_seen_ts"] = now
    if info.get("status") == "연결 끊김":
        info["status"] = "대기"



def _init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                device_name TEXT,
                helo TEXT,
                mail_from TEXT,
                rcpt_to TEXT,
                header TEXT,
                success INTEGER NOT NULL,
                response TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS device_defaults (
                device_id TEXT PRIMARY KEY,
                helo TEXT,
                mail_from TEXT,
                rcpt_to TEXT,
                header TEXT
            )
            """
        )
        conn.commit()





def _insert_log(payload: Dict[str, Optional[str]]) -> None:
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO logs (
                    device_id,
                    device_name,
                    helo,
                    mail_from,
                    rcpt_to,
                    header,
                    success,
                    response,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload.get("device_id"),
                    payload.get("device_name"),
                    payload.get("helo"),
                    payload.get("mail_from"),
                    payload.get("rcpt_to"),
                    payload.get("header"),
                    1 if payload.get("success") else 0,
                    payload.get("response"),
                    _now_string(),
                ),
            )
            conn.commit()


def _get_device_defaults(device_id: str) -> Dict[str, Optional[str]]:
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            row = conn.execute(
                "SELECT helo, mail_from, rcpt_to, header FROM device_defaults WHERE device_id = ?",
                (device_id,),
            ).fetchone()
    if not row:
        return {"helo": "", "mail_from": "", "rcpt_to": "", "header": ""}
    return {
        "helo": row[0] or "",
        "mail_from": row[1] or "",
        "rcpt_to": row[2] or "",
        "header": row[3] or "",
    }


def _save_device_defaults(device_id: str, data: Dict[str, Optional[str]]) -> None:
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO device_defaults (device_id, helo, mail_from, rcpt_to, header)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(device_id) DO UPDATE SET
                    helo = excluded.helo,
                    mail_from = excluded.mail_from,
                    rcpt_to = excluded.rcpt_to,
                    header = excluded.header
                """,
                (
                    device_id,
                    (data.get("helo") or "").strip(),
                    (data.get("mail_from") or "").strip(),
                    (data.get("rcpt_to") or "").strip(),
                    data.get("header") or "",
                ),
            )
            conn.commit()





class RegisterBody(BaseModel):
    device_id: Optional[str] = Field(None, description="디바이스 식별자")
    name: Optional[str] = Field(None, description="디바이스 표시 이름")

    @model_validator(mode="before")
    def _fill_defaults(cls, data):
        if not isinstance(data, dict):
            return data
        raw_name = _normalize_text(data.get("name"))
        raw_id = _normalize_text(data.get("device_id"))
        if not raw_id and not raw_name:
            raise ValueError("디바이스 이름이 필요합니다")
        if not raw_id:
            raw_id = raw_name or "device"
        if not raw_name:
            raw_name = raw_id
        data["device_id"] = raw_id
        data["name"] = raw_name
        return data


class SendCommandBody(BaseModel):
    device_id: str
    helo: str
    mail_from: str
    rcpt_to: str
    header: str


class ReportBody(BaseModel):
    device_id: str
    task_id: str
    success: bool
    response: str


class CancelBody(BaseModel):
    device_id: str


_devices: Dict[str, Dict[str, Optional[str]]] = {}
_pending: Dict[str, Dict[str, Optional[str]]] = {}


@app.on_event("startup")
async def on_startup() -> None:
    _init_db()


def _touch_device(device_id: str) -> None:
    if device_id in _devices:
        info = _devices[device_id]
        if "defaults" not in info:
            info["defaults"] = _get_device_defaults(device_id)
        _update_last_seen(info)
        return
    defaults = _get_device_defaults(device_id)
    now = _now_local()
    base_command = None
    if any(defaults.values()):
        base_command = {
            "helo": defaults.get("helo", ""),
            "mail_from": defaults.get("mail_from", ""),
            "rcpt_to": defaults.get("rcpt_to", ""),
            "header": defaults.get("header", ""),
        }
    _devices[device_id] = {
        "name": device_id,
        "last_seen": _format_timestamp(now),
        "last_seen_ts": now,
        "status": "대기",
        "last_success": None,
        "last_response": None,
        "last_command": base_command,
        "defaults": defaults,
    }


@app.post("/api/register")
async def register_device(body: RegisterBody):
    device_id = _normalize_text(body.device_id)
    device_name = _normalize_text(body.name)
    if not device_id:
        raise HTTPException(status_code=400, detail="device_id 필요")
    if not device_name:
        device_name = device_id
    existing = _devices.get(device_id)
    defaults = _get_device_defaults(device_id)
    if existing:
        existing["name"] = device_name
        existing["defaults"] = defaults
        if not existing.get("last_command") and any(defaults.values()):
            existing["last_command"] = {
                "helo": defaults.get("helo", ""),
                "mail_from": defaults.get("mail_from", ""),
                "rcpt_to": defaults.get("rcpt_to", ""),
                "header": defaults.get("header", ""),
            }
        _update_last_seen(existing)
    else:
        base_command = None
        if any(defaults.values()):
            base_command = {
                "helo": defaults.get("helo", ""),
                "mail_from": defaults.get("mail_from", ""),
                "rcpt_to": defaults.get("rcpt_to", ""),
                "header": defaults.get("header", ""),
            }
        _devices[device_id] = {
            "name": device_name,
            "last_seen": _now_string(),
            "status": "대기",
            "last_success": None,
            "last_response": None,
            "last_command": base_command,
            "defaults": defaults,
        }
        _update_last_seen(_devices[device_id])
    return {"message": "등록 완료"}


@app.get("/api/devices")
async def list_devices():
    result = []
    now = _now_local()
    to_remove = []
    for device_id, info in list(_devices.items()):
        pending = _pending.get(device_id)
        defaults = info.get("defaults") or _get_device_defaults(device_id)
        info["defaults"] = defaults
        last_seen_dt = _get_last_seen_dt(info)
        if last_seen_dt is None:
            stale = True
        else:
            stale = (now - last_seen_dt) > timedelta(seconds=STALE_SECONDS)
        if stale:
            to_remove.append(device_id)
            continue
        if info.get("status") == "연결 끊김":
            info["status"] = "대기"
        command_snapshot = info.get("last_command") or {
            "helo": defaults.get("helo", ""),
            "mail_from": defaults.get("mail_from", ""),
            "rcpt_to": defaults.get("rcpt_to", ""),
            "header": defaults.get("header", ""),
        }
        result.append(
            {
                "device_id": device_id,
                "name": info.get("name", device_id),
                "last_seen": info.get("last_seen"),
                "status": info.get("status"),
                "last_success": info.get("last_success"),
                "last_response": info.get("last_response"),
                "last_command": command_snapshot,
                "defaults": defaults,
                "pending": bool(pending),
                "pending_task_id": pending.get("task_id") if pending else None,
                "queued_at": pending.get("queued_at") if pending else None,
                "connected": True,
            }
        )
    for device_id in to_remove:
        _devices.pop(device_id, None)
        _pending.pop(device_id, None)
    return {"devices": result}


@app.post("/api/send")
async def queue_send(body: SendCommandBody):
    device_id = _normalize_text(body.device_id)
    if not device_id:
        raise HTTPException(status_code=400, detail="디바이스 식별자가 필요합니다")
    device = _devices.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다")
    if device_id in _pending:
        raise HTTPException(status_code=409, detail="이 디바이스는 이미 대기 중입니다")
    task_id = str(uuid.uuid4())
    command = {
        "task_id": task_id,
        "device_id": device_id,
        "helo": body.helo,
        "mail_from": body.mail_from,
        "rcpt_to": body.rcpt_to,
        "header": body.header,
        "queued_at": _now_string(),
        "status": "대기",
    }
    _pending[device_id] = command
    device["status"] = "발송 대기"
    snapshot = {
        "helo": body.helo,
        "mail_from": body.mail_from,
        "rcpt_to": body.rcpt_to,
        "header": body.header,
    }
    device["last_command"] = snapshot.copy()
    device["defaults"] = snapshot.copy()
    _save_device_defaults(device_id, snapshot)
    return {"message": "발송 대기열에 등록", "task_id": task_id}


@app.get("/api/send")
async def poll_command(device_id: str):
    device_id = _normalize_text(device_id)
    if not device_id or device_id not in _devices:
        raise HTTPException(status_code=404, detail="등록되지 않은 디바이스")
    _touch_device(device_id)
    command = _pending.get(device_id)
    if not command:
        return {"task": None}
    if command["status"] == "대기":
        command["status"] = "전송 중"
        _devices[device_id]["status"] = "전송 중"
    if command["status"] == "전송 중":
        return {
            "task": {
                "task_id": command["task_id"],
                "helo": command["helo"],
                "mail_from": command["mail_from"],
                "rcpt_to": command["rcpt_to"],
                "header": command["header"],
            }
        }
    return {"task": None}


@app.post("/api/report")
async def report_result(body: ReportBody):
    device_id = _normalize_text(body.device_id)
    if not device_id:
        raise HTTPException(status_code=400, detail="디바이스 식별자가 필요합니다")
    device = _devices.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="등록 필요")
    command = _pending.get(device_id)
    if command and command["task_id"] == body.task_id:
        _pending.pop(device_id)
    device["status"] = "대기"
    _update_last_seen(device)
    device["last_success"] = "성공" if body.success else "실패"
    device["last_response"] = body.response
    device_name = device.get("name", device_id)
    _insert_log(
        {
            "device_id": device_id,
            "device_name": device_name,
            "helo": command.get("helo") if command else None,
            "mail_from": command.get("mail_from") if command else None,
            "rcpt_to": command.get("rcpt_to") if command else None,
            "header": command.get("header") if command else None,
            "success": body.success,
            "response": body.response,
        }
    )
    return {"message": "결과 수신"}

@app.post("/api/cancel")
async def cancel_command(body: CancelBody):
    device_id = _normalize_text(body.device_id)
    if not device_id:
        raise HTTPException(status_code=400, detail="디바이스 식별자가 필요합니다")
    device = _devices.get(device_id)
    if not device:
        raise HTTPException(status_code=404, detail="미등록 디바이스")
    pending = _pending.pop(device_id, None)
    if not pending:
        return {"message": "대기 중인 발송 명령이 없습니다."}
    device["status"] = "대기"
    device["last_command"] = device.get("defaults")
    device["last_response"] = "사용자 중지"
    device["last_success"] = "중지"
    _update_last_seen(device)
    return {"message": "발송 명령을 중지했습니다."}


@app.get("/api/logs")
async def fetch_logs(device_id: Optional[str] = None, limit: int = 50):
    limit = max(1, min(limit, 200))
    base_query = "SELECT id, device_id, device_name, helo, mail_from, rcpt_to, header, success, response, created_at FROM logs"
    params = []
    if device_id:
        base_query += " WHERE device_id = ?"
        params.append(device_id)
    base_query += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            rows = conn.execute(base_query, params).fetchall()
    logs = []
    for row in rows:
        logs.append(
            {
                "id": row[0],
                "device_id": row[1],
                "device_name": row[2],
                "helo": row[3],
                "mail_from": row[4],
                "rcpt_to": row[5],
                "header": row[6],
                "success": bool(row[7]),
                "response": row[8],
                "created_at": row[9],
            }
        )
    return {"logs": logs}


@app.delete("/api/logs")
async def clear_logs(device_id: Optional[str] = None):
    with _db_lock:
        with sqlite3.connect(DB_PATH) as conn:
            if device_id:
                conn.execute("DELETE FROM logs WHERE device_id = ?", (device_id,))
            else:
                conn.execute("DELETE FROM logs")
            conn.commit()
    message = "선택한 디바이스 로그를 삭제했습니다." if device_id else "로그를 모두 삭제했습니다."
    return {"message": message}




HTML_PAGE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="euc-kr">
<title>다중 디바이스 메일 에이전트</title>
<style>
:root { color-scheme: light; }
body { font-family: 'Malgun Gothic', 'Apple SD Gothic Neo', sans-serif; background: #f4f7fb; margin: 0; padding: 24px; color: #1f2937; }
h1 { margin: 0 0 16px; font-size: 26px; font-weight: 700; }
#summary { margin-bottom: 16px; font-size: 14px; color: #4b5563; }
.device-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 18px; }
.card { background: #ffffff; border-radius: 16px; box-shadow: 0 12px 28px rgba(15, 23, 42, 0.12); padding: 18px; display: flex; flex-direction: column; gap: 14px; }
.card-header { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; }
.device-name { font-size: 18px; font-weight: 700; color: #111827; }
.meta-line { font-size: 13px; color: #6b7280; }
.status-chip { padding: 4px 12px; border-radius: 999px; font-size: 12px; font-weight: 600; }
.status-idle { background: #ecfdf5; color: #047857; }
.status-queue { background: #fef3c7; color: #92400e; }
.status-running { background: #e0f2fe; color: #0369a1; }
.status-off { background: #fee2e2; color: #b91c1c; }
.actions { display: flex; gap: 10px; }
button { padding: 8px 14px; border: none; border-radius: 8px; font-size: 13px; font-weight: 600; cursor: pointer; background: #2563eb; color: #fff; transition: transform 0.1s ease, box-shadow 0.2s ease; }
button:disabled { cursor: not-allowed; background: #cbd5f5; color: #475569; box-shadow: none; transform: none; }
button.secondary { background: #f3f4f6; color: #1f2937; }
button.secondary:hover { background: #e5e7eb; }
button.danger { background: #ef4444; color: #fff; }
button.danger:hover { background: #dc2626; }
button:hover { transform: translateY(-1px); box-shadow: 0 8px 16px rgba(37, 99, 235, 0.18); }
.field { display: flex; flex-direction: column; gap: 6px; font-size: 13px; color: #374151; }
.field > span { font-weight: 600; }
.form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; }
.field.textarea { grid-column: 1 / -1; }
input[type=text], textarea { border: 1px solid #d1d5db; border-radius: 8px; padding: 8px 10px; font-size: 13px; background: #f9fafb; color: #111827; }
textarea { height: 140px; resize: vertical; font-family: 'D2Coding', 'Courier New', monospace; }
.result-box { border-radius: 10px; padding: 10px 12px; font-size: 13px; background: #f9fafb; color: #1f2937; }
.result-box.success { border-left: 4px solid #10b981; }
.result-box.failure { border-left: 4px solid #ef4444; }
.result-box.neutral { border-left: 4px solid #9ca3af; }
.badge { display: inline-block; margin-left: 6px; padding: 2px 6px; border-radius: 6px; font-size: 12px; background: #e5e7eb; color: #374151; }
.log-section { margin-top: 32px; background: #ffffff; padding: 20px; border-radius: 16px; box-shadow: 0 10px 24px rgba(15, 23, 42, 0.12); }
.log-controls { display: flex; gap: 16px; align-items: center; flex-wrap: wrap; font-size: 13px; color: #374151; }
.log-controls label { display: flex; align-items: center; gap: 6px; }
.log-controls select { border: 1px solid #d1d5db; border-radius: 6px; padding: 6px 8px; font-size: 13px; }
.log-table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 12px; }
.log-table th, .log-table td { border: 1px solid #e5e7eb; padding: 8px; text-align: left; vertical-align: top; }
.log-table th { background: #f3f4f6; font-weight: 700; }
.log-table tr:nth-child(even) { background: #f9fafb; }
.log-response { max-width: 420px; white-space: pre-line; }
@media (max-width: 720px) {
  body { padding: 16px; }
}
</style>
</head>
<body>
<h1>다중 디바이스 메일 에이전트</h1>
<div id="summary"></div>
<div id="devices" class="device-grid"></div>
<section class="log-section">
  <div class="log-controls">
    <label>디바이스
      <select id="logDevice">
        <option value="all">전체</option>
      </select>
    </label>
    <label>표시 수
      <select id="logLimit">
        <option value="20">20</option>
        <option value="50">50</option>
        <option value="100">100</option>
      </select>
    </label>
    <button class="secondary" id="refreshLogsBtn">로그 새로고침</button>
    <button class="secondary danger" id="clearLogsBtn">로그 전체 삭제</button>
  </div>
  <table class="log-table">
    <thead>
      <tr>
        <th>시간</th>
        <th>디바이스</th>
        <th>MAIL FROM</th>
        <th>RCPT TO</th>
        <th>결과</th>
        <th>응답</th>
      </tr>
    </thead>
    <tbody id="logRows"></tbody>
  </table>
</section>
<script>
const POLL_MS = 1500;
const LOG_POLL_MS = 6000;
const decoder = new TextDecoder('euc-kr');
const fieldCache = {};
const state = { devices: [] };

function baseTemplate() {
  return { helo: '', mail_from: '', rcpt_to: '', header: '', _dirty: false };
}

function ensureCache(deviceId, defaults) {
  const base = baseTemplate();
  base.helo = (defaults?.helo || '').trim();
  base.mail_from = (defaults?.mail_from || '').trim();
  base.rcpt_to = (defaults?.rcpt_to || '').trim();
  base.header = defaults?.header || '';
  if (!fieldCache[deviceId]) {
    fieldCache[deviceId] = { ...base };
    fieldCache[deviceId]._dirty = false;
  } else if (!fieldCache[deviceId]._dirty) {
    Object.assign(fieldCache[deviceId], base);
  }
  return fieldCache[deviceId];
}

function markDirty(cache) {
  cache._dirty = true;
}

async function requestJson(url, options) {
  const res = await fetch(url, options);
  const buffer = await res.arrayBuffer();
  const text = decoder.decode(buffer);
  if (!res.ok) {
    throw new Error(text || '요청에 실패했습니다');
  }
  return text ? JSON.parse(text) : {};
}

function statusClass(status) {
  switch (status) {
    case '발송 대기':
      return 'status-chip status-queue';
    case '전송 중':
      return 'status-chip status-running';
    case '연결 끊김':
      return 'status-chip status-off';
    default:
      return 'status-chip status-idle';
  }
}

function createInputField(label, value) {
  const wrapper = document.createElement('label');
  wrapper.className = 'field';
  const span = document.createElement('span');
  span.textContent = label;
  const input = document.createElement('input');
  input.type = 'text';
  input.value = value || '';
  wrapper.append(span, input);
  return { wrapper, input };
}

function createTextareaField(label, value) {
  const wrapper = document.createElement('label');
  wrapper.className = 'field textarea';
  const span = document.createElement('span');
  span.textContent = label;
  const area = document.createElement('textarea');
  area.value = value || '';
  wrapper.append(span, area);
  return { wrapper, area };
}

function updateSummary() {
  const total = state.devices.length;
  const connected = state.devices.filter((dev) => dev.connected).length;
  const pending = state.devices.filter((dev) => dev.pending).length;
  const summary = document.getElementById('summary');
  summary.textContent = `총 ${total}대 · 연결 ${connected}대 · 대기 중 ${pending}건`;
}

function updateDeviceOptions() {
  const select = document.getElementById('logDevice');
  const current = select.value;
  select.innerHTML = '<option value="all">전체</option>';
  state.devices.forEach((dev) => {
    const option = document.createElement('option');
    option.value = dev.device_id;
    option.textContent = dev.name || dev.device_id;
    select.appendChild(option);
  });
  if ([...select.options].some((opt) => opt.value === current)) {
    select.value = current;
  }
}

function renderDevices(data) {
  state.devices = data.devices || [];
  updateSummary();
  updateDeviceOptions();
  const container = document.getElementById('devices');
  const activeElement = document.activeElement;
  let activeInfo = null;
  if (activeElement && container.contains(activeElement) && (activeElement.tagName === 'INPUT' || activeElement.tagName === 'TEXTAREA')) {
    activeInfo = {
      deviceId: activeElement.dataset.deviceId || null,
      field: activeElement.dataset.field || null,
      selectionStart: activeElement.selectionStart,
      selectionEnd: activeElement.selectionEnd,
    };
  }
  container.innerHTML = '';
  const fragment = document.createDocumentFragment();
  state.devices.forEach((dev) => {
    const defaults = dev.defaults || baseTemplate();
    const command = dev.last_command || defaults;
    const cache = ensureCache(dev.device_id, command);
    if (!dev.pending && !cache._dirty) {
      Object.assign(cache, { ...command, _dirty: false });
    }

    const card = document.createElement('div');
    card.className = 'card';

    const header = document.createElement('div');
    header.className = 'card-header';

    const titleWrap = document.createElement('div');
    const title = document.createElement('div');
    title.className = 'device-name';
    title.textContent = dev.name || dev.device_id;
    titleWrap.append(title);

    const statusText = dev.connected ? (dev.status || '대기') : '연결 끊김';
    const status = document.createElement('span');
    status.className = statusClass(statusText);
    status.textContent = statusText;

    header.append(titleWrap, status);
    card.append(header);

    const connectionLine = document.createElement('div');
    connectionLine.className = 'meta-line';
    const connectionText = dev.connected ? '연결 중' : '연결 끊김';
    connectionLine.textContent = `연결: ${connectionText} · 최근 접속: ${dev.last_seen || '기록 없음'}`;
    card.append(connectionLine);

    const form = document.createElement('div');
    form.className = 'form-grid';

    const heloField = createInputField('HELO', cache.helo);
    heloField.input.dataset.deviceId = dev.device_id;
    heloField.input.dataset.field = 'helo';
    heloField.input.addEventListener('input', () => { cache.helo = heloField.input.value; markDirty(cache); });

    const mailField = createInputField('MAIL FROM', cache.mail_from);
    mailField.input.dataset.deviceId = dev.device_id;
    mailField.input.dataset.field = 'mail_from';
    mailField.input.addEventListener('input', () => { cache.mail_from = mailField.input.value; markDirty(cache); });

    const rcptField = createInputField('RCPT TO', cache.rcpt_to);
    rcptField.input.dataset.deviceId = dev.device_id;
    rcptField.input.dataset.field = 'rcpt_to';
    rcptField.input.addEventListener('input', () => { cache.rcpt_to = rcptField.input.value; markDirty(cache); });

    const headerField = createTextareaField('HEADER', cache.header);
    headerField.area.dataset.deviceId = dev.device_id;
    headerField.area.dataset.field = 'header';
    headerField.area.addEventListener('input', () => { cache.header = headerField.area.value; markDirty(cache); });

    form.append(heloField.wrapper, mailField.wrapper, rcptField.wrapper, headerField.wrapper);
    card.append(form);

    const actions = document.createElement('div');
    actions.className = 'actions';

    const sendBtn = document.createElement('button');
    sendBtn.textContent = dev.pending ? '발송 대기 중' : '발송';
    sendBtn.disabled = !!dev.pending;

    const cancelBtn = document.createElement('button');
    cancelBtn.textContent = '중지';
    cancelBtn.className = 'secondary';
    cancelBtn.disabled = !dev.pending;

    sendBtn.addEventListener('click', () => {
      queueSend(dev.device_id, {
        helo: heloField.input.value,
        mail_from: mailField.input.value,
        rcpt_to: rcptField.input.value,
        header: headerField.area.value,
      }, sendBtn, cancelBtn, cache);
    });

    cancelBtn.addEventListener('click', () => {
      cancelSend(dev.device_id, cancelBtn, sendBtn);
    });

    actions.append(sendBtn, cancelBtn);
    card.append(actions);

    const resultBox = document.createElement('div');
    const successState = dev.last_success;
    if (successState === '성공') {
      resultBox.className = 'result-box success';
      resultBox.textContent = `결과: ${dev.last_response || '성공'}`;
    } else if (successState === '실패') {
      resultBox.className = 'result-box failure';
      resultBox.textContent = `결과: ${dev.last_response || '실패'}`;
    } else if (successState === '중지') {
      resultBox.className = 'result-box neutral';
      resultBox.textContent = dev.last_response || '사용자 중지';
    } else {
      resultBox.className = 'result-box neutral';
      resultBox.textContent = '최근 결과 없음';
    }

    if (dev.pending && dev.pending_task_id) {
      const badge = document.createElement('span');
      badge.className = 'badge';
      badge.textContent = `작업 ID: ${dev.pending_task_id}`;
      resultBox.appendChild(badge);
    }

    card.append(resultBox);
    fragment.append(card);
  });
  container.append(fragment);
  if (activeInfo && activeInfo.deviceId && activeInfo.field) {
    const selector = `[data-device-id="${activeInfo.deviceId}"][data-field="${activeInfo.field}"]`;
    const nextActive = container.querySelector(selector);
    if (nextActive) {
      nextActive.focus();
      if (typeof activeInfo.selectionStart === 'number' && typeof activeInfo.selectionEnd === 'number') {
        nextActive.setSelectionRange(activeInfo.selectionStart, activeInfo.selectionEnd);
      }
    }
  }
}

async function refresh() {
  try {
    const data = await requestJson('/api/devices');
    renderDevices(data);
  } catch (err) {
    console.error('디바이스 목록 조회 실패', err);
  }
}

async function queueSend(deviceId, payload, sendButton, cancelButton, cache) {
  sendButton.disabled = true;
  cancelButton.disabled = true;
  try {
    await requestJson('/api/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=euc-kr' },
      body: JSON.stringify({
        device_id: deviceId,
        helo: payload.helo,
        mail_from: payload.mail_from,
        rcpt_to: payload.rcpt_to,
        header: payload.header,
      }),
    });
    cache._dirty = false;
    cache.helo = payload.helo;
    cache.mail_from = payload.mail_from;
    cache.rcpt_to = payload.rcpt_to;
    cache.header = payload.header;
    await refresh();
    await refreshLogs(false);
  } catch (err) {
    alert(err.message || err);
  } finally {
    sendButton.disabled = false;
  }
}

async function cancelSend(deviceId, cancelButton, sendButton) {
  cancelButton.disabled = true;
  try {
    await requestJson('/api/cancel', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=euc-kr' },
      body: JSON.stringify({ device_id: deviceId }),
    });
    await refresh();
    await refreshLogs(false);
  } catch (err) {
    alert(err.message || err);
  } finally {
    cancelButton.disabled = false;
    sendButton.disabled = false;
  }
}

function renderLogs(data) {
  const tbody = document.getElementById('logRows');
  tbody.innerHTML = '';
  (data.logs || []).forEach((log) => {
    const row = document.createElement('tr');

    const created = document.createElement('td');
    created.textContent = log.created_at || '';

    const device = document.createElement('td');
    device.textContent = log.device_name || log.device_id || '';

    const mailFrom = document.createElement('td');
    mailFrom.textContent = log.mail_from || '';

    const rcpt = document.createElement('td');
    rcpt.textContent = log.rcpt_to || '';

    const result = document.createElement('td');
    result.textContent = log.success ? '성공' : '실패';

    const response = document.createElement('td');
    response.className = 'log-response';
    response.textContent = (log.response || '').slice(0, 400);

    row.append(created, device, mailFrom, rcpt, result, response);
    tbody.append(row);
  });
}

async function clearLogs() {
  const deviceSelect = document.getElementById('logDevice');
  const deviceId = deviceSelect.value;
  const scope = deviceId && deviceId !== 'all' ? '선택한 디바이스 로그를' : '모든 로그를';
  if (!confirm(`${scope} 삭제하시겠습니까?`)) {
    return;
  }
  const url = deviceId && deviceId !== 'all' ? `/api/logs?device_id=${encodeURIComponent(deviceId)}` : '/api/logs';
  try {
    await requestJson(url, { method: 'DELETE' });
    await refreshLogs(true);
  } catch (err) {
    alert(err.message || err);
  }
}

async function refreshLogs(showAlert = false) {
  try {
    const deviceSelect = document.getElementById('logDevice');
    const limitSelect = document.getElementById('logLimit');
    const deviceId = deviceSelect.value;
    const limit = parseInt(limitSelect.value, 10) || 20;
    let url = `/api/logs?limit=${limit}`;
    if (deviceId && deviceId !== 'all') {
      url += `&device_id=${encodeURIComponent(deviceId)}`;
    }
    const data = await requestJson(url);
    renderLogs(data);
  } catch (err) {
    if (showAlert) {
      alert(err.message || err);
    } else {
      console.error('로그 조회 실패', err);
    }
  }
}

document.getElementById('refreshLogsBtn').addEventListener('click', () => refreshLogs(true));
document.getElementById('clearLogsBtn').addEventListener('click', () => clearLogs());
document.getElementById('logLimit').addEventListener('change', () => refreshLogs());
document.getElementById('logDevice').addEventListener('change', () => refreshLogs());

refresh();
refreshLogs();
setInterval(refresh, POLL_MS);
setInterval(() => refreshLogs(), LOG_POLL_MS);
</script>
</body>
</html>
"""

@app.get("/")
async def index():
    return EUCKRHTMLResponse(content=HTML_PAGE)
