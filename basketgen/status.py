from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .storage import load_json, save_json


STATUS_FILENAME = "job_status.json"


def status_path(job_path: Path) -> Path:
    return job_path / STATUS_FILENAME


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _default_status() -> dict[str, Any]:
    return {
        "stage": "idle",
        "progress": 0,
        "message": "Ожидание запуска.",
        "done": False,
        "error": "",
        "updated_at": _now_iso(),
    }


def load_job_status(job_path: Path) -> dict[str, Any]:
    payload = load_json(status_path(job_path), default={})
    status = _default_status()
    status.update(payload)
    status["progress"] = int(max(0, min(100, int(status.get("progress", 0)))))
    status["stage"] = str(status.get("stage") or "idle")
    status["message"] = str(status.get("message") or "")
    status["done"] = bool(status.get("done"))
    status["error"] = str(status.get("error") or "")
    return status


def save_job_status(
    job_path: Path,
    *,
    stage: str,
    progress: int,
    message: str,
    done: bool = False,
    error: str = "",
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = load_job_status(job_path)
    payload.update(
        {
            "stage": stage,
            "progress": int(max(0, min(100, progress))),
            "message": message,
            "done": done,
            "error": error,
            "updated_at": _now_iso(),
        }
    )
    if extra:
        payload.update(extra)
    save_json(status_path(job_path), payload)
    return payload


def init_job_status(job_path: Path, message: str = "Ожидание запуска.") -> dict[str, Any]:
    return save_job_status(
        job_path,
        stage="idle",
        progress=0,
        message=message,
        done=False,
        error="",
    )


def fail_job_status(job_path: Path, message: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    return save_job_status(
        job_path,
        stage="error",
        progress=100,
        message="Ошибка.",
        done=True,
        error=message,
        extra=extra,
    )
