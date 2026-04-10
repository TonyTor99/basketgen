from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_job(base_dir: Path) -> str:
    job_id = uuid4().hex[:12]
    ensure_dir(base_dir / job_id)
    return job_id


def job_dir(base_dir: Path, job_id: str) -> Path:
    return ensure_dir(base_dir / job_id)


def save_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return default or {}
    return json.loads(path.read_text(encoding="utf-8"))


def copy_upload(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
