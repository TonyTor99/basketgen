from __future__ import annotations

import tempfile
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from .engine import SearchParams, search_strategies, strategy_equity_payload, strategy_matches
from .excel import (
    DatasetPreparationError,
    FeaturePair,
    build_header_info,
    build_quick_summary,
    detect_feature_pairs,
    format_filter_diagnostics,
    prepare_dataset_detailed,
)
from .status import fail_job_status, init_job_status, load_job_status, save_job_status
from .storage import create_job, job_dir, load_json, save_json


ALLOWED_EXTENSIONS = {"xlsx", "xlsm", "xls"}

JOB_STAGE_FLOW = [
    {"id": "reading_excel", "title": "Чтение Excel", "progress": 10, "message": "Читаю Excel..."},
    {"id": "preparing_dataset", "title": "Подготовка датасета", "progress": 25, "message": "Определяю признаки..."},
    {"id": "applying_filters", "title": "Применение фильтров", "progress": 40, "message": "Применяю фильтры..."},
    {"id": "encoding_features", "title": "Кодирование признаков", "progress": 55, "message": "Кодирую признаки..."},
    {"id": "searching_strategies", "title": "Поиск стратегий", "progress": 78, "message": "Генерирую стратегии..."},
    {"id": "saving_results", "title": "Сохранение результатов", "progress": 92, "message": "Сохраняю результаты..."},
    {"id": "done", "title": "Готово", "progress": 100, "message": "Готово."},
]
JOB_STAGE_BY_ID = {item["id"]: item for item in JOB_STAGE_FLOW}
GENERATION_HISTORY_FILE = "generation_history.json"


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def _serialize_feature(pair: FeaturePair) -> dict[str, Any]:
    return {
        "name": pair.name,
        "home_col": pair.home_col,
        "away_col": pair.away_col,
        "completeness": round(pair.completeness * 100.0, 2),
    }


def _deserialize_feature(data: dict[str, Any]) -> FeaturePair:
    completeness_raw = float(data.get("completeness", 0))
    completeness = completeness_raw / 100.0 if completeness_raw > 1 else completeness_raw
    return FeaturePair(
        name=str(data["name"]),
        home_col=str(data["home_col"]),
        away_col=str(data["away_col"]),
        completeness=completeness,
    )


def _parse_int(value: str, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Поле \"{field_name}\" должно быть целым числом.") from exc


def _parse_optional_int(value: str, field_name: str) -> int | None:
    raw = (value or "").strip()
    if not raw:
        return None
    return _parse_int(raw, field_name)


def _parse_optional_float(value: str, field_name: str) -> float | None:
    raw = (value or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError as exc:
        raise ValueError(f"Поле \"{field_name}\" должно быть числом.") from exc


def _monthly_stats_to_text(monthly_stats: Any) -> str:
    if not isinstance(monthly_stats, list):
        return ""
    chunks: list[str] = []
    for item in monthly_stats:
        if not isinstance(item, dict):
            continue
        month = str(item.get("month", "")).strip()
        if not month:
            continue
        matches = item.get("matches")
        profit = item.get("profit")
        roi = item.get("roi")

        parts = [f"{month}: {matches if matches is not None else '—'} матч."]
        if profit is not None:
            try:
                parts.append(f"прибыль {float(profit):+.2f}")
            except (TypeError, ValueError):
                pass
        if roi is not None:
            try:
                parts.append(f"ROI {float(roi):.2f}%")
            except (TypeError, ValueError):
                pass
        chunks.append(", ".join(parts))
    return " | ".join(chunks)


def _monthly_pnl_to_text(monthly_pnl: Any) -> str:
    if not isinstance(monthly_pnl, dict):
        return ""
    parsed: list[tuple[str, float, pd.Timestamp | None]] = []
    for month, profit in monthly_pnl.items():
        try:
            profit_value = float(profit)
        except (TypeError, ValueError):
            continue
        parsed_month = pd.to_datetime(str(month), format="%m-%Y", errors="coerce")
        parsed.append((str(month), profit_value, None if pd.isna(parsed_month) else parsed_month))
    parsed.sort(key=lambda item: (item[2] is None, item[2] if item[2] is not None else pd.Timestamp.max, item[0]))
    chunks = [f"{month}: {profit_value:+.2f}" for month, profit_value, _ in parsed]
    return " | ".join(chunks)


def _build_results_csv_dataframe(results: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in results:
        train = row.get("train", {})
        test = row.get("test", {})
        monthly_text = _monthly_stats_to_text(row.get("monthly_stats", []))
        if not monthly_text:
            monthly_text = _monthly_pnl_to_text(row.get("monthly_pnl", {}))

        rows.append(
            {
                "ID стратегии": row.get("strategy_id"),
                "Правило": row.get("rule"),
                "Использовано признаков": row.get("used_features"),
                "Матчи (всего)": row.get("matches"),
                "Победы": row.get("wins"),
                "Поражения": row.get("losses"),
                "Возвраты": row.get("pushes"),
                "Win rate, %": row.get("win_rate"),
                "Средний коэффициент": row.get("avg_odds"),
                "Прибыль, units": row.get("profit"),
                "ROI, %": row.get("roi"),
                "Score": row.get("score"),
                "Profit / DD": row.get("profit_dd_ratio"),
                "Макс. просадка, units": row.get("max_drawdown"),
                "Макс. просадка, %": row.get("max_drawdown_pct"),
                "Дней в просадке": row.get("max_drawdown_days"),
                "Серия побед, макс.": row.get("plus_streak"),
                "Серия поражений, макс.": row.get("minus_streak"),
                "Train: матчи": train.get("matches"),
                "Train: прибыль, units": train.get("profit"),
                "Train: ROI, %": train.get("roi"),
                "Train: Win rate, %": train.get("win_rate"),
                "Train: макс. просадка, units": train.get("max_drawdown"),
                "Train: макс. просадка, %": train.get("max_drawdown_pct"),
                "Train: Profit / DD": train.get("profit_dd_ratio"),
                "Test: матчи": test.get("matches"),
                "Test: прибыль, units": test.get("profit"),
                "Test: ROI, %": test.get("roi"),
                "Test: Win rate, %": test.get("win_rate"),
                "Test: макс. просадка, units": test.get("max_drawdown"),
                "Test: макс. просадка, %": test.get("max_drawdown_pct"),
                "Test: Profit / DD": test.get("profit_dd_ratio"),
                "Помесячная динамика": monthly_text,
            }
        )
    return pd.DataFrame(rows)


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _format_history_datetime(raw_iso: Any) -> str:
    if not raw_iso:
        return "—"
    try:
        dt = datetime.fromisoformat(str(raw_iso).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        local_dt = dt.astimezone()
        return local_dt.strftime("%d.%m.%Y %H:%M")
    except ValueError:
        return str(raw_iso)


def create_app() -> Flask:
    root_dir = Path(__file__).resolve().parents[1]
    instance_path = root_dir / "instance"
    jobs_root = instance_path / "jobs"
    history_path = instance_path / GENERATION_HISTORY_FILE
    history_lock = threading.Lock()

    app = Flask(
        __name__,
        template_folder=str(root_dir / "templates"),
        static_folder=str(root_dir / "static"),
        instance_path=str(instance_path),
        instance_relative_config=False,
    )
    app.config["SECRET_KEY"] = "basketgen-local-secret"
    app.config["MAX_CONTENT_LENGTH"] = 512 * 1024 * 1024
    jobs_root.mkdir(parents=True, exist_ok=True)

    def map_history_item(item: dict[str, Any]) -> dict[str, Any]:
        selected_feature_names_raw = item.get("selected_feature_names", [])
        if isinstance(selected_feature_names_raw, list):
            selected_feature_names = [str(name) for name in selected_feature_names_raw if str(name).strip()]
        else:
            selected_feature_names = []

        return {
            "generated_at": item.get("generated_at"),
            "generated_at_label": _format_history_datetime(item.get("generated_at")),
            "filename": str(item.get("filename") or "—"),
            "job_id": str(item.get("job_id") or ""),
            "bot_filter": str(item.get("bot_filter") or ""),
            "signal_time": _safe_int(item.get("signal_time")),
            "odds_range_label": item.get("odds_range_label") or "—",
            "min_bets": _safe_int(item.get("min_bets")),
            "top_k": _safe_int(item.get("top_k")),
            "holdout_days": _safe_int(item.get("holdout_days")),
            "min_used_features": _safe_int(item.get("min_used_features")),
            "selected_features_count": _safe_int(item.get("selected_features_count")) or len(selected_feature_names),
            "selected_feature_names_label": ", ".join(selected_feature_names) if selected_feature_names else "—",
            "source_rows": _safe_int(item.get("source_rows")) or 0,
            "strategies_count": _safe_int(item.get("strategies_count")) or 0,
        }

    def fallback_history_from_jobs(limit: int | None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in jobs_root.iterdir():
            if not path.is_dir():
                continue
            meta = load_json(path / "meta.json", default={})
            params = load_json(path / "run_params.json", default={})
            results_payload = load_json(path / "results.json", default={})
            if not params or not results_payload:
                continue

            min_odds = _safe_float(params.get("min_odds"))
            max_odds = _safe_float(params.get("max_odds"))
            if min_odds is None and max_odds is None:
                odds_range_label = "—"
            elif min_odds is None:
                odds_range_label = f"до {max_odds:.2f}"
            elif max_odds is None:
                odds_range_label = f"от {min_odds:.2f}"
            else:
                odds_range_label = f"{min_odds:.2f} .. {max_odds:.2f}"

            status = load_job_status(path)
            rows.append(
                map_history_item(
                    {
                        "generated_at": status.get("updated_at"),
                        "job_id": path.name,
                        "filename": str(meta.get("filename") or ""),
                        "bot_filter": params.get("bot_filter"),
                        "signal_time": params.get("signal_time"),
                        "odds_range_label": odds_range_label,
                        "min_bets": params.get("min_bets"),
                        "top_k": params.get("top_k"),
                        "holdout_days": params.get("holdout_days"),
                        "min_used_features": params.get("min_used_features"),
                        "selected_features_count": len(meta.get("selected_feature_names", [])),
                        "selected_feature_names": meta.get("selected_feature_names", []),
                        "source_rows": params.get("source_rows"),
                        "strategies_count": len(results_payload.get("results", [])),
                    }
                )
            )
        rows.sort(key=lambda row: str(row.get("generated_at") or ""), reverse=True)
        if limit is None:
            return rows
        return rows[: max(0, limit)]

    def load_generation_history(limit: int | None = None) -> list[dict[str, Any]]:
        payload = load_json(history_path, default={})
        items_raw = payload.get("items", [])
        if not isinstance(items_raw, list):
            items_raw = []

        rows = [map_history_item(item) for item in items_raw if isinstance(item, dict)]
        rows.sort(key=lambda row: str(row.get("generated_at") or ""), reverse=True)
        if rows:
            if limit is None:
                return rows
            return rows[: max(0, limit)]
        return fallback_history_from_jobs(limit)

    def append_generation_history(entry: dict[str, Any]) -> None:
        with history_lock:
            payload = load_json(history_path, default={})
            items = payload.get("items", [])
            if not isinstance(items, list):
                items = []
            items.append(entry)
            save_json(history_path, {"items": items})

    def push_stage(current_job_dir: Path, stage_id: str) -> None:
        stage = JOB_STAGE_BY_ID[stage_id]
        save_job_status(
            current_job_dir,
            stage=stage_id,
            progress=int(stage["progress"]),
            message=str(stage["message"]),
            done=False,
            error="",
        )

    def run_generation_job(job_id: str, run_payload: dict[str, Any]) -> None:
        current_job_dir = job_dir(jobs_root, job_id)
        try:
            meta = load_json(current_job_dir / "meta.json")
            if not meta:
                raise RuntimeError("Job не найден.")

            all_features = [_deserialize_feature(item) for item in meta.get("features", [])]
            selected_lookup = set(run_payload["selected_feature_names"])
            selected_pairs = [item for item in all_features if item.name in selected_lookup]
            if not selected_pairs:
                raise RuntimeError("Выберите хотя бы один признак.")

            input_path = Path(meta["input_path"])
            push_stage(current_job_dir, "reading_excel")
            header_info = build_header_info(input_path)

            def stage_callback(stage_id: str) -> None:
                if stage_id in JOB_STAGE_BY_ID:
                    push_stage(current_job_dir, stage_id)

            prepared = prepare_dataset_detailed(
                input_path,
                header_info,
                selected_pairs=selected_pairs,
                bot_filter=run_payload["bot_filter"],
                signal_time=run_payload["signal_time"],
                min_odds=run_payload["min_odds"],
                max_odds=run_payload["max_odds"],
                stage_callback=stage_callback,
            )

            push_stage(current_job_dir, "searching_strategies")
            params = SearchParams(
                min_bets=int(run_payload["min_bets"]),
                top_k=int(run_payload["top_k"]),
                holdout_days=int(run_payload["holdout_days"]),
                min_used_features=int(run_payload["min_used_features"]),
            )
            results = search_strategies(prepared.df, prepared.codes, selected_pairs, params)
            if not results:
                raise RuntimeError(
                    "Не удалось построить стратегии: "
                    f"слишком высокий min_bets ({params.min_bets}) для текущего набора данных."
                )

            push_stage(current_job_dir, "saving_results")
            prepared.df.to_pickle(current_job_dir / "prepared.pkl")
            pd.DataFrame(prepared.codes).to_pickle(current_job_dir / "codes.pkl")
            save_json(current_job_dir / "selected_pairs.json", {"pairs": [_serialize_feature(item) for item in selected_pairs]})
            save_json(current_job_dir / "results.json", {"results": results})
            save_json(
                current_job_dir / "run_params.json",
                {
                    "bot_filter": run_payload["bot_filter"],
                    "signal_time": run_payload["signal_time"],
                    "min_odds": run_payload["min_odds"],
                    "max_odds": run_payload["max_odds"],
                    "min_bets": params.min_bets,
                    "top_k": params.top_k,
                    "holdout_days": params.holdout_days,
                    "min_used_features": params.min_used_features,
                    "source_rows": int(len(prepared.df)),
                    "filter_diagnostics": prepared.diagnostics.as_dict(),
                    "filter_diagnostics_lines": format_filter_diagnostics(prepared.diagnostics),
                },
            )
            min_odds = _safe_float(run_payload.get("min_odds"))
            max_odds = _safe_float(run_payload.get("max_odds"))
            if min_odds is None and max_odds is None:
                odds_range_label = "—"
            elif min_odds is None:
                odds_range_label = f"до {max_odds:.2f}"
            elif max_odds is None:
                odds_range_label = f"от {min_odds:.2f}"
            else:
                odds_range_label = f"{min_odds:.2f} .. {max_odds:.2f}"

            append_generation_history(
                {
                    "generated_at": _now_iso_utc(),
                    "job_id": job_id,
                    "filename": str(meta.get("filename") or ""),
                    "bot_filter": run_payload["bot_filter"],
                    "signal_time": run_payload["signal_time"],
                    "odds_range_label": odds_range_label,
                    "min_bets": params.min_bets,
                    "top_k": params.top_k,
                    "holdout_days": params.holdout_days,
                    "min_used_features": params.min_used_features,
                    "selected_features_count": len(selected_pairs),
                    "selected_feature_names": list(run_payload["selected_feature_names"]),
                    "source_rows": int(len(prepared.df)),
                    "strategies_count": len(results),
                }
            )

            save_job_status(
                current_job_dir,
                stage="done",
                progress=100,
                message="Готово.",
                done=True,
                error="",
            )
        except DatasetPreparationError as exc:
            diagnostics_lines = format_filter_diagnostics(exc.diagnostics)
            fail_job_status(
                current_job_dir,
                f"{exc}\n" + "\n".join(diagnostics_lines),
                extra={
                    "filter_diagnostics": exc.diagnostics.as_dict(),
                    "filter_diagnostics_lines": diagnostics_lines,
                },
            )
        except Exception as exc:
            app.logger.exception("Generation failed for job %s", job_id)
            fail_job_status(current_job_dir, str(exc) or "Неизвестная ошибка.")

    @app.get("/")
    def index() -> str:
        history_items = load_generation_history()
        return render_template("index.html", history_items=history_items)

    @app.post("/upload")
    def upload() -> Any:
        uploaded = request.files.get("collector_file")
        if not uploaded or uploaded.filename == "":
            flash("Выберите Excel-файл сборщика.", "error")
            return redirect(url_for("index"))
        if not allowed_file(uploaded.filename):
            flash("Поддерживаются только Excel-файлы .xlsx / .xlsm / .xls.", "error")
            return redirect(url_for("index"))

        job_id = create_job(jobs_root)
        current_job_dir = job_dir(jobs_root, job_id)
        filename = secure_filename(uploaded.filename) or "collector.xlsx"
        input_path = current_job_dir / filename
        uploaded.save(input_path)

        header_info = build_header_info(input_path)
        features = detect_feature_pairs(input_path, header_info=header_info)

        quick_summary: dict[str, Any] = {}
        quick_summary_error = ""
        try:
            quick_summary = build_quick_summary(input_path, header_info=header_info)
        except Exception as exc:
            app.logger.exception("Failed to build quick summary for job %s", job_id)
            quick_summary_error = f"Не удалось построить сводку по файлу: {exc}"

        save_json(
            current_job_dir / "meta.json",
            {
                "job_id": job_id,
                "filename": filename,
                "input_path": str(input_path),
                "features": [_serialize_feature(item) for item in features],
                "selected_feature_names": [item.name for item in features[:8]],
                "quick_summary": quick_summary,
                "quick_summary_error": quick_summary_error,
            },
        )
        init_job_status(current_job_dir, message="Файл загружен. Настройте параметры и запустите генерацию.")
        return redirect(url_for("job_page", job_id=job_id))

    @app.get("/job/<job_id>")
    def job_page(job_id: str) -> str:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        if not meta:
            flash("Job не найден.", "error")
            return redirect(url_for("index"))
        defaults = load_json(current_job_dir / "run_params.json", default={})
        return render_template("job.html", meta=meta, defaults=defaults)

    @app.post("/job/<job_id>/generate")
    def generate(job_id: str) -> Any:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        if not meta:
            flash("Job не найден.", "error")
            return redirect(url_for("index"))
        current_status = load_job_status(current_job_dir)
        if not current_status.get("done") and current_status.get("stage") not in {"idle", "error"}:
            return redirect(url_for("job_wait_page", job_id=job_id))

        all_features = [_deserialize_feature(item) for item in meta.get("features", [])]
        selected_names = request.form.getlist("features")
        selected_lookup = set(selected_names)
        selected_pairs = [item for item in all_features if item.name in selected_lookup]
        if not selected_pairs:
            flash("Выберите хотя бы один признак.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        try:
            bot_filter = (request.form.get("bot_filter") or "SB 1 Basket Fonbet").strip()
            signal_time = _parse_optional_int(request.form.get("signal_time", "20"), "Время сигнала")
            min_odds = _parse_optional_float(request.form.get("min_odds", ""), "Минимальный коэффициент")
            max_odds = _parse_optional_float(request.form.get("max_odds", ""), "Максимальный коэффициент")
            min_bets = _parse_int(request.form.get("min_bets", "300"), "Min bets")
            top_k = _parse_int(request.form.get("top_k", "200"), "Top K")
            holdout_days = _parse_int(request.form.get("holdout_days", "60"), "Holdout days")
            min_used_features = _parse_int(request.form.get("min_used_features", "2"), "Мин. используемых признаков")
        except ValueError as exc:
            flash(str(exc), "error")
            return redirect(url_for("job_page", job_id=job_id))

        if min_odds is not None and max_odds is not None and min_odds > max_odds:
            flash("Минимальный коэффициент не может быть больше максимального.", "error")
            return redirect(url_for("job_page", job_id=job_id))
        if min_bets <= 0 or top_k <= 0 or holdout_days <= 0:
            flash("Параметры min bets, top K и holdout days должны быть больше нуля.", "error")
            return redirect(url_for("job_page", job_id=job_id))
        if min_used_features <= 0:
            flash("Мин. используемых признаков должен быть больше нуля.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        run_payload = {
            "bot_filter": bot_filter,
            "signal_time": signal_time,
            "min_odds": min_odds,
            "max_odds": max_odds,
            "min_bets": min_bets,
            "top_k": top_k,
            "holdout_days": holdout_days,
            "min_used_features": min_used_features,
            "selected_feature_names": [item.name for item in selected_pairs],
        }
        meta["selected_feature_names"] = run_payload["selected_feature_names"]
        save_json(current_job_dir / "meta.json", meta)
        save_json(current_job_dir / "job_request.json", run_payload)
        save_job_status(
            current_job_dir,
            stage="queued",
            progress=2,
            message="Запускаю задачу...",
            done=False,
            error="",
        )

        worker = threading.Thread(target=run_generation_job, args=(job_id, run_payload), daemon=True)
        worker.start()
        return redirect(url_for("job_wait_page", job_id=job_id))

    @app.get("/job/<job_id>/wait")
    def job_wait_page(job_id: str) -> str:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        if not meta:
            flash("Job не найден.", "error")
            return redirect(url_for("index"))
        return render_template(
            "job_wait.html",
            meta=meta,
            job_id=job_id,
            wait_steps=[{"id": item["id"], "title": item["title"]} for item in JOB_STAGE_FLOW],
        )

    @app.get("/job/<job_id>/status")
    def job_status(job_id: str) -> Any:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        if not meta:
            payload = {
                "stage": "error",
                "progress": 100,
                "message": "Job не найден.",
                "done": True,
                "error": "Job не найден.",
            }
            return jsonify(payload), 404

        status = load_job_status(current_job_dir)
        if status.get("done") and not status.get("error"):
            status["result_url"] = url_for("results_page", job_id=job_id)
        return jsonify(status)

    @app.get("/job/<job_id>/results")
    def results_page(job_id: str) -> str:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        result_payload = load_json(current_job_dir / "results.json")
        params = load_json(current_job_dir / "run_params.json")
        if not result_payload:
            flash("Сначала сгенерируйте стратегии.", "error")
            return redirect(url_for("job_page", job_id=job_id))
        results = result_payload.get("results", [])
        best_profit_dd_candidates = [float(row["profit_dd_ratio"]) for row in results if row.get("profit_dd_ratio") is not None]
        summary = {
            "strategies": len(results),
            "best_test_roi": max((row.get("test", {}).get("roi", 0) for row in results), default=0),
            "best_score": max((row.get("score", 0) for row in results), default=0),
            "best_profit_dd": round(max(best_profit_dd_candidates), 2) if best_profit_dd_candidates else None,
        }
        return render_template("results.html", meta=meta, params=params, results=results, summary=summary, job_id=job_id)

    @app.get("/job/<job_id>/strategy/<int:strategy_id>")
    def strategy_page(job_id: str, strategy_id: int) -> str:
        current_job_dir = job_dir(jobs_root, job_id)
        result_payload = load_json(current_job_dir / "results.json")
        if not result_payload:
            flash("Сначала сгенерируйте стратегии.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        results = result_payload.get("results", [])
        strategy = next((row for row in results if int(row["strategy_id"]) == strategy_id), None)
        if strategy is None:
            flash("Стратегия не найдена.", "error")
            return redirect(url_for("results_page", job_id=job_id))

        df = pd.read_pickle(current_job_dir / "prepared.pkl")
        codes_df = pd.read_pickle(current_job_dir / "codes.pkl")
        codes = codes_df.to_numpy(dtype="int8")
        matches = strategy_matches(df, codes, strategy)
        equity = strategy_equity_payload(df, codes, strategy)
        return render_template(
            "strategy.html",
            job_id=job_id,
            strategy=strategy,
            matches=matches.to_dict("records"),
            columns=list(matches.columns),
            equity=equity,
        )

    @app.get("/job/<job_id>/results.csv")
    def download_results_csv(job_id: str) -> Any:
        current_job_dir = job_dir(jobs_root, job_id)
        result_payload = load_json(current_job_dir / "results.json")
        if not result_payload:
            flash("Сначала сгенерируйте стратегии.", "error")
            return redirect(url_for("job_page", job_id=job_id))
        df = _build_results_csv_dataframe(result_payload.get("results", []))
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False, encoding="utf-8-sig")
        return send_file(tmp.name, as_attachment=True, download_name=f"basket_strategies_{job_id}.csv")

    return app
