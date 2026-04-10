from __future__ import annotations

import tempfile
import threading
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, flash, jsonify, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from .engine import SearchParams, search_strategies, strategy_matches
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


def create_app() -> Flask:
    root_dir = Path(__file__).resolve().parents[1]
    instance_path = root_dir / "instance"
    jobs_root = instance_path / "jobs"

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
        return render_template("index.html")

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
        summary = {
            "strategies": len(results),
            "best_profit": max((row["profit"] for row in results), default=0),
            "best_roi": max((row["roi"] for row in results), default=0),
            "best_test_roi": max((row.get("test", {}).get("roi", 0) for row in results), default=0),
        }
        return render_template("results.html", meta=meta, params=params, results=results, summary=summary, job_id=job_id)

    @app.get("/job/<job_id>/strategy/<int:strategy_id>")
    def strategy_page(job_id: str, strategy_id: int) -> str:
        current_job_dir = job_dir(jobs_root, job_id)
        result_payload = load_json(current_job_dir / "results.json")
        pairs_payload = load_json(current_job_dir / "selected_pairs.json")
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
        selected_pairs = [_deserialize_feature(item) for item in pairs_payload.get("pairs", [])]
        matches = strategy_matches(df, codes, strategy)
        return render_template(
            "strategy.html",
            job_id=job_id,
            strategy=strategy,
            matches=matches.to_dict("records"),
            columns=list(matches.columns),
        )

    @app.get("/job/<job_id>/results.csv")
    def download_results_csv(job_id: str) -> Any:
        current_job_dir = job_dir(jobs_root, job_id)
        result_payload = load_json(current_job_dir / "results.json")
        if not result_payload:
            flash("Сначала сгенерируйте стратегии.", "error")
            return redirect(url_for("job_page", job_id=job_id))
        rows = []
        for row in result_payload.get("results", []):
            flat = {k: v for k, v in row.items() if k not in {"pattern_digits", "monthly_pnl", "train", "test"}}
            flat.update({f"train_{k}": v for k, v in row.get("train", {}).items()})
            flat.update({f"test_{k}": v for k, v in row.get("test", {}).items()})
            flat["monthly_pnl"] = "; ".join(f"{k}: {v}" for k, v in row.get("monthly_pnl", {}).items())
            rows.append(flat)
        df = pd.DataFrame(rows)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df.to_csv(tmp.name, index=False, encoding="utf-8-sig")
        return send_file(tmp.name, as_attachment=True, download_name=f"basket_strategies_{job_id}.csv")

    return app
