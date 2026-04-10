from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
from flask import Flask, flash, redirect, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from .engine import SearchParams, search_strategies, strategy_matches
from .excel import FeaturePair, build_header_info, detect_feature_pairs, prepare_dataset
from .storage import copy_upload, create_job, job_dir, load_json, save_json


ALLOWED_EXTENSIONS = {"xlsx", "xlsm", "xls"}



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
    return FeaturePair(
        name=str(data["name"]),
        home_col=str(data["home_col"]),
        away_col=str(data["away_col"]),
        completeness=float(data.get("completeness", 0)) / 100.0 if float(data.get("completeness", 0)) > 1 else float(data.get("completeness", 0)),
    )



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
        save_json(
            current_job_dir / "meta.json",
            {
                "job_id": job_id,
                "filename": filename,
                "input_path": str(input_path),
                "features": [_serialize_feature(item) for item in features],
                "selected_feature_names": [item.name for item in features[:8]],
            },
        )
        return redirect(url_for("job_page", job_id=job_id))

    @app.get("/job/<job_id>")
    def job_page(job_id: str) -> str:
        meta = load_json(job_dir(jobs_root, job_id) / "meta.json")
        if not meta:
            flash("Job не найден.", "error")
            return redirect(url_for("index"))
        return render_template("job.html", meta=meta)

    @app.post("/job/<job_id>/generate")
    def generate(job_id: str) -> Any:
        current_job_dir = job_dir(jobs_root, job_id)
        meta = load_json(current_job_dir / "meta.json")
        if not meta:
            flash("Job не найден.", "error")
            return redirect(url_for("index"))

        input_path = Path(meta["input_path"])
        header_info = build_header_info(input_path)
        all_features = [_deserialize_feature(item) for item in meta.get("features", [])]
        selected_names = set(request.form.getlist("features"))
        selected_pairs = [item for item in all_features if item.name in selected_names]
        if not selected_pairs:
            flash("Выберите хотя бы один признак.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        bot_filter = (request.form.get("bot_filter") or "SB 1 Basket Fonbet").strip()
        time_signal_raw = (request.form.get("signal_time") or "20").strip()
        min_odds_raw = (request.form.get("min_odds") or "").strip()
        max_odds_raw = (request.form.get("max_odds") or "").strip()
        min_bets = int(request.form.get("min_bets") or 300)
        top_k = int(request.form.get("top_k") or 200)
        holdout_days = int(request.form.get("holdout_days") or 60)
        min_used_features = int(request.form.get("min_used_features") or 2)

        signal_time = int(time_signal_raw) if time_signal_raw else None
        min_odds = float(min_odds_raw) if min_odds_raw else None
        max_odds = float(max_odds_raw) if max_odds_raw else None

        df, codes = prepare_dataset(
            input_path,
            header_info,
            selected_pairs=selected_pairs,
            bot_filter=bot_filter,
            signal_time=signal_time,
            min_odds=min_odds,
            max_odds=max_odds,
        )
        if df.empty:
            flash("После фильтров не осталось матчей. Смягчите параметры.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        params = SearchParams(
            min_bets=min_bets,
            top_k=top_k,
            holdout_days=holdout_days,
            min_used_features=min_used_features,
        )
        results = search_strategies(df, codes, selected_pairs, params)
        if not results:
            flash("Стратегии не найдены. Попробуйте уменьшить min bets или выбрать меньше признаков.", "error")
            return redirect(url_for("job_page", job_id=job_id))

        df.to_pickle(current_job_dir / "prepared.pkl")
        pd.DataFrame(codes).to_pickle(current_job_dir / "codes.pkl")
        save_json(current_job_dir / "selected_pairs.json", {"pairs": [_serialize_feature(item) for item in selected_pairs]})
        save_json(current_job_dir / "results.json", {"results": results})
        save_json(
            current_job_dir / "run_params.json",
            {
                "bot_filter": bot_filter,
                "signal_time": signal_time,
                "min_odds": min_odds,
                "max_odds": max_odds,
                "min_bets": min_bets,
                "top_k": top_k,
                "holdout_days": holdout_days,
                "min_used_features": min_used_features,
                "source_rows": int(len(df)),
            },
        )
        return redirect(url_for("results_page", job_id=job_id))

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
