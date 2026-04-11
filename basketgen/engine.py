from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd

from .excel import FeaturePair


@dataclass(slots=True)
class SearchParams:
    min_bets: int = 300
    top_k: int = 200
    holdout_days: int = 60
    min_used_features: int = 2
    candidate_multiplier: int = 4



def _weights(feature_count: int) -> np.ndarray:
    return (3 ** np.arange(feature_count)).astype(np.int64)



def encode_rows(codes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    weights = _weights(codes.shape[1])
    ids = (codes.astype(np.int64) * weights).sum(axis=1)
    return ids, weights



def decode_pattern(pattern_id: int, feature_count: int) -> list[int]:
    digits: list[int] = []
    value = int(pattern_id)
    for _ in range(feature_count):
        digits.append(value % 3)
        value //= 3
    return digits



def used_features_count(pattern_id: int, feature_count: int) -> int:
    used = 0
    value = int(pattern_id)
    for _ in range(feature_count):
        used += int((value % 3) != 0)
        value //= 3
    return used



def wildcard_transform(vector: np.ndarray, feature_count: int) -> np.ndarray:
    arr = vector.reshape((3,) * feature_count)
    for axis in range(feature_count):
        moved = np.moveaxis(arr, axis, 0)
        arr = np.stack([moved.sum(axis=0), moved[1], moved[2]], axis=0)
        arr = np.moveaxis(arr, 0, axis)
    return arr.reshape(-1)


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def calc_profit_dd_ratio(profit: float, max_drawdown: float) -> float | None:
    dd_abs = abs(float(max_drawdown))
    if dd_abs <= 1e-9:
        return None
    return float(profit) / dd_abs


def calc_score(roi_test: float, profit_dd_ratio: float | None, max_drawdown_pct: float | None, matches: int) -> float:
    roi_component = float(np.clip(roi_test, -200.0, 200.0))
    ratio_component = 0.0 if profit_dd_ratio is None else float(np.tanh(float(profit_dd_ratio) / 4.0) * 100.0)
    dd_component = -25.0 if max_drawdown_pct is None else float(max(-100.0, 100.0 - (float(max_drawdown_pct) * 2.0)))
    stability_component = min(max(int(matches), 0) / 400.0, 1.0) * 100.0

    score = (roi_component * 0.5) + (ratio_component * 0.2) + (dd_component * 0.2) + (stability_component * 0.1)
    if matches < 150:
        score -= float((150 - matches) * 0.15)
    return round(float(score), 2)



def calc_drawdown(profits: pd.Series, dates: pd.Series) -> tuple[float, int, float | None]:
    if profits.empty:
        return 0.0, 0, None
    equity = profits.cumsum()
    rolling_max = equity.cummax()
    drawdowns = equity - rolling_max
    max_drawdown = float(drawdowns.min())
    min_idx = int(drawdowns.to_numpy(dtype=float).argmin())
    peak_balance = float(rolling_max.iloc[min_idx]) if len(rolling_max) else 0.0
    if max_drawdown == 0.0:
        max_drawdown_pct: float | None = 0.0
    elif peak_balance > 0:
        max_drawdown_pct = abs(max_drawdown) / peak_balance * 100.0
    else:
        max_drawdown_pct = None

    in_dd = drawdowns < 0
    max_days = 0
    start_date = None
    for date, flag in zip(dates, in_dd):
        if flag and start_date is None:
            start_date = date
        elif not flag and start_date is not None:
            max_days = max(max_days, int((date - start_date).days))
            start_date = None
    if start_date is not None:
        max_days = max(max_days, int((dates.iloc[-1] - start_date).days))
    return max_drawdown, max_days, max_drawdown_pct



def calc_streaks(outcomes: pd.Series) -> tuple[int, int]:
    max_win = 0
    max_loss = 0
    cur_win = 0
    cur_loss = 0
    for outcome in outcomes:
        if outcome == "win":
            cur_win += 1
            cur_loss = 0
        elif outcome == "loss":
            cur_loss += 1
            cur_win = 0
        else:
            cur_win = 0
            cur_loss = 0
        max_win = max(max_win, cur_win)
        max_loss = max(max_loss, cur_loss)
    return max_win, max_loss



def human_rule(digits: list[int], pairs: list[FeaturePair]) -> str:
    parts: list[str] = []
    mapping = {0: "X", 1: "1", 2: "2"}
    for digit, pair in zip(digits, pairs):
        if digit:
            parts.append(f"{pair.name}={mapping[digit]}")
    return " · ".join(parts) if parts else "Все X"



def matches_mask(codes: np.ndarray, digits: list[int]) -> np.ndarray:
    mask = np.ones(codes.shape[0], dtype=bool)
    for idx, digit in enumerate(digits):
        if digit:
            mask &= codes[:, idx] == digit
    return mask



def build_monthly_stats(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    grouped = (
        df.groupby(df["Дата"].dt.to_period("M"))
        .agg(
            matches=("Прибыль/убыток", "size"),
            profit=("Прибыль/убыток", "sum"),
            stake=("stake", "sum"),
        )
        .sort_index()
    )

    payload: list[dict[str, Any]] = []
    for period, row in grouped.iterrows():
        profit = float(row["profit"])
        stake = float(row["stake"])
        roi = (profit / stake * 100.0) if stake > 0 else 0.0
        payload.append(
            {
                "month": period.strftime("%m-%Y"),
                "matches": int(row["matches"]),
                "profit": round(profit, 2),
                "roi": round(roi, 2),
            }
        )
    return payload


def build_monthly_pnl(df: pd.DataFrame) -> dict[str, float]:
    return {row["month"]: row["profit"] for row in build_monthly_stats(df)}



def summarize_subset(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "profit": 0.0,
            "roi": 0.0,
            "matches": 0,
            "wins": 0,
            "losses": 0,
            "pushes": 0,
            "win_rate": 0.0,
            "max_drawdown": 0.0,
            "max_drawdown_pct": None,
            "profit_dd_ratio": None,
        }
    stake = float(df["stake"].sum())
    profit = float(df["Прибыль/убыток"].sum())
    matches = int(len(df))
    wins = int((df["outcome"] == "win").sum())
    losses = int((df["outcome"] == "loss").sum())
    pushes = int((df["outcome"] == "push").sum())
    win_rate = float(wins / matches * 100.0) if matches else 0.0
    max_drawdown, _, max_drawdown_pct = calc_drawdown(df["Прибыль/убыток"], df["Дата"])
    profit_dd_ratio = calc_profit_dd_ratio(profit, max_drawdown)

    return {
        "profit": round(profit, 2),
        "roi": round((profit / stake * 100.0) if stake > 0 else 0.0, 2),
        "matches": matches,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": round(win_rate, 2),
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_pct": _round_or_none(max_drawdown_pct, 2),
        "profit_dd_ratio": _round_or_none(profit_dd_ratio, 2),
    }


def _strategy_matched_rows(df: pd.DataFrame, codes: np.ndarray, digits: list[int]) -> pd.DataFrame:
    mask = matches_mask(codes, digits)
    return df.loc[mask].copy().sort_values("Дата").reset_index(drop=True)


def build_equity_payload(matched: pd.DataFrame, max_points: int = 1200) -> dict[str, Any]:
    if matched.empty:
        return {"granularity": "match", "points": []}

    granularity = "match"
    series = matched.set_index("Дата")["Прибыль/убыток"]
    if len(series) > max_points:
        granularity = "day"
        series = matched.groupby(matched["Дата"].dt.floor("D"))["Прибыль/убыток"].sum()
    if len(series) > max_points:
        granularity = "month"
        monthly = matched.groupby(matched["Дата"].dt.to_period("M"))["Прибыль/убыток"].sum()
        monthly.index = monthly.index.to_timestamp()
        series = monthly

    equity = series.cumsum()
    points: list[dict[str, Any]] = []
    for dt_value, amount in equity.items():
        if granularity == "match":
            label = pd.Timestamp(dt_value).strftime("%Y-%m-%d %H:%M")
        else:
            label = pd.Timestamp(dt_value).strftime("%Y-%m-%d")
        points.append({"label": label, "value": round(float(amount), 2)})

    return {"granularity": granularity, "points": points}



def enrich_strategy(df: pd.DataFrame, codes: np.ndarray, pattern_id: int, pairs: list[FeaturePair], holdout_days: int) -> dict[str, Any]:
    digits = decode_pattern(pattern_id, len(pairs))
    matched = _strategy_matched_rows(df, codes, digits)

    matches = int(len(matched))
    profit = float(matched["Прибыль/убыток"].sum())
    stake = float(matched["stake"].sum())
    avg_odds = float(matched["Коэффициент"].mean()) if matches else 0.0
    win_count = int((matched["outcome"] == "win").sum())
    loss_count = int((matched["outcome"] == "loss").sum())
    push_count = int((matched["outcome"] == "push").sum())
    win_rate = float(win_count / matches * 100.0) if matches else 0.0
    roi = float(profit / stake * 100.0) if stake > 0 else 0.0
    max_win_streak, max_loss_streak = calc_streaks(matched["outcome"]) if matches else (0, 0)
    if matches:
        max_drawdown, max_drawdown_days, max_drawdown_pct = calc_drawdown(matched["Прибыль/убыток"], matched["Дата"])
    else:
        max_drawdown, max_drawdown_days, max_drawdown_pct = 0.0, 0, None
    profit_dd_ratio = calc_profit_dd_ratio(profit, max_drawdown)

    cutoff = matched["Дата"].max() - timedelta(days=holdout_days) if matches else None
    train = matched[matched["Дата"] < cutoff] if matches and cutoff is not None else matched.iloc[0:0]
    test = matched[matched["Дата"] >= cutoff] if matches and cutoff is not None else matched.iloc[0:0]

    train_summary = summarize_subset(train)
    test_summary = summarize_subset(test)
    score = calc_score(test_summary["roi"], profit_dd_ratio, max_drawdown_pct, matches)
    monthly_stats = build_monthly_stats(matched)

    strategy = {
        "strategy_id": int(pattern_id),
        "rule": human_rule(digits, pairs),
        "matches": matches,
        "profit": round(profit, 2),
        "roi": round(roi, 2),
        "avg_odds": round(avg_odds, 3),
        "win_rate": round(win_rate, 2),
        "wins": win_count,
        "losses": loss_count,
        "pushes": push_count,
        "plus_streak": max_win_streak,
        "minus_streak": max_loss_streak,
        "max_drawdown": round(max_drawdown, 2),
        "max_drawdown_pct": _round_or_none(max_drawdown_pct, 2),
        "profit_dd_ratio": _round_or_none(profit_dd_ratio, 2),
        "max_drawdown_days": int(max_drawdown_days),
        "score": score,
        "used_features": int(sum(d != 0 for d in digits)),
        "pattern_digits": digits,
        "monthly_pnl": {row["month"]: row["profit"] for row in monthly_stats},
        "monthly_stats": monthly_stats,
        "train": train_summary,
        "test": test_summary,
    }
    return strategy



def search_strategies(df: pd.DataFrame, codes: np.ndarray, pairs: list[FeaturePair], params: SearchParams) -> list[dict[str, Any]]:
    feature_count = codes.shape[1]
    if feature_count == 0:
        return []
    if feature_count > 12:
        raise ValueError("Для генерации в браузере лучше использовать не больше 12 признаков за один запуск.")

    row_ids, _ = encode_rows(codes)
    state_count = 3 ** feature_count
    profits = df["Прибыль/убыток"].to_numpy(dtype=float)
    stakes = df["stake"].to_numpy(dtype=float)
    odds = df["Коэффициент"].to_numpy(dtype=float)
    outcomes = df["outcome"].to_numpy(dtype=object)

    counts = np.bincount(row_ids, minlength=state_count).astype(np.int32)
    profit_sum = np.bincount(row_ids, weights=profits, minlength=state_count)
    stake_sum = np.bincount(row_ids, weights=stakes, minlength=state_count)
    odds_sum = np.bincount(row_ids, weights=odds, minlength=state_count)
    wins = np.bincount(row_ids, weights=(outcomes == "win").astype(np.int16), minlength=state_count)
    losses = np.bincount(row_ids, weights=(outcomes == "loss").astype(np.int16), minlength=state_count)
    pushes = np.bincount(row_ids, weights=(outcomes == "push").astype(np.int16), minlength=state_count)

    w_counts = wildcard_transform(counts, feature_count)
    w_profit = wildcard_transform(profit_sum, feature_count)
    w_stake = wildcard_transform(stake_sum, feature_count)
    w_odds = wildcard_transform(odds_sum, feature_count)
    w_wins = wildcard_transform(wins, feature_count)
    w_losses = wildcard_transform(losses, feature_count)
    w_pushes = wildcard_transform(pushes, feature_count)

    roi = np.divide(w_profit, w_stake, out=np.zeros_like(w_profit, dtype=float), where=w_stake > 0) * 100.0
    avg_odds = np.divide(w_odds, w_counts, out=np.zeros_like(w_odds, dtype=float), where=w_counts > 0)
    win_rate = np.divide(w_wins, w_counts, out=np.zeros_like(w_counts, dtype=float), where=w_counts > 0) * 100.0
    score = roi * np.log1p(w_counts)

    valid_ids: list[int] = []
    for pattern_id in range(state_count):
        if w_counts[pattern_id] < params.min_bets:
            continue
        if used_features_count(pattern_id, feature_count) < params.min_used_features:
            continue
        valid_ids.append(pattern_id)

    if not valid_ids:
        return []

    valid_ids_arr = np.array(valid_ids, dtype=np.int64)
    ranked = valid_ids_arr[np.argsort(score[valid_ids_arr])[::-1]]
    candidate_count = min(len(ranked), max(params.top_k * params.candidate_multiplier, params.top_k))
    candidate_ids = ranked[:candidate_count]

    results: list[dict[str, Any]] = []
    for pattern_id in candidate_ids:
        item = enrich_strategy(df, codes, int(pattern_id), pairs, params.holdout_days)
        item["quick_matches"] = int(w_counts[pattern_id])
        item["quick_profit"] = round(float(w_profit[pattern_id]), 2)
        item["quick_roi"] = round(float(roi[pattern_id]), 2)
        item["quick_avg_odds"] = round(float(avg_odds[pattern_id]), 3)
        item["quick_win_rate"] = round(float(win_rate[pattern_id]), 2)
        item["quick_wins"] = int(w_wins[pattern_id])
        item["quick_losses"] = int(w_losses[pattern_id])
        item["quick_pushes"] = int(w_pushes[pattern_id])
        results.append(item)

    def ratio_for_sort(item: dict[str, Any]) -> float:
        value = item.get("profit_dd_ratio")
        return float("-inf") if value is None else float(value)

    results.sort(
        key=lambda row: (
            row.get("score", 0.0),
            row.get("test", {}).get("roi", 0.0),
            ratio_for_sort(row),
            row.get("roi", 0.0),
            row.get("matches", 0),
        ),
        reverse=True,
    )
    return results[: params.top_k]


def strategy_equity_payload(df: pd.DataFrame, codes: np.ndarray, strategy: dict[str, Any], max_points: int = 1200) -> dict[str, Any]:
    digits = list(strategy["pattern_digits"])
    matched = _strategy_matched_rows(df, codes, digits)
    return build_equity_payload(matched, max_points=max_points)



def strategy_matches(df: pd.DataFrame, codes: np.ndarray, strategy: dict[str, Any]) -> pd.DataFrame:
    digits = list(strategy["pattern_digits"])
    matched = _strategy_matched_rows(df, codes, digits)
    cols = [
        "Дата",
        "Чемпионат",
        'Команда "Хозяева"',
        'Команда "Гости"',
        "Прогноз",
        "Коэффициент",
        "Результат",
        "Прибыль/убыток",
    ]
    out = matched.loc[:, cols].copy().sort_values("Дата", ascending=False)
    out["Дата"] = out["Дата"].dt.strftime("%d.%m.%Y %H:%M")
    return out.reset_index(drop=True)
