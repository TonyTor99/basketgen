from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import openpyxl
import pandas as pd


ODDS_LABELS = {
    "П1", "X", "П2", "ТБ", "ТМ", "ФОРА", "Инд.тотал 1", "Инд.тотал 2",
    "КФ ТБ", "КФ ТМ", "КФ ФОРА1", "КФ ФОРА2", "КФ ИТБ1", "КФ ИТМ1",
    "КФ ИТБ2", "КФ ИТМ2", "1-я полов. ТБ", "1-я полов. КФ ТБ",
    "1-я полов. ТМ", "1-я полов. КФ ТМ", "ФОРА1", "ФОРА2", "ИТБ1",
    "ИТМ1", "ИТБ2", "ИТМ2",
}

EXCLUDE_FEATURE_TERMS = [
    "3 четв",
    "4 четв",
    "итоговый счет",
    "текущий банк",
    "max ставка",
    "размер ставки",
]

DEFAULT_FEATURE_PRIORITY = [
    "Голы",
    "Фолы",
    "2-х очковые",
    "3-х очковые",
    "Штраф броски",
    "Реализац атак, %",
    "1 четв",
    "2 четв",
    "Ост таймаут",
]


@dataclass(slots=True)
class FeaturePair:
    name: str
    home_col: str
    away_col: str
    completeness: float


@dataclass(slots=True)
class HeaderInfo:
    names: list[str]
    index_by_name: dict[str, int]


class ExcelFormatError(RuntimeError):
    pass



def build_header_info(path: Path) -> HeaderInfo:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    row0 = [v for v in next(ws.iter_rows(min_row=1, max_row=1, values_only=True))]
    row1 = [v for v in next(ws.iter_rows(min_row=2, max_row=2, values_only=True))]
    wb.close()

    groups: list[str | None] = []
    current_group: str | None = None
    for value in row0:
        if value not in (None, ""):
            current_group = str(value).strip()
        groups.append(current_group)

    base_names: list[str] = []
    for idx, (group, sub) in enumerate(zip(groups, row1)):
        sub_name = None if sub is None else str(sub).strip()
        group_name = None if group is None else str(group).strip()

        if sub_name:
            base = sub_name
            if group_name and group_name != sub_name and sub_name in ODDS_LABELS:
                base = f"{group_name}::{sub_name}"
        else:
            base = group_name or f"col_{idx}"
        base_names.append(base)

    counts: dict[str, int] = {}
    for name in base_names:
        counts[name] = counts.get(name, 0) + 1

    seen: dict[str, int] = {}
    final_names: list[str] = []
    for name in base_names:
        seen[name] = seen.get(name, 0) + 1
        final_names.append(name if counts[name] == 1 else f"{name}__{seen[name]}")

    return HeaderInfo(names=final_names, index_by_name={name: i for i, name in enumerate(final_names)})



def _pair_name(header: str) -> tuple[str, str] | None:
    match = re.match(r"^(.*?)(?:\sД)(.*)$", header)
    if not match:
        return None
    base, suffix = match.groups()
    return base, suffix



def detect_feature_pairs(path: Path, header_info: HeaderInfo | None = None, sample_rows: int = 8000) -> list[FeaturePair]:
    header_info = header_info or build_header_info(path)
    pairs_meta: list[tuple[str, str, str]] = []
    for header in header_info.names:
        pair = _pair_name(header)
        if not pair:
            continue
        base, suffix = pair
        away_col = f"{base} Г{suffix}"
        if away_col not in header_info.index_by_name:
            continue
        feature_name = f"{base}{suffix}".strip()
        lowered = feature_name.lower()
        if any(term in lowered for term in EXCLUDE_FEATURE_TERMS):
            continue
        pairs_meta.append((feature_name, header, away_col))

    pairs_meta = list(dict.fromkeys(pairs_meta))
    if not pairs_meta:
        return []

    sample_cols = [home for _, home, _ in pairs_meta] + [away for _, _, away in pairs_meta]
    sample_df = read_subset(path, header_info, sample_cols, nrows=sample_rows)

    features: list[FeaturePair] = []
    for name, home, away in pairs_meta:
        home_vals = pd.to_numeric(sample_df[home], errors="coerce")
        away_vals = pd.to_numeric(sample_df[away], errors="coerce")
        completeness = float((~(home_vals.isna() | away_vals.isna())).mean())
        label = "Очки на момент сигнала" if name == "Голы" else name
        features.append(FeaturePair(name=label, home_col=home, away_col=away, completeness=completeness))

    def feature_key(item: FeaturePair) -> tuple[int, float, str]:
        try:
            priority = DEFAULT_FEATURE_PRIORITY.index(item.name)
        except ValueError:
            priority = 999
        return (priority, -item.completeness, item.name)

    return sorted(features, key=feature_key)



def read_subset(path: Path, header_info: HeaderInfo, columns: Iterable[str], nrows: int | None = None) -> pd.DataFrame:
    selected = list(dict.fromkeys(columns))
    missing = [col for col in selected if col not in header_info.index_by_name]
    if missing:
        raise ExcelFormatError(f"Не найдены колонки: {', '.join(missing)}")

    usecols = [header_info.index_by_name[col] for col in selected]
    names = [header_info.names[i] for i in usecols]
    return pd.read_excel(
        path,
        header=1,
        names=names,
        usecols=usecols,
        nrows=nrows,
        engine="openpyxl",
    )



def estimate_stake(profit: float, odds: float, outcome: str) -> float:
    if pd.isna(profit) or pd.isna(odds):
        return math.nan
    if outcome == "loss":
        return abs(float(profit))
    if outcome == "win":
        return abs(float(profit)) / max(float(odds) - 1.0, 1e-9)
    if outcome == "push":
        return max(abs(float(profit)), 1.0)
    return math.nan



def normalize_outcome(result_value: object, profit: float) -> str:
    value = str(result_value).strip().lower()
    if "выиг" in value:
        return "win"
    if "проиг" in value:
        return "loss"
    if "возв" in value or "расч" in value:
        return "push"
    if pd.notna(profit):
        if profit > 0:
            return "win"
        if profit < 0:
            return "loss"
    return "push"



def build_codes(df: pd.DataFrame, pairs: list[FeaturePair]) -> np.ndarray:
    codes = np.zeros((len(df), len(pairs)), dtype=np.int8)
    for idx, pair in enumerate(pairs):
        home_vals = pd.to_numeric(df[pair.home_col], errors="coerce").to_numpy(dtype=float)
        away_vals = pd.to_numeric(df[pair.away_col], errors="coerce").to_numpy(dtype=float)
        codes[:, idx] = np.where(
            np.isnan(home_vals) | np.isnan(away_vals),
            0,
            np.where(home_vals > away_vals, 1, np.where(home_vals < away_vals, 2, 0)),
        )
    return codes



def prepare_dataset(
    path: Path,
    header_info: HeaderInfo,
    selected_pairs: list[FeaturePair],
    bot_filter: str = "",
    signal_time: int | None = 20,
    min_odds: float | None = None,
    max_odds: float | None = None,
) -> tuple[pd.DataFrame, np.ndarray]:
    base_cols = [
        "Дата",
        "Чемпионат",
        'Команда "Хозяева"',
        'Команда "Гости"',
        "Прогноз",
        "Коэффициент",
        "Результат",
        "Прибыль/убыток",
        "Бот",
        "Время сигнала",
    ]
    feature_cols = [pair.home_col for pair in selected_pairs] + [pair.away_col for pair in selected_pairs]
    df = read_subset(path, header_info, base_cols + feature_cols)

    df["Дата"] = pd.to_datetime(df["Дата"], dayfirst=True, errors="coerce")
    df["Коэффициент"] = pd.to_numeric(df["Коэффициент"], errors="coerce")
    df["Прибыль/убыток"] = pd.to_numeric(df["Прибыль/убыток"], errors="coerce")
    df["Время сигнала"] = pd.to_numeric(df["Время сигнала"], errors="coerce")

    for pair in selected_pairs:
        df[pair.home_col] = pd.to_numeric(df[pair.home_col], errors="coerce")
        df[pair.away_col] = pd.to_numeric(df[pair.away_col], errors="coerce")

    if bot_filter:
        df = df[df["Бот"].astype(str).str.contains(bot_filter, case=False, na=False)]
    if signal_time is not None:
        df = df[df["Время сигнала"].eq(signal_time)]
    if min_odds is not None:
        df = df[df["Коэффициент"] >= min_odds]
    if max_odds is not None:
        df = df[df["Коэффициент"] <= max_odds]

    df = df.dropna(subset=["Дата", "Коэффициент", "Прибыль/убыток"])
    df["outcome"] = [normalize_outcome(res, pnl) for res, pnl in zip(df["Результат"], df["Прибыль/убыток"])]
    df["stake"] = [estimate_stake(pnl, odds, outcome) for pnl, odds, outcome in zip(df["Прибыль/убыток"], df["Коэффициент"], df["outcome"])]
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["stake"])
    df = df.reset_index(drop=True)

    codes = build_codes(df, selected_pairs)
    return df, codes
