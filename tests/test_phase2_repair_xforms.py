from __future__ import annotations

import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_core.phase2.repair.xforms import XFormPipeline, supported_transform_names


def test_xforms_pipeline_loads_yaml_and_applies_to_series(tmp_path: Path) -> None:
    yaml_path = tmp_path / "xform.yaml"
    yaml_path.write_text(
        "\n".join(
            [
                "schema: phase2_xforms.v1",
                "transforms:",
                "  - name: winsorize",
                "    params:",
                "      lower_q: 0.0",
                "      upper_q: 0.8",
                "  - name: rank",
                "    params:",
                "      pct: true",
                "      center: true",
                "  - name: zscore",
                "    params:",
                "      ddof: 0",
                "      clip_std: 3.0",
                "  - name: sign_flip",
                "    params: {}",
            ]
        ),
        encoding="utf-8",
    )

    series = pd.Series([1.0, 2.0, 100.0, np.nan])
    pipe = XFormPipeline.from_yaml(yaml_path)
    out = pipe.apply(series)

    assert isinstance(out, pd.Series)
    assert len(out) == len(series)
    assert out.dropna().abs().max() <= 3.0


def test_xforms_pipeline_applies_lag_on_panel() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-01", "2026-01-02"]),
            "stock_id": ["A", "A", "B", "B"],
            "factor_value": [1.0, 2.0, 10.0, 20.0],
        }
    )
    pipe = XFormPipeline.from_specs([{"name": "lag", "params": {"periods": 1}}])
    out = pipe.apply(df)

    assert isinstance(out, pd.DataFrame)
    assert out["factor_value"].isna().sum() == 2


def test_xforms_pipeline_no_groupby_apply_deprecation_warning() -> None:
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                [
                    "2026-01-02",
                    "2026-01-01",
                    "2026-01-03",
                    "2026-01-01",
                    "2026-01-03",
                    "2026-01-02",
                ]
            ),
            "stock_id": ["A", "A", "A", "B", "B", "B"],
            "factor_value": [2.0, 1.0, 3.0, 20.0, 30.0, 10.0],
        }
    )
    pipe = XFormPipeline.from_specs(
        [
            {"name": "rank", "params": {"pct": True, "center": True}},
            {"name": "lag", "params": {"periods": 1}},
            {"name": "smooth", "params": {"window": 2, "min_periods": 1}},
        ]
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = pipe.apply(df)

    dep_warnings = [
        w
        for w in caught
        if issubclass(w.category, DeprecationWarning)
        and "DataFrameGroupBy.apply" in str(w.message)
    ]
    assert dep_warnings == []
    assert isinstance(out, pd.DataFrame)


def test_supported_transform_names_matches_pipeline_registry() -> None:
    names = supported_transform_names()
    assert names == sorted(names)
    assert "winsorize" in names
    assert "clip" in names
    assert "rank" in names
    assert "zscore" in names
    assert "fillna" in names
    assert "sign_flip" in names
    assert "lag" in names
    assert "smooth" in names
