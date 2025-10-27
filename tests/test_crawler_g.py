import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from crawler import parse_berth_g_html, BerthGData


def test_parse_berth_g_sample_html():
    html = Path("tests/samples/berth_g_sample.html").read_text(encoding="utf-8")
    result = parse_berth_g_html(html)
    assert isinstance(result, BerthGData)
    assert isinstance(result.blocks, pd.DataFrame)
    assert isinstance(result.calendar, pd.DataFrame)

    assert len(result.blocks) == 4

    first = result.blocks.iloc[0]
    assert first["vessel_name"] == "POS SHANGHAI"
    assert first["info_label"] == "SPSO-40(S)"
    assert first["eta_display"] == "14"
    assert first["etd_display"] == "09"
    assert first["background_color"] == "rgb(254,209,254)"
    assert first["info_lines"] == ["도선"]

    # 캘린더 파싱 확인
    assert len(result.calendar) == 4
    berth1_oct25 = result.calendar[
        (result.calendar["berth_label"] == "1 ( 9 )")
        & (result.calendar["date_label"] == "2025.10.25 (SAT)")
    ].iloc[0]
    assert berth1_oct25["cell_class"] == "bg_color13"
    assert berth1_oct25["cell_text"] is None or berth1_oct25["cell_text"] == ""
