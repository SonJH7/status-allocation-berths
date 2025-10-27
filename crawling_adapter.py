"""crawling 폴더 내 외부 스크립트를 동적으로 로드하여 활용하기 위한 어댑터."""
from __future__ import annotations

import importlib.util
import sys
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd


@lru_cache(maxsize=1)
def _load_crawling_main() -> ModuleType:
    """crawling/crawling/main.py 모듈을 동적으로 로드한다."""

    base_dir = Path(__file__).resolve().parent
    module_path = base_dir / "crawling" / "crawling" / "main.py"
    if not module_path.is_file():
        raise FileNotFoundError(f"crawling 모듈을 찾을 수 없습니다: {module_path}")

    module_dir = str(module_path.parent)
    path_added = False
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
        path_added = True

    try:
        spec = importlib.util.spec_from_file_location("crawling_main", module_path)
        if spec is None or spec.loader is None:
            raise ImportError("crawling 메인 모듈 spec을 생성할 수 없습니다.")

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        if path_added and module_dir in sys.path:
            sys.path.remove(module_dir)


def collect_crawling_dataframe(*, time: str = "3days", route: str = "ALL", berth: str = "A", debug: bool = False) -> pd.DataFrame:
    """외부 crawling 패키지의 collect_berth_info 함수를 호출하여 DataFrame을 반환한다."""

    module = _load_crawling_main()
    if not hasattr(module, "collect_berth_info"):
        raise AttributeError("collect_berth_info 함수가 crawling 모듈에 존재하지 않습니다.")

    collect = getattr(module, "collect_berth_info")
    df: Any = collect(time=time, route=route, berth=berth, debug=debug)
    if not isinstance(df, pd.DataFrame):
        raise TypeError("collect_berth_info 결과가 pandas.DataFrame이 아닙니다.")
    return df
