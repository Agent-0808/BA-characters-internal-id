"""
皮肤名翻译词表查询模块

词表为手动维护文件（随 main 分支代码走，勿写入爬虫产物），
键为 API 的 `skin` 值，值为 `{"en": ..., "kr": ...}` 多语言字典。
未覆盖的词条返回空串，由调用方在启动时汇总告警。
"""

import json
import logging
from functools import cache
from pathlib import Path

from .config import SKIN_MAP_FILENAME


@cache
def get_skin_map() -> dict[str, dict[str, str]]:
    """加载皮肤名翻译词表（带缓存），缺失时返回空 dict 并告警"""
    filepath = Path(__file__).parent / SKIN_MAP_FILENAME
    if not filepath.exists():
        logging.warning(f"皮肤名翻译词表不存在: {filepath}，skin_en / skin_kr 将输出空串")
        return {}
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def translate_skin(skin: str, lang: str) -> str:
    """翻译皮肤名，空 `skin` 或未覆盖的词条返回空串"""
    if not skin:
        return ""
    return get_skin_map().get(skin, {}).get(lang, "")
