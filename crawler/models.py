"""
数据模型定义
"""

from dataclasses import dataclass, fields, astuple
from typing import Any


# --- 工具函数 ---

def strip_key(data: dict[str, Any], key: str) -> None:
    """对字典中指定键的值进行处理，如果键存在且字符串长度超过10则执行strip操作，否则保留原样"""
    if key in data:
        if not data[key]:
            data[key] = []
        elif isinstance(data[key], str) and len(data[key]) > 10:
            data[key] = "(stripped)"
        elif isinstance(data[key], list) and len(data[key]) > 0:
            data[key] = "(stripped)"


def remove_key(data: dict[str, Any], key: str) -> None:
    """从字典中删除指定的键"""
    data.pop(key, None)


# --- 中间输出数据类 ---

@dataclass
class School:
    """学校数据"""
    id: int
    name: str
    name_cn: str
    logo: str = ""


@dataclass
class Spine:
    """Spine动画数据"""
    id: int
    name: str
    remark: str
    type: str


@dataclass
class KivoWikiPage:
    """KivoWiki页面数据（皮肤名字段按数据可得性递减排列：jp > kr > en > tw > cn）"""
    page_id: int
    skin: str
    skin_jp: str
    skin_kr: str
    skin_en: str
    skin_tw: str
    skin_cn: str
    avatar: str
    spines: list[int]
    is_install: bool = False
    is_install_cn: bool = False
    is_install_global: bool = False
    is_npc: bool = False
    rarity: int = 0
    limited: bool = False
    sd_model_image: str = ""  # SD模型立绘 URL（页面级资产，缓存未更新时为空）
    recollection_lobby_image: str = ""  # 回忆大厅背景 URL（页面级资产）
    game_id: int = 0  # API character_datas.character_id，游戏机制侧 ID（非资产 ID）
    dev_name: str = ""  # API character_datas.dev_name，部分与主file_id一致，用途未知

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式"""
        return {
            "page_id": self.page_id,
            "skin": self.skin,
            "skin_jp": self.skin_jp,
            "skin_kr": self.skin_kr,
            "skin_en": self.skin_en,
            "skin_tw": self.skin_tw,
            "skin_cn": self.skin_cn,
            "avatar": self.avatar,
            "spines": self.spines,
            "is_install": self.is_install,
            "is_install_cn": self.is_install_cn,
            "is_install_global": self.is_install_global,
            "is_npc": self.is_npc,
            "rarity": self.rarity,
            "limited": self.limited,
            "sd_model_image": self.sd_model_image,
            "recollection_lobby_image": self.recollection_lobby_image,
            "game_id": self.game_id,
            "dev_name": self.dev_name
        }


@dataclass
class Student:
    """学生（角色）数据，包含多个KivoWiki页面（名字段按数据可得性递减排列：jp > kr > en > tw > cn）"""
    id: int
    name: str
    name_jp: str
    name_kr: str
    name_en: str
    name_tw: str
    name_cn: str
    school_id: int
    pages: list[KivoWikiPage]

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式，用于JSON输出"""
        return {
            "id": self.id,
            "name": self.name,
            "name_jp": self.name_jp,
            "name_kr": self.name_kr,
            "name_en": self.name_en,
            "name_tw": self.name_tw,
            "name_cn": self.name_cn,
            "school_id": self.school_id,
            "pages": [p.to_dict() for p in self.pages]
        }


# --- CSV输出数据类（保留现有格式） ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类（25列，字段顺序即CSV列序）

    语言列按数据可得性递减排列（kivo → jp → kr → en → tw → cn），
    每种语言一组 full/name/skin 三元组：full = name + skin（仅 full_kivo 额外拼入 spine_remark），
    name_x 为学生级基础名（不含皮肤名），skin_x 为页面级皮肤名。
    """
    file_id: str
    student_id: int
    page_id: int
    spine_id: int | None
    spine_remark: str
    full_kivo: str
    name_kivo: str
    skin_kivo: str
    full_jp: str
    name_jp: str
    skin_jp: str
    full_kr: str
    name_kr: str
    skin_kr: str
    full_en: str
    name_en: str
    skin_en: str
    full_tw: str
    name_tw: str
    skin_tw: str
    full_cn: str
    name_cn: str
    skin_cn: str
    school_id: int
    school_name: str


@dataclass
class SkippedRecord:
    """用于存储跳过的ID及其原因的类"""
    student_id: int = 0
    spine_id: int | None = None
    reason: str = ""
    spine_name: str | None = None
    spine_remark: str | None = None
    name: str = ""
    name_jp: str = ""
    name_en: str = ""
    school: int | str = ""
    school_name: str = ""
