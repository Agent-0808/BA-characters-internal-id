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
    """学生（角色）数据，包含多个KivoWiki页面

    姓与名分离存储（不做拼接），字段后缀按数据可得性递减排列：jp > kr > en > tw > cn；
    无后缀字段为 kivo 字段（kivowiki 源数据，简体中文民译）。
    """
    id: int
    family_name: str
    given_name: str
    family_name_jp: str
    given_name_jp: str
    family_name_kr: str
    given_name_kr: str
    family_name_en: str
    given_name_en: str
    family_name_tw: str
    given_name_tw: str
    family_name_cn: str
    given_name_cn: str
    school_id: int
    pages: list[KivoWikiPage]

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式，用于JSON输出"""
        return {
            "id": self.id,
            "family_name": self.family_name,
            "given_name": self.given_name,
            "family_name_jp": self.family_name_jp,
            "given_name_jp": self.given_name_jp,
            "family_name_kr": self.family_name_kr,
            "given_name_kr": self.given_name_kr,
            "family_name_en": self.family_name_en,
            "given_name_en": self.given_name_en,
            "family_name_tw": self.family_name_tw,
            "given_name_tw": self.given_name_tw,
            "family_name_cn": self.family_name_cn,
            "given_name_cn": self.given_name_cn,
            "school_id": self.school_id,
            "pages": [p.to_dict() for p in self.pages]
        }


# --- CSV输出数据类（保留现有格式） ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类（31列，字段顺序即CSV列序）

    语言列按数据可得性递减排列（kivo → jp → kr → en → tw → cn），
    每种语言一组 full/family/given/skin 四元组（姓与名分离存储），
    family_x 为学生级姓氏，given_x 为学生级名字，skin_x 为页面级皮肤名；
    full_x 为便利派生列，由分量列按各语言的括号/空格规则拼接（规则与
    web/src/nameFormat.ts 保持一致，仅 kivo 组额外拼接 spine_remark），
    供外部消费直接使用，也可用分量列自行拼接。
    """
    file_id: str
    student_id: int
    page_id: int
    spine_id: int | None
    spine_remark: str
    full_kivo: str
    family_kivo: str
    given_kivo: str
    skin_kivo: str
    full_jp: str
    family_jp: str
    given_jp: str
    skin_jp: str
    full_kr: str
    family_kr: str
    given_kr: str
    skin_kr: str
    full_en: str
    family_en: str
    given_en: str
    skin_en: str
    full_tw: str
    family_tw: str
    given_tw: str
    skin_tw: str
    full_cn: str
    family_cn: str
    given_cn: str
    skin_cn: str
    school_id: int
    school_name: str


@dataclass
class SkippedRecord:
    """用于存储跳过的页面ID及其原因的类"""
    page_id: int = 0
    spine_id: int | None = None
    reason: str = ""
    spine_name: str | None = None
    spine_remark: str | None = None
    name: str = ""
    name_jp: str = ""
    name_en: str = ""
    school: int | str = ""
    school_name: str = ""
