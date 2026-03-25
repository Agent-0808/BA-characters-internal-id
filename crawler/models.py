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


@dataclass
class Spine:
    """Spine动画数据"""
    id: int
    name: str
    remark: str
    type: str


@dataclass
class KivoWikiPage:
    """KivoWiki页面数据"""
    kivowiki_id: int
    skin_name: str
    skin_name_cn: str
    avatar: str
    spines: list[int]


@dataclass
class Student:
    """学生（角色）数据，包含多个KivoWiki页面"""
    id: int
    name: str
    name_cn: str
    name_jp: str
    name_en: str
    name_kr: str
    name_tw: str
    school_id: int
    pages: list[KivoWikiPage]

    def to_dict(self) -> dict[str, Any]:
        """转换为字典格式，用于JSON输出"""
        return {
            "id": self.id,
            "name": self.name,
            "name_cn": self.name_cn,
            "name_jp": self.name_jp,
            "name_en": self.name_en,
            "name_kr": self.name_kr,
            "name_tw": self.name_tw,
            "school_id": self.school_id,
            "pages": [
                {
                    "kivowiki_id": p.kivowiki_id,
                    "skin_name": p.skin_name,
                    "skin_name_cn": p.skin_name_cn,
                    "avatar": p.avatar,
                    "spines": p.spines
                }
                for p in self.pages
            ]
        }


# --- CSV输出数据类（保留现有格式） ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类"""
    file_id: str
    char_id: int
    spine_id: int | None
    full_name: str
    name: str
    skin_name: str
    name_cn: str
    name_jp: str
    name_tw: str
    name_en: str
    name_kr: str
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
