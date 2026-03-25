"""
缓存管理模块
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from .config import CACHE_DIR
from .models import strip_key, remove_key


class CacheManager:
    """负责本地数据的缓存管理"""

    def __init__(self, base_dir: Path = CACHE_DIR):
        self.base_dir = base_dir
        self.students_dir = base_dir / "students"
        self.spines_dir = base_dir / "spines"
        self.state_file = base_dir / "state.json"
        self.schools_cache_file = base_dir / "schools.json"
        self._ensure_dirs()

    def _ensure_dirs(self):
        """确保缓存目录存在"""
        self.students_dir.mkdir(parents=True, exist_ok=True)
        self.spines_dir.mkdir(parents=True, exist_ok=True)

    def _clean_student_data(self, json_data: dict[str, Any]) -> dict[str, Any]:
        """
        深度清洗学生数据，移除所有非ID/名称/Spine映射所需的字段。
        """
        if not json_data or 'data' not in json_data:
            return json_data

        data = json_data['data']
        if not isinstance(data, dict):
            return json_data

        # 1. 定义需要处理的字段
        # keys_to_remove: 完全移除的字段
        # keys_to_strip: 需要清洗的字段
        keys_to_remove = [
            # 大文本 / 列表
            'gallery', 'more', 
            'sd_model_image', 'avatar',
            'recollection_lobby_image',
            'introduction', 'introduction_cn',
            'voice_play_icon', 'voice_pause_icon',
            'source', 'contributor'
        ]

        keys_to_strip = ['voice', 'voice_cn', 'voice_kr']
        
        # 统一处理：先检查移除，再检查标记
        for key in keys_to_remove + keys_to_strip:
            if key in keys_to_remove:
                remove_key(data, key)
            elif key in keys_to_strip and key in data:
                # 如果列表存在且不为空，替换为标记
                strip_key(data, key)

        # 清洗 character_datas
        if 'character_datas' in data:
            for char_data in data['character_datas']:
                # 移除 character_datas 内部的冗余字段
                sub_keys_to_remove = [
                    'skill', 'cultivate_material', 'equipment', 
                    'basic',
                ]
                for key in sub_keys_to_remove:
                    remove_key(char_data, key)
                
                # 深度清洗 weapons 字段，移除嵌套的无用字段
                if 'weapons' in char_data and isinstance(char_data['weapons'], dict):
                    weapons_fields_to_remove = [
                        'icon', 'description', 'description_cn', 
                        'info', 'skill'
                    ]
                    for field in weapons_fields_to_remove:
                        remove_key(char_data['weapons'], field)

        return json_data

    async def get_student(self, student_id: int) -> dict | None:
        """从缓存读取学生数据"""
        file_path = self.students_dir / f"{student_id}.json"
        return await self._read_json(file_path)

    async def save_student(self, student_id: int, data: dict):
        """清洗并保存学生数据到缓存"""
        cleaned_data = self._clean_student_data(data)
        remove_key(cleaned_data, "time")
        file_path = self.students_dir / f"{student_id}.json"
        if cleaned_data:
            await self._write_json(file_path, cleaned_data)

    async def get_spine(self, spine_id: int) -> dict | None:
        """从缓存读取 Spine 数据"""
        file_path = self.spines_dir / f"{spine_id}.json"
        return await self._read_json(file_path)

    async def save_spine(self, spine_id: int, data: dict):
        """保存 Spine 数据到缓存 (Spine 数据通常较小，不做额外清洗)"""
        remove_key(data, "time")
        file_path = self.spines_dir / f"{spine_id}.json"
        await self._write_json(file_path, data)

    async def get_state(self) -> dict:
        """读取状态文件"""
        logging.info(f"正在读取状态文件: {self.state_file}")
        if state := await self._read_json(self.state_file):
            logging.info(f"成功读取状态文件。包含数据: {state}")
            return state
        logging.info("状态文件不存在或为空，返回默认状态")
        return {
            "max_student_id": 0,
            "max_spine_id": 0,
            "last_updated": None
        }

    async def save_state(self, max_student_id: int, max_spine_id: int):
        """保存状态文件"""
        state = {
            "max_student_id": max_student_id,
            "max_spine_id": max_spine_id,
            "last_updated": asyncio.get_event_loop().time()
        }
        await self._write_json(self.state_file, state)

    async def get_schools(self) -> dict[int, dict[str, Any]]:
        """从缓存读取学校数据"""
        if schools_data := await self._read_json(self.schools_cache_file):
            if schools_data.get("code") == 2000 and "data" in schools_data and "school" in schools_data["data"]:
                return {school["id"]: school for school in schools_data["data"]["school"]}
        return {}

    async def save_schools(self, schools_data: dict):
        """保存学校数据到缓存"""
        remove_key(schools_data, "time")
        await self._write_json(self.schools_cache_file, schools_data)

    def list_student_ids(self) -> list[int]:
        """列出所有缓存的学生ID"""
        student_ids = []
        for f in self.students_dir.glob("*.json"):
            try:
                student_ids.append(int(f.stem))
            except ValueError:
                continue
        return sorted(student_ids)

    def list_spine_ids(self) -> list[int]:
        """列出所有缓存的Spine ID"""
        spine_ids = []
        for f in self.spines_dir.glob("*.json"):
            try:
                spine_ids.append(int(f.stem))
            except ValueError:
                continue
        return sorted(spine_ids)

    async def _read_json(self, path: Path) -> dict | None:
        """异步读取 JSON 文件"""
        if not path.exists():
            return None
        try:
            return await asyncio.to_thread(self._read_json_sync, path)
        except Exception as e:
            logging.warning(f"读取缓存失败 {path}: {e}")
            return None

    def _read_json_sync(self, path: Path) -> dict:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async def _write_json(self, path: Path, data: dict):
        """异步写入紧凑格式 JSON"""
        try:
            await asyncio.to_thread(self._write_json_sync, path, data)
        except Exception as e:
            logging.error(f"写入缓存失败 {path}: {e}")

    def _write_json_sync(self, path: Path, data: dict):
        with open(path, 'w', encoding='utf-8') as f:
            # 使用 separators 生成紧凑的 JSON (无多余空格)
            json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
