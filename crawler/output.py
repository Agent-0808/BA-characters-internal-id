"""
文件输出模块 - 支持JSON和CSV格式输出

流程：
1. 缓存 → 中间JSON（schools.json, students.json, spines.json）
2. 中间JSON → 最终CSV（students_data.csv）
"""

import csv
import json
import logging
import re
from dataclasses import fields, astuple
from pathlib import Path
from typing import Any

from .config import (
    OUTPUT_DIR,
    OUTPUT_FILENAME,
    SKIPPED_FILENAME,
    SCHOOLS_OUTPUT_FILENAME,
    STUDENTS_OUTPUT_FILENAME,
    SPINES_OUTPUT_FILENAME,
)
from .models import Student, School, Spine, KivoWikiPage, StudentForm, SkippedRecord


class OutputWriter:
    """负责将处理好的数据写入文件"""

    def __init__(self):
        # 确保输出目录存在
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    def _get_alternative_filename(self, original_filename: str) -> str:
        """生成备用文件名"""
        base, ext = original_filename.rsplit('.', 1)
        return f"{base}_backup.{ext}"

    # --- JSON 输出方法 ---

    def write_schools_json(self, schools: list[School]):
        """写入学校JSON文件"""
        if not schools:
            logging.warning("没有可供写入的学校数据。")
            return

        filepath = OUTPUT_DIR / SCHOOLS_OUTPUT_FILENAME
        data = [
            {"id": s.id, "name": s.name, "name_cn": s.name_cn, "logo": s.logo}
            for s in schools
        ]
        self._write_json(filepath, data)
        logging.info(f"学校数据成功写入 {filepath}，共 {len(schools)} 条记录")

    def write_students_json(self, students: list[Student]):
        """写入学生JSON文件"""
        if not students:
            logging.warning("没有可供写入的学生数据。")
            return

        filepath = OUTPUT_DIR / STUDENTS_OUTPUT_FILENAME
        data = [s.to_dict() for s in students]
        self._write_json(filepath, data)
        logging.info(f"学生数据成功写入 {filepath}，共 {len(students)} 条记录")

    def write_spines_json(self, spines: list[Spine]):
        """写入Spine JSON文件"""
        if not spines:
            logging.warning("没有可供写入的Spine数据。")
            return

        filepath = OUTPUT_DIR / SPINES_OUTPUT_FILENAME
        data = [
            {"id": s.id, "name": s.name, "remark": s.remark, "type": s.type}
            for s in spines
        ]
        self._write_json(filepath, data)
        logging.info(f"Spine数据成功写入 {filepath}，共 {len(spines)} 条记录")

    def _write_json(self, filepath: Path, data: Any):
        """写入JSON文件"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except IOError as e:
            logging.error(f"写入文件 {filepath} 时发生错误: {e}")

    # --- CSV 输出方法（保留现有格式） ---

    def write_students_csv(self, data: list[StudentForm]):
        """将StudentForm列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的数据。")
            return

        full_path = OUTPUT_DIR / OUTPUT_FILENAME
        filenames_to_try = [full_path, OUTPUT_DIR / self._get_alternative_filename(OUTPUT_FILENAME)]

        for filepath in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条记录写入到 {filepath}...")
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    # 获取dataclass的字段名作为表头
                    header = [f.name for f in fields(StudentForm)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    # 使用推导式和astuple提高写入效率
                    writer.writerows(astuple(form) for form in data)
                logging.info(f"数据成功写入 {filepath}。")
                return  # 成功写入，退出函数
            except IOError as e:
                if filepath == filenames_to_try[-1]:
                    logging.error(f"写入文件 {filepath} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，数据未能保存。")
                else:
                    logging.warning(f"写入文件 {filepath} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue

    def write_skipped_csv(self, data: list[SkippedRecord]):
        """将SkippedRecord列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的跳过记录。")
            return

        full_path = OUTPUT_DIR / SKIPPED_FILENAME
        filenames_to_try = [full_path, OUTPUT_DIR / self._get_alternative_filename(SKIPPED_FILENAME)]

        for filepath in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条跳过记录写入到 {filepath}...")
                with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    header = [f.name for f in fields(SkippedRecord)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    writer.writerows(astuple(record) for record in data)
                logging.info(f"跳过记录成功写入 {filepath}。")
                return
            except IOError as e:
                if filepath == filenames_to_try[-1]:
                    logging.error(f"写入文件 {filepath} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，跳过记录未能保存。")
                else:
                    logging.warning(f"写入文件 {filepath} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue


class StudentAggregator:
    """负责将KivoWiki页面数据聚合为角色数据"""

    def __init__(self, schools_map: dict[int, dict[str, Any]]):
        self.schools_map = schools_map

    def aggregate(
        self, 
        student_cache_data: dict[int, dict[str, Any]],
        spine_cache_data: dict[int, dict[str, Any]]
    ) -> tuple[list[Student], list[Spine]]:
        """
        从缓存数据聚合生成学生列表和Spine列表
        
        Args:
            student_cache_data: 学生ID -> 学生JSON数据
            spine_cache_data: Spine ID -> Spine JSON数据
        
        Returns:
            (学生列表, Spine列表)
        """
        # 1. 使用 skin_list 进行角色聚合
        # skin_list 中的所有ID属于同一个角色
        processed_ids: set[int] = set()
        students: list[Student] = []
        spines: list[Spine] = []
        seen_spine_ids: set[int] = set()

        # 按ID排序处理，确保稳定性
        sorted_ids = sorted(student_cache_data.keys())

        for kivowiki_id in sorted_ids:
            if kivowiki_id in processed_ids:
                continue

            json_data = student_cache_data[kivowiki_id]
            if not json_data or 'data' not in json_data:
                continue

            data = json_data['data']
            
            # 跳过特定学校ID（官方账号）
            if data.get("school") == 30:
                processed_ids.add(kivowiki_id)
                continue
            
            # 跳过彩蛋
            if data.get("id") == 348:
                processed_ids.add(kivowiki_id)
                continue

            # 获取 skin_list，确定属于同一角色的所有页面
            skin_list = data.get("skin_list", [])
            if not skin_list:
                # 如果没有 skin_list，单独处理
                skin_list = [{"id": kivowiki_id}]

            # 收集该角色的所有页面ID
            page_ids = [item["id"] for item in skin_list if isinstance(item.get("id"), int)]
            
            # 角色ID取最小的ID
            student_id = min(page_ids)
            
            # 标记所有页面为已处理
            processed_ids.update(page_ids)

            # 构建页面列表
            pages: list[KivoWikiPage] = []
            for page_id in page_ids:
                page_json = student_cache_data.get(page_id, {})
                page_data = page_json.get('data', {})
                
                # 获取头像URL（从skin_list中查找）
                avatar = ""
                for item in skin_list:
                    if item.get("id") == page_id:
                        avatar = item.get("avatar", "")
                        break
                
                # 获取spine列表
                spine_ids = page_data.get("spine", [])
                if not isinstance(spine_ids, list):
                    spine_ids = []

                pages.append(KivoWikiPage(
                    page_id=page_id,
                    skin_name=page_data.get("skin", ""),
                    skin_name_cn=page_data.get("skin_cn", ""),
                    skin_name_jp=page_data.get("skin_jp", ""),
                    skin_name_tw=page_data.get("skin_zh_tw", ""),
                    avatar=avatar,
                    spines=spine_ids
                ))

                # 收集Spine数据
                for spine_id in spine_ids:
                    if spine_id in seen_spine_ids:
                        continue
                    seen_spine_ids.add(spine_id)
                    
                    spine_json = spine_cache_data.get(spine_id, {})
                    spine_data = spine_json.get('data', {})
                    if spine_data:
                        spines.append(Spine(
                            id=spine_id,
                            name=spine_data.get("name", ""),
                            remark=spine_data.get("remark", ""),
                            type=spine_data.get("type", "")
                        ))

            # 构建学生数据
            school_id = data.get("school", 0)
            
            # 构建名称
            family_name = data.get("family_name", "")
            given_name = data.get("given_name", "")
            name = f"{family_name} {given_name}".strip() if family_name else given_name

            students.append(Student(
                id=student_id,
                name=name,
                name_cn=f"{data.get('family_name_cn', '')} {data.get('given_name_cn', '')}".strip(),
                name_jp=f"{data.get('family_name_jp', '')} {data.get('given_name_jp', '')}".strip(),
                name_en=f"{data.get('family_name_en', '')} {data.get('given_name_en', '')}".strip(),
                name_kr=f"{data.get('family_name_kr', '')} {data.get('given_name_kr', '')}".strip(),
                name_tw=f"{data.get('family_name_zh_tw', '')} {data.get('given_name_zh_tw', '')}".strip(),
                school_id=school_id,
                pages=pages
            ))

        # 按ID排序
        students.sort(key=lambda s: s.id)
        spines.sort(key=lambda s: s.id)

        return students, spines


class CsvGenerator:
    """从中间JSON文件生成最终CSV"""

    # Spine跳过规则
    SPINE_KEYWORDS_TO_SKIP: list[str] = ["toschool", "minori", "ui_"]
    SPINE_SUFFIXES_TO_SKIP: list[str] = [
        "_cn", "_steam", "_glitch_spr", "_cbt", "_halofix", "spr-2", "_old", "_old_spr", "_new"
    ]

    def __init__(self):
        self.schools_map: dict[int, str] = {}  # school_id -> school_name
        self.spines_map: dict[int, dict[str, Any]] = {}  # spine_id -> spine_data

    def load_intermediate_files(self):
        """加载中间JSON文件"""
        # 加载学校
        schools_path = OUTPUT_DIR / SCHOOLS_OUTPUT_FILENAME
        if schools_path.exists():
            with open(schools_path, 'r', encoding='utf-8') as f:
                schools = json.load(f)
                self.schools_map = {s["id"]: s["name"] for s in schools}

        # 加载spines
        spines_path = OUTPUT_DIR / SPINES_OUTPUT_FILENAME
        if spines_path.exists():
            with open(spines_path, 'r', encoding='utf-8') as f:
                spines = json.load(f)
                self.spines_map = {s["id"]: s for s in spines}

    def _normalize_file_id(self, file_id: str) -> str:
        """标准化文件ID格式"""
        cleaned_id = file_id.strip()
        
        # 移除前缀
        for prefix in ['J_', 'new_', 'old_']:
            if cleaned_id.lower().startswith(prefix.lower()):
                cleaned_id = cleaned_id[len(prefix):]
        
        # 移除 _spr 及后面的所有内容
        if '_spr' in cleaned_id.lower():
            cleaned_id = cleaned_id.lower().split('_spr')[0]
        
        # 提取标准格式
        if match := re.search(r"(CH|NP)(\d{4})(_[a-z]+)?", cleaned_id, re.IGNORECASE):
            return f"{match.group(1).upper()}{match.group(2)}{match.group(3).lower() if match.group(3) else ''}"
            
        return cleaned_id.lower()

    def _process_spine_remark(self, remark: str | None, base_skin: str | None, name: str | None = None) -> str:
        """处理Spine备注信息"""
        if not remark:
            return ""

        processed = remark

        patterns = [
            r"初始立绘",
            r"立绘",
            r"差分",
            r"[\(（][^\)）]*?\d{2,4}[年\.][^\)）]*?[\)）]",
            r"\d{2,4}[年\.-]\d{1,2}[月\.-]\d{0,2}日?\s*(?:之?[前后]|更新|版本修改)?",
            r"[\(（](?:已)?更新至实装[\)）]",
            r"修正版?",
            r"更新",
            r"高清",
            r"(?i)\b(old|new|fixed|ver\.?\d*)\b",
            r"[旧新]",
            r"[\(（][\)）]",
        ]

        for pat in patterns:
            processed = re.sub(pat, "", processed)

        processed = processed.replace("()", "").replace("（）", "").strip()
        processed = processed.strip(",， ")
        processed = re.sub(r"[,，]\s*[,，]", ",", processed)
        processed = re.sub(r"[\(（]\s*([^)）]+?)\s*[\)）]", r",\1", processed)
        processed = processed.strip(",，")
        
        replacement_rules = [
            (r"礼服(?:日奈|亚子)", "礼服"),
            ("西服", "西装"),
        ]
        
        for pattern, replacement in replacement_rules:
            processed = re.sub(pattern, replacement, processed)

        if base_skin and processed == base_skin:
            return ""
        if name and processed == name:
            return ""

        return processed

    def _should_skip_spine(self, spine_data: dict[str, Any]) -> str | None:
        """检查是否应该跳过该Spine，返回跳过原因或None"""
        name = spine_data.get("name", "")
        if not name:
            return "缺少名称"

        name_lower = name.lower()

        # 只接受spr类型
        if spine_data.get("type") != "spr":
            return f"类型 ({spine_data.get('type')})"

        # 跳过包含特定关键词的
        for keyword in self.SPINE_KEYWORDS_TO_SKIP:
            if keyword in name_lower:
                return f"包含 ({keyword})"

        # 跳过特定后缀的
        for suffix in self.SPINE_SUFFIXES_TO_SKIP:
            if name_lower.endswith(suffix):
                return f"后缀 ({suffix.removeprefix('_')})"

        return None

    def generate_student_forms(self) -> tuple[list[StudentForm], list[SkippedRecord]]:
        """从中间JSON生成StudentForm列表"""
        self.load_intermediate_files()

        students_path = OUTPUT_DIR / STUDENTS_OUTPUT_FILENAME
        if not students_path.exists():
            logging.error(f"中间文件 {students_path} 不存在")
            return [], []

        with open(students_path, 'r', encoding='utf-8') as f:
            students_data = json.load(f)

        all_forms: list[StudentForm] = []
        all_skipped: list[SkippedRecord] = []

        for student in students_data:
            student_id = student["id"]
            school_id = student.get("school_id", 0)
            school_name = self.schools_map.get(school_id, "")

            for page in student.get("pages", []):
                page_id = page["page_id"]
                skin_name = page.get("skin_name", "")
                skin_name_cn = page.get("skin_name_cn", "")
                skin_name_jp = page.get("skin_name_jp", "")
                skin_name_tw = page.get("skin_name_tw", "")

                # 构建皮肤显示名称（中文优先）
                skin_display = skin_name_cn if skin_name_cn else skin_name

                # 处理该页面的所有spine
                forms_map: dict[str, StudentForm] = {}
                
                for spine_id in page.get("spines", []):
                    spine_data = self.spines_map.get(spine_id, {})
                    
                    # 检查是否跳过
                    if skip_reason := self._should_skip_spine(spine_data):
                        all_skipped.append(SkippedRecord(
                            student_id=student_id,
                            spine_id=spine_id,
                            reason=skip_reason,
                            spine_name=spine_data.get("name"),
                            spine_remark=spine_data.get("remark", ""),
                            name=student.get("name", ""),
                            name_jp=student.get("name_jp", ""),
                            name_en=student.get("name_en", ""),
                            school=school_id,
                            school_name=school_name
                        ))
                        continue

                    spine_name_raw = spine_data.get("name", "")
                    file_id = self._normalize_file_id(spine_name_raw)
                    
                    if not file_id:
                        continue

                    spine_remark = spine_data.get("remark", "")

                    # 构建各语言名称
                    base_name = student.get("name", "")
                    base_name_cn = student.get("name_cn", "")
                    base_name_jp = student.get("name_jp", "")
                    base_name_tw = student.get("name_tw", "")
                    base_name_en = student.get("name_en", "")
                    base_name_kr = student.get("name_kr", "")

                    # 处理备注（用于full_name）
                    processed_remark = self._process_spine_remark(spine_remark, skin_display, base_name)

                    # 构建完整名称（包含皮肤）
                    def build_full_name(base: str, skin: str, remark: str) -> str:
                        if not base:
                            return ""
                        parts = [s for s in [skin, remark] if s]
                        if parts:
                            return f"{base}（{','.join(parts)}）"
                        return base

                    # full_name: 使用中文皮肤名 + remark
                    full_name = build_full_name(base_name, skin_display, processed_remark)
                    # name_cn/jp/tw: 只使用对应语言的皮肤名，不加remark
                    name_cn = build_full_name(base_name_cn, skin_name_cn, "")
                    name_jp = build_full_name(base_name_jp, skin_name_jp, "")
                    name_tw = build_full_name(base_name_tw, skin_name_tw, "")

                    # 构建skin_name字段
                    skin_parts = [s for s in [skin_display, processed_remark] if s]
                    final_skin = ",".join(skin_parts)

                    form = StudentForm(
                        file_id=file_id,
                        student_id=student_id,
                        page_id=page_id,
                        spine_id=spine_id,
                        full_name=full_name,
                        name=base_name,
                        skin_name=final_skin,
                        name_cn=name_cn,
                        name_jp=name_jp,
                        name_tw=name_tw,
                        name_en=base_name_en,
                        name_kr=base_name_kr,
                        school_id=school_id,
                        school_name=school_name
                    )

                    # 去重逻辑
                    if file_id in forms_map:
                        existing = forms_map[file_id]
                        if (spine_id or 0) > (existing.spine_id or 0):
                            forms_map[file_id] = form
                    else:
                        forms_map[file_id] = form

                all_forms.extend(forms_map.values())

        # 排序
        all_forms.sort(key=lambda x: (x.student_id, x.page_id, x.file_id))
        all_skipped.sort(key=lambda x: (x.student_id, x.spine_id or -1))

        return all_forms, all_skipped
