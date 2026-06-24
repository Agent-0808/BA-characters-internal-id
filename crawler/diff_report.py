"""
变更报告生成模块 - 对比新旧JSON文件生成Markdown格式的变更报告
"""

import json
from pathlib import Path
from typing import Any

from .config import (
    OUTPUT_DIR,
    STUDENTS_OUTPUT_FILENAME,
    SPINES_OUTPUT_FILENAME,
)


# 旧数据目录（由workflow备份）
PREV_OUTPUT_DIR = Path("prev_output")

# 报告输出文件名
DIFF_REPORT_FILENAME = "diff_report.md"


def load_json(filepath: Path) -> list[dict[str, Any]]:
    """加载JSON文件"""
    if not filepath.exists():
        return []
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def diff_students(prev_students: list, curr_students: list) -> tuple[list[str], int, int, int]:
    """
    对比students.json变更
    返回: (变更列表, 新增数量, 删除数量, 改动数量)
    """
    lines: list[str] = []
    added_count = 0
    removed_count = 0
    changed_count = 0

    # 构建ID映射
    prev_map = {s["id"]: s for s in prev_students}
    curr_map = {s["id"]: s for s in curr_students}

    prev_ids = set(prev_map.keys())
    curr_ids = set(curr_map.keys())

    # 新增学生
    added_ids = curr_ids - prev_ids
    added_count = len(added_ids)
    for sid in sorted(added_ids):
        s = curr_map[sid]
        lines.append(f"+ {s['name']} (student_id: {sid})")

    # 学生信息变化（放在中间）
    common_ids = prev_ids & curr_ids
    for sid in sorted(common_ids):
        prev_s = prev_map[sid]
        curr_s = curr_map[sid]

        # 检查学生级别字段变化（排除pages）
        student_fields = ["name", "name_cn", "name_jp", "name_en", "name_kr", "name_tw", "school_id"]
        for field in student_fields:
            if prev_s.get(field) != curr_s.get(field):
                changed_count += 1
                lines.append(f"* {curr_s['name']}: {field} [{prev_s.get(field, '')}] → [{curr_s.get(field, '')}]")

        # 页面级别变化
        pages_diff, pages_added, pages_removed, pages_changed = diff_pages(prev_s, curr_s)
        lines.extend(pages_diff)
        changed_count += pages_added + pages_removed + pages_changed

    # 删除学生（放在最后）
    removed_ids = prev_ids - curr_ids
    removed_count = len(removed_ids)
    for sid in sorted(removed_ids):
        s = prev_map[sid]
        lines.append(f"- {s['name']} (student_id: {sid})")

    return lines, added_count, removed_count, changed_count


def diff_pages(prev_student: dict, curr_student: dict) -> tuple[list[str], int, int, int]:
    """
    对比单个学生的pages变更
    返回: (变更列表, 新增数量, 删除数量, 改动数量)
    """
    lines: list[str] = []
    added_count = 0
    removed_count = 0
    changed_count = 0
    student_name = curr_student["name"]

    prev_pages = {p["page_id"]: p for p in prev_student.get("pages", [])}
    curr_pages = {p["page_id"]: p for p in curr_student.get("pages", [])}

    prev_page_ids = set(prev_pages.keys())
    curr_page_ids = set(curr_pages.keys())

    # 新增页面
    added_page_ids = curr_page_ids - prev_page_ids
    added_count = len(added_page_ids)
    for page_id in sorted(added_page_ids):
        p = curr_pages[page_id]
        skin_name = p.get("skin_name", "") or "默认"
        lines.append(f"  + {student_name} - {skin_name} (page_id: {page_id})")

    # 页面信息变化（放在中间）
    common_page_ids = prev_page_ids & curr_page_ids
    for page_id in sorted(common_page_ids):
        prev_p = prev_pages[page_id]
        curr_p = curr_pages[page_id]

        # spines列表变化
        prev_spines = prev_p.get("spines", [])
        curr_spines = curr_p.get("spines", [])
        if prev_spines != curr_spines:
            changed_count += 1
            skin_name = curr_p.get("skin_name", "") or "默认"
            # 显示新增/删除的spine id
            added_spines = set(curr_spines) - set(prev_spines)
            removed_spines = set(prev_spines) - set(curr_spines)
            change_desc = []
            if added_spines:
                change_desc.append(f"+spines:{sorted(added_spines)}")
            if removed_spines:
                change_desc.append(f"-spines:{sorted(removed_spines)}")
            lines.append(f"  * {student_name} - {skin_name} (page_id: {page_id}): {', '.join(change_desc)}")

    # 删除页面（放在最后）
    removed_page_ids = prev_page_ids - curr_page_ids
    removed_count = len(removed_page_ids)
    for page_id in sorted(removed_page_ids):
        p = prev_pages[page_id]
        skin_name = p.get("skin_name", "") or "默认"
        lines.append(f"  - {student_name} - {skin_name} (page_id: {page_id})")

    return lines, added_count, removed_count, changed_count


def diff_spines(prev_spines: list, curr_spines: list) -> tuple[list[str], list[str], int, int]:
    """
    对比spines.json变更
    返回: (新增列表, 删除列表, 新增数量, 删除数量)
    """
    added_lines: list[str] = []
    removed_lines: list[str] = []

    prev_map = {s["id"]: s for s in prev_spines}
    curr_map = {s["id"]: s for s in curr_spines}

    prev_ids = set(prev_map.keys())
    curr_ids = set(curr_map.keys())

    # 新增spine
    added_ids = curr_ids - prev_ids
    for sid in sorted(added_ids):
        s = curr_map[sid]
        remark = s.get("remark", "")
        remark_str = f" ({remark})" if remark else ""
        added_lines.append(f"+ {sid}: {s['name']}{remark_str}")

    # 删除spine（放在最后）
    removed_ids = prev_ids - curr_ids
    for sid in sorted(removed_ids):
        s = prev_map[sid]
        remark = s.get("remark", "")
        remark_str = f" ({remark})" if remark else ""
        removed_lines.append(f"- {sid}: {s['name']}{remark_str}")

    return added_lines, removed_lines, len(added_ids), len(removed_ids)


def generate_diff_report(prev_dir: Path = PREV_OUTPUT_DIR, curr_dir: Path = OUTPUT_DIR) -> str:
    """
    生成完整的变更报告
    返回Markdown格式字符串
    """
    # 加载新旧数据
    prev_students = load_json(prev_dir / STUDENTS_OUTPUT_FILENAME)
    curr_students = load_json(curr_dir / STUDENTS_OUTPUT_FILENAME)
    prev_spines = load_json(prev_dir / SPINES_OUTPUT_FILENAME)
    curr_spines = load_json(curr_dir / SPINES_OUTPUT_FILENAME)

    # 生成变更内容
    student_changes, stu_added, stu_removed, stu_changed = diff_students(prev_students, curr_students)
    spine_added_lines, spine_removed_lines, spine_added, spine_removed = diff_spines(prev_spines, curr_spines)

    # 构建报告
    report_lines: list[str] = []

    report_lines.append("## 数据变更报告")
    report_lines.append("")

    # students变更
    report_lines.append("### 学生数据变更")
    report_lines.append("")
    if student_changes:
        report_lines.extend(student_changes)
    else:
        report_lines.append("无变更")
    report_lines.append("")

    # spines变更
    report_lines.append("### Spine数据变更")
    report_lines.append("")
    if spine_added_lines or spine_removed_lines:
        # 新增在前
        if spine_added_lines:
            report_lines.extend(spine_added_lines)
        # 删除在后
        if spine_removed_lines:
            report_lines.extend(spine_removed_lines)
    else:
        report_lines.append("无变更")
    report_lines.append("")

    # 统计信息
    report_lines.append("### 统计")
    report_lines.append("")
    # 格式: prev→curr (+added -removed *changed)
    def format_stat(prev: int, curr: int, added: int, removed: int, changed: int) -> str:
        if added == 0 and removed == 0 and changed == 0:
            return f"{prev}→{curr}"
        parts = []
        if added > 0:
            parts.append(f"+{added}")
        if removed > 0:
            parts.append(f"-{removed}")
        if changed > 0:
            parts.append(f"*{changed}")
        return f"{prev}→{curr} ({' '.join(parts)})"

    report_lines.append(f"- 学生总数: {format_stat(len(prev_students), len(curr_students), stu_added, stu_removed, stu_changed)}")
    report_lines.append(f"- Spine总数: {format_stat(len(prev_spines), len(curr_spines), spine_added, spine_removed, 0)}")

    return "\n".join(report_lines)


def write_report(report: str, output_path: Path = OUTPUT_DIR / DIFF_REPORT_FILENAME):
    """写入报告文件"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)


def main():
    """命令行入口"""
    report = generate_diff_report()
    write_report(report)
    print(f"变更报告已生成: {OUTPUT_DIR / DIFF_REPORT_FILENAME}")
    print("---")
    print(report)


if __name__ == "__main__":
    main()