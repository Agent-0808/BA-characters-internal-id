# main.py
import asyncio
import csv
import logging
import re
from dataclasses import dataclass, fields, astuple
from typing import Final
import httpx

# --- 1. 配置模块 ---

# 可配置的常量
API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/students/{student_id}"
SPINE_API_BASE_URL: Final[str] = "https://api.kivo.wiki/api/v1/data/spines/{spine_id}"
STUDENT_ID_RANGE: Final[range] = range(1, 20)
OUTPUT_FILENAME: Final[str] = "students_data.csv"
SKIPPED_FILENAME: Final[str] = "skipped_ids.csv"
MAX_CONCURRENT_REQUESTS: Final[int] = 3
REQUEST_DELAY_SECONDS: Final[float] = 2

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# 设置 httpx 日志级别为 WARNING，以屏蔽 INFO 级别的成功请求日志
logging.getLogger("httpx").setLevel(logging.WARNING)

# 正则表达式预编译
# 统一的 file_id 模式，匹配如 CH0201, np0001 等
FILE_ID_PATTERN: Final[re.Pattern[str]] = re.compile(r"(?:CH|ch|NP|np)\d{4}")


# --- 2. 数据结构定义 ---

@dataclass
class StudentForm:
    """用于存储单个角色形态结构化数据的类"""
    file_id: str
    kivo_wiki_id: int
    spine_id: int | None
    name: str
    skin_name: str
    name_cn: str
    name_jp: str
    name_tw: str
    name_en: str
    name_kr: str


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


class APIClient:
    """负责处理所有网络请求的客户端"""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client

        self.client.headers.update({
            "User-Agent": "BA-characters-internal-id (https://github.com/Agent-0808/BA-characters-internal-id)"
        })

    async def fetch_student_data(self, student_id: int) -> tuple[dict | None, str | None]:
        """
        根据学生ID获取数据。
        返回 (数据, None) 或 (None, 错误/跳过原因)。
        """
        url = API_BASE_URL.format(student_id=student_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            if response.status_code == 404:
                return None, "未找到 (404)"
            response.raise_for_status()
            return response.json(), None
        except httpx.RequestError as e:
            return None, f"网络错误: {e}"
        except Exception as e:
            logging.error(f"处理 ID {student_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}"

    async def fetch_spine_data(self, spine_id: int) -> tuple[dict[str, any] | None, str | None]:
        """根据 spine_id 获取 spine 数据。返回 (数据, None) 或 (None, 错误原因)"""
        url = SPINE_API_BASE_URL.format(spine_id=spine_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            # 确保返回的数据是有效的字典且包含 'data' 键
            if isinstance(json_response, dict) and 'data' in json_response:
                return json_response['data'], None
            logging.warning(f"Spine ID {spine_id} 的响应格式无效: {json_response}")
            return None, "响应格式无效"
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None, "未找到 (404)"
            return None, f"HTTP错误: {e.response.status_code}"
        except httpx.RequestError as e:
            logging.warning(f"请求 Spine ID {spine_id} 时网络错误: {e}")
            return None, f"网络错误: {e}"
        except Exception as e:
            logging.error(f"处理 Spine ID {spine_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}"

# --- 4. 数据解析模块 ---

class DataParser:
    """负责解析JSON数据并根据规则提取信息"""

    def _get_spine_skip_reason(self, spine_item: dict[str, any]) -> str | None:
        """
        检查单个 spine 数据，如果应跳过则返回原因，否则返回 None。
        """
        if not spine_item or not spine_item.get("name"):
            return "缺少名称或数据无效"

        # 只接受spr类型
        if type := spine_item.get("type"):
            if type != "spr":
                return f"类型 ({type})"

        # 跳过特定后缀的形态
        SPINE_SUFFIXES_TO_SKIP: Final[list[str]] = ["_cn", "_steam", "_glitch_spr", "_cbt"]

        for suffix in SPINE_SUFFIXES_TO_SKIP:
            if spine_item.get("name").lower().endswith(suffix.lower()):
                return f"后缀 ({suffix})"

        return None

    def _validate_and_get_skip_reason(self, char_data: dict | None) -> str | None:
        """
        对JSON数据进行预检查，如果应跳过则返回原因，否则返回None。
        """
        if not char_data or 'data' not in char_data:
            return "数据无效或缺少 'data' 键"

        data = char_data['data']
        if not data:
            return "键 'data' 的值为空"

        # 规则1: 跳过特定学校ID（例如官方账号）
        if data.get("school") == 30:
            return "官方账号"

        return None

    def _build_name(self, family: str | None, given: str | None) -> str:
        """根据姓和名构建全名"""
        family_name = family or ""
        given_name = given or ""
        if family_name:
            return f"{family_name} {given_name}".strip()
        return given_name

    def _normalize_file_id(self, file_id: str) -> str:
        """
        标准化文件ID格式：
        - 移除 'J_' 前缀
        - 移除 '_spr' 后缀
        - CH/NP 类统一使用大写
        - 其他类统一使用小写
        """
        # 移除 'J_' 前缀
        if file_id.startswith('J_'):
            file_id = file_id.removeprefix('J_')

        # 移除 '_spr' 后缀
        if file_id.endswith('_spr'):
            file_id = file_id[:-4]  # 移除 "_spr"

        # 检查是否以CH或NP开头，并且后面跟着4个数字
        if re.match(r"^(CH|NP)\d{4}$", file_id, re.IGNORECASE):
            return file_id.upper()
        return file_id.lower()

    def _is_valid_file_id(self, file_id: str) -> bool:
        """
        检查file_id是否有效
        - 长度大于2
        - 不是纯数字
        """
        return len(file_id) > 2 and not file_id.isdigit()

    def _extract_file_id_from_url(self, url: str | None) -> str | None:
        """
        从 URL 或字符串中提取标准的 file_id (CH/NPXXXX)。
        """
        if url and (match := FILE_ID_PATTERN.search(url)):
            return self._normalize_file_id(match.group(0))
        return None

    def _find_file_id_from_avatar(self, data: dict) -> str | None:
        """
        从 avatar 字段提取 file_id。
        """
        if file_id := self._extract_file_id_from_url(data.get("avatar")):
            return file_id

        return None

    def _find_file_id_from_given_name_jp(self, given_name_jp: str | None) -> str | None:
        """
        从 given_name_jp 字段中提取 file_id。
        """
        if not given_name_jp:
            return None

        # 直接使用_normalize_file_id处理，它会自动移除_spr后缀
        normalized_id = self._normalize_file_id(given_name_jp)

        return normalized_id

    def _find_file_id_from_voice(self, voices: list[dict]) -> str | None:
        """
        最高优先级：从语音数据的 description 字段提取 file_id。
        """
        for voice in voices:
            description = voice.get("description", "")
            if match := FILE_ID_PATTERN.search(description):
                # 使用 group(0) 获取完整匹配，然后标准化格式
                return self._normalize_file_id(match.group(0))
        return None

    def _parse_skin_name_from_title(self, title: str) -> str | None:
        """
        从 gallery 的 title 中解析出皮肤名。
        如果 title 代表一个特殊形态，则返回处理后的 skin_name (可能为空字符串)；
        否则返回 None。
        """
        # 定义标识特殊形态的核心关键字
        IDENTIFY_KEYWORDS: Final[tuple[str, ...]] = ("立绘", "差分")
        # 定义需要从标题中移除的关键字，按长度降序排列以避免错误替换
        CLEANUP_KEYWORDS: Final[tuple[str, ...]] = ("初始", "差分", "立绘", "脸部", "表情", "脸图", "-")

        # 检查标题是否包含任何一个核心关键字
        if not any(key in title for key in IDENTIFY_KEYWORDS):
            return None

        # 移除所有关键字以提取皮肤名
        skin_name = title
        for key in CLEANUP_KEYWORDS:
            skin_name = skin_name.replace(key, "")

        return skin_name.strip()

    def _find_special_forms_from_gallery(self, gallery: list[dict], base_skin_name: str = "") -> dict[str, str]:
        """
        次高优先级：从图库中提取特殊形态的 file_id 及其形态名称。
        主要针对 "领航服差分" 等未在 character_datas 中定义的形态。
        """
        special_forms = {}
        for gallery_item in gallery:
            title = gallery_item.get("title", "")
            # 使用辅助函数来判断和提取 skin_name
            if (gallery_skin_name := self._parse_skin_name_from_title(title)) is not None:
                for image_url in gallery_item.get("images", []):
                    file_id_found: str | None = None
                    # 优先匹配标准 file_id 格式 (如 CH0123, NP0456)
                    if file_id := self._extract_file_id_from_url(image_url):
                        file_id_found = file_id
                    # 若无标准ID，则尝试从文件名提取非标准ID (如 shiroko_robber)
                    else:
                        filename = image_url.split('/')[-1]
                        # 假定ID是文件名中 "_spr_" 之前的部分
                        if '_spr_' in filename:
                            potential_id = filename.split('_spr_', 1)[0]
                            # 验证提取的ID是否有效
                            if self._is_valid_file_id(potential_id):
                                file_id_found = self._normalize_file_id(potential_id)

                    if file_id_found:
                        # 结合基础皮肤名和图库皮肤名
                        if base_skin_name and gallery_skin_name:
                            combined_skin_name = f"{base_skin_name},{gallery_skin_name}"
                        elif base_skin_name:
                            combined_skin_name = base_skin_name
                        else:
                            combined_skin_name = gallery_skin_name

                        # 使用找到的第一个有效ID作为此形态的ID，然后处理下一个gallery item
                        special_forms[file_id_found] = combined_skin_name
                        break
        return special_forms

    def parse(self, json_data: dict, kivo_wiki_id: int, spine_data: list[dict[str, any]]) -> tuple[
        list[StudentForm], list[SkippedRecord], str | None]:
        """
        解析单个JSON响应。
        返回 (StudentForm列表, SkippedRecord列表, 学生级别的跳过原因 | None)。
        """
        if skip_reason := self._validate_and_get_skip_reason(json_data):
            return [], [], skip_reason

        data = json_data['data']
        results: list[StudentForm] = []
        skipped_spines: list[SkippedRecord] = []
        processed_file_ids: set[str] = set()

        # 提取基础信息
        name = self._build_name(data.get("family_name"), data.get("given_name"))
        base_name_cn = self._build_name(data.get("family_name_cn"), data.get("given_name_cn"))
        base_name_jp = self._build_name(data.get("family_name_jp"), data.get("given_name_jp"))
        base_name_tw = self._build_name(data.get("family_name_zh_tw"), data.get("given_name_zh_tw"))
        base_name_en = self._build_name(data.get("family_name_en"), data.get("given_name_en"))
        base_name_kr = self._build_name(data.get("family_name_kr"), data.get("given_name_kr"))

        # 从学生主数据中提取各语言的基础皮肤名
        base_skin_cn = data.get("skin") or data.get("skin_cn") or ""
        base_skin_jp = data.get("skin_jp") or ""
        base_skin_tw = data.get("skin_zh_tw") or ""

        # 1. 最高优先级：从 spine 数据提取
        for spine_item in spine_data:
            if skip_reason := self._get_spine_skip_reason(spine_item):
                # 从 spine_item 中获取 spine_id, name 和 remark
                spine_id = spine_item.get("id")
                spine_name = spine_item.get("name")
                spine_remark = spine_item.get("remark", "")
                
                skipped_spines.append(SkippedRecord(
                    student_id=kivo_wiki_id,
                    spine_id=spine_id,
                    reason=skip_reason,
                    spine_name=spine_name,
                    spine_remark=spine_remark,
                    name=name, 
                    name_jp=base_name_jp, 
                    name_en=base_name_en, 
                    school=data.get("school", "")
                ))
                continue

            # 经过滤，我们确信 spine_name_raw 存在
            spine_name_raw = spine_item["name"]
            file_id = self._normalize_file_id(spine_name_raw)
            if not file_id or file_id in processed_file_ids:
                continue

            # 从 spine_item 中获取 spine_id 和 remark
            spine_id = spine_item.get("id")
            spine_remark = spine_item.get("remark", "")

            # 根据 remark 和基础皮肤名确定最终皮肤名
            final_skin_cn = base_skin_cn
            final_skin_jp = base_skin_jp
            final_skin_tw = base_skin_tw

            # 如果 remark 不是 "初始立绘"，则将其作为附加皮肤名
            if spine_remark and spine_remark != "初始立绘":
                # 如果已有基础皮肤名，则用逗号连接
                final_skin_cn = f"{base_skin_cn},{spine_remark}" if base_skin_cn else spine_remark
                # 对于日文和繁中，如果主数据中没有，则也使用 remark
                final_skin_jp = f"{base_skin_jp},{spine_remark}" if base_skin_jp else spine_remark
                final_skin_tw = f"{base_skin_tw},{spine_remark}" if base_skin_tw else spine_remark

            # 根据最终皮肤名构建多语言名称
            name_cn = f"{base_name_cn} （{final_skin_cn}）" if base_name_cn and final_skin_cn else base_name_cn
            name_jp = f"{base_name_jp} （{final_skin_jp}）" if base_name_jp and final_skin_jp else base_name_jp
            name_tw = f"{base_name_tw} （{final_skin_tw}）" if base_name_tw and final_skin_tw else base_name_tw

            results.append(StudentForm(
                file_id=file_id,
                kivo_wiki_id=kivo_wiki_id,
                spine_id=spine_id,
                name=name,
                skin_name=final_skin_cn,
                name_cn=name_cn,
                name_jp=name_jp,
                name_tw=name_tw,
                name_en=base_name_en,
                name_kr=base_name_kr
            ))
            processed_file_ids.add(file_id)


        # --- 2. 后备逻辑 ---
        # 为后备方案统一构建名称
        fallback_name_cn = f"{base_name_cn} （{base_skin_cn}）" if base_name_cn and base_skin_cn else base_name_cn
        fallback_name_jp = f"{base_name_jp} （{base_skin_jp}）" if base_name_jp and base_skin_jp else base_name_jp
        fallback_name_tw = f"{base_name_tw} （{base_skin_tw}）" if base_name_tw and base_skin_tw else base_name_tw

        # 2a. 预先提取所有可能的 file_id 来源
        file_id_from_voice = self._find_file_id_from_voice(data.get("voice", []))
        file_id_from_avatar = self._find_file_id_from_avatar(data)
        file_id_from_given_name_jp = None

        if given_name_jp := data.get("given_name_jp"):
            # 仅当 given_name_jp 看起来像一个文件名时才处理
            if given_name_jp.endswith("_spr"):
                file_id_from_given_name_jp = self._find_file_id_from_given_name_jp(given_name_jp)

        # 2b. 处理 character_datas 中的常规形态
        for char_data in data.get("character_datas", []):
            file_id: str | None = None
            dev_name = char_data.get("dev_name")

            # 优先级 1: 语音
            if file_id_from_voice:
                file_id = file_id_from_voice
            # 优先级 2: Avatar
            elif file_id_from_avatar:
                file_id = file_id_from_avatar
            # 优先级 3: given_name_jp (处理_spr结尾的情况)
            elif file_id_from_given_name_jp:
                file_id = file_id_from_given_name_jp
                logging.debug(f"ID {kivo_wiki_id}: 从given_name_jp提取file_id: '{data.get('given_name_jp')}' -> '{file_id}'")
            # 优先级 4: dev_name 作为后备
            elif dev_name:
                file_id = self._normalize_file_id(dev_name.removesuffix("_default"))
                logging.debug(f"ID {kivo_wiki_id}: 未能从语音、avatar或given_name_jp中找到 file_id, "
                              f"回退'{dev_name}' -> '{file_id}'")

            if not file_id or file_id in processed_file_ids:
                continue

            results.append(StudentForm(
                file_id=file_id, kivo_wiki_id=kivo_wiki_id, spine_id=None, name=name,
                skin_name=base_skin_cn, name_cn=fallback_name_cn, name_jp=fallback_name_jp,
                name_tw=fallback_name_tw, name_en=base_name_en, name_kr=base_name_kr
            ))
            processed_file_ids.add(file_id)

        # 2c. 后备方案：如果 character_datas 为空或未解析出结果
        if not processed_file_ids:
            fallback_file_id = file_id_from_voice or file_id_from_avatar or file_id_from_given_name_jp
            if fallback_file_id and fallback_file_id not in processed_file_ids:
                logging.debug(f"ID {kivo_wiki_id}: 使用后备 file_id '{fallback_file_id}'")
                results.append(StudentForm(
                    file_id=fallback_file_id, kivo_wiki_id=kivo_wiki_id, spine_id=None, name=name,
                    skin_name=base_skin_cn, name_cn=fallback_name_cn, name_jp=fallback_name_jp,
                    name_tw=fallback_name_tw, name_en=base_name_en, name_kr=base_name_kr
                ))
                processed_file_ids.add(fallback_file_id)

        # 2d. 处理 gallery 中的特殊形态
        special_forms = self._find_special_forms_from_gallery(data.get("gallery", []), base_skin_cn)
        for file_id, skin_name in special_forms.items():
            if file_id not in processed_file_ids:
                # gallery 的 skin_name 是新解析的，需重新构建名称
                name_cn = f"{base_name_cn} （{skin_name}）" if base_name_cn and skin_name else base_name_cn
                name_jp = f"{base_name_jp} （{skin_name}）" if base_name_jp and skin_name else base_name_jp
                name_tw = f"{base_name_tw} （{skin_name}）" if base_name_tw and skin_name else base_name_tw
                results.append(StudentForm(
                    file_id=file_id,
                    kivo_wiki_id=kivo_wiki_id,
                    spine_id=None,
                    name=name,
                    skin_name=skin_name,
                    name_cn=name_cn,
                    name_jp=name_jp,
                    name_tw=name_tw,
                    name_en=base_name_en,
                    name_kr=base_name_kr
                ))
                processed_file_ids.add(file_id)

        # 在函数末尾增加检查：如果最终没有解析到任何数据，则返回具体原因
        if not results and not skipped_spines:
            return [], [], "未找到可解析的角色形态"

        return results, skipped_spines, None

# --- 5. 文件输出模块 ---

class CsvWriter:
    """负责将处理好的数据写入CSV文件"""

    def __init__(self, filename: str):
        self.filename = filename

    def _get_alternative_filename(self, original_filename: str) -> str:
        """生成备用文件名"""
        base, ext = original_filename.rsplit('.', 1)
        return f"{base}_backup.{ext}"

    def write(self, data: list[StudentForm]):
        """将StudentForm列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的数据。")
            return

        filenames_to_try = [self.filename, self._get_alternative_filename(self.filename)]

        for filename in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条记录写入到 {filename}...")
                with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    # 获取dataclass的字段名作为表头
                    header = [f.name for f in fields(StudentForm)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    # 使用推导式和astuple提高写入效率
                    writer.writerows(astuple(form) for form in data)
                logging.info(f"数据成功写入 {filename}。")
                return  # 成功写入，退出函数
            except IOError as e:
                if filename == filenames_to_try[-1]:
                    # 已经是最后一个文件名，仍然失败
                    logging.error(f"写入文件 {filename} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，数据未能保存。")
                else:
                    # 还有备用文件名可以尝试
                    logging.warning(f"写入文件 {filename} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue

    def write_skipped(self, data: list[SkippedRecord]):
        """将SkippedRecord列表写入CSV文件"""
        if not data:
            logging.warning("没有可供写入的跳过记录。")
            return

        filenames_to_try = [self.filename, self._get_alternative_filename(self.filename)]

        for filename in filenames_to_try:
            try:
                logging.info(f"开始将 {len(data)} 条跳过记录写入到 {filename}...")
                with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    # 获取dataclass的字段名作为表头
                    header = [f.name for f in fields(SkippedRecord)]
                    writer = csv.writer(csvfile)
                    writer.writerow(header)
                    # 使用推导式和astuple提高写入效率
                    writer.writerows(astuple(record) for record in data)
                logging.info(f"跳过记录成功写入 {filename}。")
                return  # 成功写入，退出函数
            except IOError as e:
                if filename == filenames_to_try[-1]:
                    # 已经是最后一个文件名，仍然失败
                    logging.error(f"写入文件 {filename} 时发生错误: {e}")
                    logging.error("所有尝试的文件名均失败，跳过记录未能保存。")
                else:
                    # 还有备用文件名可以尝试
                    logging.warning(f"写入文件 {filename} 失败，可能是文件被占用，尝试使用备用文件名...")
                    continue


# --- 6. 主逻辑与执行 ---

async def process_student_id(
    student_id: int,
    client: APIClient,
    parser: DataParser,
    semaphore: asyncio.Semaphore
) -> tuple[int, list[StudentForm], list[SkippedRecord]]:
    """
    获取、解析并处理单个学生ID的数据。
    返回学生ID、处理结果的列表和一个SkippedRecord列表。
    """
    async with semaphore:
        all_skipped: list[SkippedRecord] = []
        json_data, fetch_reason = await client.fetch_student_data(student_id)
        # 即使请求学生数据失败，也需要延迟，避免对API造成过大压力
        await asyncio.sleep(REQUEST_DELAY_SECONDS)

        if not json_data:
            # 在无法获取JSON数据时，创建一个包含基本信息的SkippedRecord
            skipped = SkippedRecord(
                student_id=student_id,
                spine_id=None,
                reason=fetch_reason or "未知网络原因",
                spine_name=None, 
                spine_remark=None,
                name="", 
                name_jp="", 
                name_en="", 
                school=""
            )
            return student_id, [], [skipped]

        # 获取 spine 数据
        spine_ids = json_data.get("data", {}).get("spine", [])
        spine_tasks = [client.fetch_spine_data(sid) for sid in spine_ids if isinstance(sid, int)]
        spine_results_raw = await asyncio.gather(*spine_tasks)
        # 只提取成功获取的数据部分，忽略错误信息
        spine_results = [data for data, error in spine_results_raw if data is not None]

        forms, skipped_spines, student_skip_reason = parser.parse(json_data, student_id, spine_results)
        all_skipped.extend(skipped_spines)

        if student_skip_reason:
            # 如果整个学生因规则被跳过，则从JSON数据中提取详细信息
            data = json_data.get("data", {})
            name = parser._build_name(data.get("family_name"), data.get("given_name")) or data.get("given_name_cn", "")
            name_jp = parser._build_name(data.get("family_name_jp"), data.get("given_name_jp")) or ""
            name_en = parser._build_name(data.get("family_name_en"), data.get("given_name_en")) or ""
            school = data.get("school", "")

            skipped = SkippedRecord(
                student_id=student_id,
                spine_id=None,
                reason=student_skip_reason,
                spine_name=None, 
                spine_remark=None,
                name=name,
                name_jp=name_jp,
                name_en=name_en,
                school=school
            )
            all_skipped.append(skipped)

        return student_id, forms, all_skipped

async def main():
    """主执行函数"""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    parser = DataParser()
    all_student_forms: list[StudentForm] = []
    skipped_records: list[SkippedRecord] = []

    async with httpx.AsyncClient() as http_client:
        client = APIClient(http_client)
        student_ids = list(STUDENT_ID_RANGE)
        total_count = len(student_ids)

        tasks = [
            process_student_id(student_id, client, parser, semaphore)
            for student_id in student_ids
        ]

        logging.info(f"开始处理 {total_count} 个学生 ID...")

        processed_count = 0
        for future in asyncio.as_completed(tasks):
            processed_count += 1
            student_id, forms_list, newly_skipped_records = await future

            progress_prefix = f"[{processed_count}/{total_count}]"

            if forms_list:
                # 成功提取到数据
                file_ids_str = ", ".join(form.file_id for form in forms_list)
                print(f"{progress_prefix} ID: {student_id} -> 成功, File IDs: {file_ids_str}")
                all_student_forms.extend(forms_list)

            if newly_skipped_records:
                # 记录并打印跳过信息
                for skipped in newly_skipped_records:
                    if skipped.spine_id:
                        print(f"{progress_prefix} ID: {student_id} -> Spine ID {skipped.spine_id} 已跳过 ({skipped.reason})")
                    else:
                        print(f"{progress_prefix} ID: {student_id} -> 已跳过 ({skipped.reason})")
                skipped_records.extend(newly_skipped_records)


    # 按 file_id 排序以保证输出顺序稳定
    all_student_forms.sort(key=lambda x: (x.kivo_wiki_id, x.file_id))

    # 按 student_id 和 spine_id 排序以保证输出顺序稳定
    skipped_records.sort(key=lambda x: (x.student_id, x.spine_id or -1))

    # 写入文件
    writer = CsvWriter(OUTPUT_FILENAME)
    writer.write(all_student_forms)

    # 写入跳过记录文件
    skipped_writer = CsvWriter(SKIPPED_FILENAME)
    skipped_writer.write_skipped(skipped_records)


if __name__ == "__main__":
    asyncio.run(main())