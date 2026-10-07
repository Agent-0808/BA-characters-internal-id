"""
配置模块 - 存放所有可配置的常量
"""

from pathlib import Path

# API URLs
BASE_API_URL: str = "https://api.kivo.wiki/api/v1/data"
CHAR_API_BASE_URL: str = f"{BASE_API_URL}/students/{{student_id}}"
SPINE_API_BASE_URL: str = f"{BASE_API_URL}/spines/{{spine_id}}"
STUDENTS_LIST_API_URL: str = f"{BASE_API_URL}/students/?id_sort=desc"
SPINES_LIST_API_URL: str = f"{BASE_API_URL}/spines/"
SCHOOLS_API_URL: str = f"{BASE_API_URL}/schools/?page_size=40"
STUDENTS_UPDATED_API_URL: str = f"{BASE_API_URL}/students/"

# 动态ID范围（将在main中更新）
FINAL_STUDENT_ID: int = 0
FINAL_SPINE_ID: int = 0
STUDENT_ID_RANGE: range = range(1, FINAL_STUDENT_ID + 1)

# 输出目录和文件名配置
# 如果在 crawler/ 子目录运行，输出到上级目录的 output/
_OUTPUT_DIR = Path("output")
if Path.cwd().name == "crawler":
    _OUTPUT_DIR = Path("..") / "output"
OUTPUT_DIR: Path = _OUTPUT_DIR
OUTPUT_FILENAME: str = "students_data.csv"
SKIPPED_FILENAME: str = "skipped_ids.csv"

# 中间输出文件名
SCHOOLS_OUTPUT_FILENAME: str = "schools.json"
STUDENTS_OUTPUT_FILENAME: str = "students.json"
SPINES_OUTPUT_FILENAME: str = "spines.json"

# 手动维护的翻译词表（main 分支，勿写入爬虫产物）
# 学校名多语言：键为学校 id（字符串），值按语言键扩展（如 {"en": ..., "jp": ...}），
# 合并到 schools.json 时生成 name_{lang} 字段，未覆盖 id 的字段输出空串
SCHOOL_NAMES_FILENAME: str = "school_names.json"

# 皮肤名多语言：键为 API 的 `skin` 值，值为 {"en": ..., "kr": ...}，
# 由 crawler/skin_map.py 的 translate_skin() 查询，未覆盖词条输出空串
SKIN_MAP_FILENAME: str = "skin_map.json"

# 缓存目录配置
_CACHE_DIR = Path("cache")
if Path.cwd().name == "crawler":
    _CACHE_DIR = Path("..") / "cache"
CACHE_DIR: Path = _CACHE_DIR

# 请求配置
MAX_CONCURRENT_REQUESTS: int = 3  # 最大并发请求数
REQUEST_DELAY_SECONDS: float = 2  # 两次请求之间的间隔（秒）
PAGE_SIZE: int = 1  # API请求页大小，用于获取最新数据

# 增量探测窗口（最近更新列表的 page_size）
# 有新增页面ID时可能积压较多修改，使用更大窗口追平；日常运行使用较小窗口，
# 日常实际请求数由水位线边界控制（约等于更新页面数 + 1 次边界探测）
RECENT_UPDATED_WINDOW_WITH_NEW_IDS: int = 200
RECENT_UPDATED_WINDOW: int = 50

# 运行模式配置
TEST_MODE: bool = False  # 测试模式：True 表示只检测更新，不执行完整爬取
TEST_OVERWRITE_CACHE: bool = True  # 测试模式下是否用新数据覆盖本地缓存
