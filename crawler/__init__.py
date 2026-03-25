"""
BA-characters-internal-id 爬虫模块

数据流程：
缓存 → 中间JSON → 最终CSV
"""

from .config import *
from .models import *
from .cache import CacheManager
from .api import APIClient
from .output import OutputWriter, StudentAggregator, CsvGenerator
from .crawler import Crawler
