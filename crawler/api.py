"""
API客户端模块
"""

import logging
from typing import Any

import httpx

from .config import (
    CHAR_API_BASE_URL,
    SPINE_API_BASE_URL,
    STUDENTS_LIST_API_URL,
    SPINES_LIST_API_URL,
    SCHOOLS_API_URL,
    STUDENTS_UPDATED_API_URL,
)
from .cache import CacheManager
from .models import remove_key


class APIClient:
    """负责处理所有网络请求及缓存管理的客户端"""

    def __init__(self, client: httpx.AsyncClient, cache_manager: CacheManager):
        self.client = client
        self.cache = cache_manager
        
        # 统计 API 请求次数
        self.student_req_count: int = 0
        self.spine_req_count: int = 0

        self.client.headers.update({
            "User-Agent": "BA-characters-internal-id (https://github.com/Agent-0808/BA-characters-internal-id)"
        })

    async def get_remote_max_ids(self) -> tuple[int, int]:
        """获取远程最新的 Student ID 和 Spine ID"""
        max_student_id = 0
        max_spine_id = 0
        
        # 1. 获取最新学生ID
        try:
            resp = await self.client.get(STUDENTS_LIST_API_URL, timeout=10.0)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == 2000 and (students := data.get("data", {}).get("students")):
                    max_student_id = students[0]["id"]
        except Exception as e:
            logging.error(f"获取最新学生ID失败: {e}")

        # 2. 获取最新Spine ID
        try:
            # 先获取最大页数
            resp_page = await self.client.get(SPINES_LIST_API_URL, params={"page": 1}, timeout=10.0)
            if resp_page.status_code == 200:
                data_page = resp_page.json()
                if max_page := data_page.get("data", {}).get("max_page"):
                    # 获取最后一页数据
                    resp_last = await self.client.get(SPINES_LIST_API_URL, params={"page": max_page}, timeout=10.0)
                    if resp_last.status_code == 200:
                        data_last = resp_last.json()
                        if spine_list := data_last.get("data", {}).get("spine"):
                            max_spine_id = spine_list[-1]["id"]
        except Exception as e:
            logging.error(f"获取最新Spine ID失败: {e}")

        return max_student_id, max_spine_id

    async def fetch_student_data(self, student_id: int, force_refresh: bool = False) -> tuple[dict | None, str | None, bool]:
        """
        根据学生ID获取数据（优先查缓存）。
        返回 (数据, 错误/跳过原因, 是否命中缓存)。
        """
        # 1. 尝试读取缓存（条件：未开启强制刷新 且 缓存存在）
        if not force_refresh:
            if cached_data := await self.cache.get_student(student_id):
                logging.debug(f"ID {student_id}: 命中缓存")
                return cached_data, None, True

        # 2. 执行 API 请求（开启了强制刷新 或 缓存未命中）
        self.student_req_count += 1
        url = CHAR_API_BASE_URL.format(student_id=student_id)
        
        try:
            response = await self.client.get(url, timeout=10.0)
            
            if response.status_code == 404:
                return None, "未找到 (404)", False
            
            response.raise_for_status()
            json_data = response.json()
            
            # 成功获取且数据有效时，保存到缓存
            if json_data and json_data.get('code') == 2000:
                await self.cache.save_student(student_id, json_data)
            
            return json_data, None, False

        except httpx.RequestError as e:
            return None, f"网络错误: {e}", False
        except Exception as e:
            logging.error(f"处理 ID {student_id} 时发生未知错误: {e}")
            return None, f"未知错误: {e}", False

    async def fetch_spine_data(self, spine_id: int) -> tuple[dict[str, Any] | None, str | None]:
        """
        根据 spine_id 获取 spine 数据（优先查缓存）。
        注意：此函数暂不需要返回是否命中缓存，因为并发获取时不由它控制主延迟。
        """
        # 1. 尝试从缓存获取
        if cached_data := await self.cache.get_spine(spine_id):
            if isinstance(cached_data, dict) and 'data' in cached_data:
                return cached_data['data'], None
            return cached_data, None 

        # 2. 缓存未命中，从 API 获取
        # 记录请求计数
        self.spine_req_count += 1

        url = SPINE_API_BASE_URL.format(spine_id=spine_id)
        try:
            response = await self.client.get(url, timeout=10.0)
            response.raise_for_status()
            json_response = response.json()
            
            if isinstance(json_response, dict) and 'data' in json_response:
                # 3. 成功获取后，保存完整响应到缓存
                await self.cache.save_spine(spine_id, json_response)
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

    async def fetch_schools_data(self, force_refresh: bool = False) -> tuple[dict[int, dict[str, Any]] | None, str | None]:
        """
        从API获取学校列表数据（优先查缓存）。
        返回 (学校字典, 错误原因)。
        """
        # 1. 如果强制刷新，跳过缓存直接从API获取
        if force_refresh:
            try:
                response = await self.client.get(SCHOOLS_API_URL, timeout=10.0)
                response.raise_for_status()
                json_data = response.json()
                
                if json_data and json_data.get('code') == 2000:
                    if 'data' in json_data and 'school' in json_data['data']:
                        for school in json_data['data']['school']:
                            for key in ['description', 'logo', 'preview_image']:
                                remove_key(school, key)
                        
                    
                    # 成功获取后，保存到缓存
                    await self.cache.save_schools(json_data)
                    
                    # 返回学校字典，以id为键
                    schools = json_data['data']['school']
                    return {school["id"]: school for school in schools}, None
                else:
                    return None, "无效的学校数据格式"
            except httpx.RequestError as e:
                return None, f"网络错误: {e}"
            except Exception as e:
                logging.error(f"获取学校数据失败: {e}")
                return None, f"未知错误: {e}"
        
        # 2. 尝试从缓存获取
        if cached_schools := await self.cache.get_schools():
            return cached_schools, None
        
        # 3. 缓存未命中，从 API 获取
        return await self.fetch_schools_data(force_refresh=True)

    async def fetch_recently_updated_ids(self) -> set[int]:
        """
        获取最近修改过的学生ID列表（按更新时间降序，取前100个）。
        返回去重后的学生ID集合。
        """
        try:
            response = await self.client.get(STUDENTS_UPDATED_API_URL, timeout=10.0)
            response.raise_for_status()
            json_data = response.json()
            
            if json_data and json_data.get('code') == 2000:
                if 'data' in json_data and 'students' in json_data['data']:
                    students = json_data['data']['students']
                    # 提取所有ID并返回集合
                    return {student['id'] for student in students if 'id' in student}
            
            logging.warning("获取最近更新的学生ID失败：响应格式无效")
            return set()
        except httpx.RequestError as e:
            logging.error(f"获取最近更新的学生ID失败（网络错误）: {e}")
            return set()
        except Exception as e:
            logging.error(f"获取最近更新的学生ID失败（未知错误）: {e}")
            return set()
