"""
爬虫工作流模块

职责：仅负责更新缓存，不做数据解析
"""

import asyncio
import logging
from typing import Any

from .cache import CacheManager
from .api import APIClient


async def process_student_id(
    student_id: int,
    client: APIClient,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_refresh: bool = False,
    schools_map: dict[int, dict[str, Any]] | None = None
) -> tuple[int, str, bool]:
    """
    获取单个学生ID的数据并更新缓存。
    
    Returns:
        (student_id, status, from_cache) - 状态为 "success" 或错误信息
    """
    schools_map = schools_map or {}
    async with semaphore:
        # 获取数据
        json_data, fetch_reason, from_cache = await client.fetch_student_data(student_id)
        
        # 如果强制刷新且数据来自缓存，则重新获取
        if force_refresh and from_cache:
            logging.debug(f"ID {student_id}: 强制刷新，清除缓存并重新获取")
            json_data, fetch_reason, from_cache = await client.fetch_student_data(student_id, force_refresh=True)
        
        # 如果数据不是来自缓存，执行延迟
        if not from_cache:
            await asyncio.sleep(delay)

        if not json_data:
            return student_id, fetch_reason or "未知网络原因", from_cache

        # 获取学校名称（用于日志）
        school_name = ""
        if 'data' in json_data and 'school' in json_data['data']:
            school_id = json_data['data']['school']
            if isinstance(school_id, int) and school_id in schools_map:
                school_name = schools_map[school_id].get('name', '')

        # 获取 spine 数据（触发缓存更新）
        spine_ids = json_data.get("data", {}).get("spine", [])
        spine_tasks = [client.fetch_spine_data(sid) for sid in spine_ids if isinstance(sid, int)]
        await asyncio.gather(*spine_tasks)

        # 返回成功状态
        name_parts = []
        if data := json_data.get('data'):
            if fn := data.get('family_name'):
                name_parts.append(fn)
            if gn := data.get('given_name'):
                name_parts.append(gn)
        name = ' '.join(name_parts) if name_parts else f"ID {student_id}"
        
        return student_id, f"success: {name} ({school_name})", from_cache


class Crawler:
    """核心爬虫工作流 - 仅负责更新缓存"""
    
    def __init__(self, client: APIClient, cache_manager: CacheManager, max_concurrent: int, delay: float):
        self.client = client
        self.cache_manager = cache_manager
        self.max_concurrent = max_concurrent
        self.delay = delay
    
    async def run(self, student_ids: list[int], force_refresh_ids: set[int] | None = None) -> tuple[int, int]:
        """
        执行爬取或缓存读取流程，仅更新缓存。
        
        Args:
            student_ids: 需要处理的学生ID列表
            force_refresh_ids: 需要强制刷新的学生ID集合
        
        Returns:
            (成功数量, 失败数量)
        """
        if force_refresh_ids is None:
            force_refresh_ids = set()
            
        force_refresh_schools = len(force_refresh_ids) > 0
        
        # 1. 获取学校列表
        schools_map, error = await self.client.fetch_schools_data(force_refresh=force_refresh_schools)
        if error:
            logging.error(f"获取学校数据失败: {error}")
            schools_map = {}
        else:
            logging.info(f"成功获取 {len(schools_map)} 个学校数据")
        
        # 2. 创建并发任务
        semaphore = asyncio.Semaphore(self.max_concurrent)
        tasks = [
            process_student_id(
                student_id, 
                self.client, 
                semaphore, 
                self.delay, 
                force_refresh=(student_id in force_refresh_ids), 
                schools_map=schools_map
            )
            for student_id in student_ids
        ]
        
        success_count = 0
        fail_count = 0
        
        action_name = f"处理 {len(student_ids)} 个学生数据"
        logging.info(f"开始{action_name}，其中 {len(force_refresh_ids)} 个需要强制刷新...")
        
        # 3. 执行并收集结果
        total_count = len(student_ids)
        for i, future in enumerate(asyncio.as_completed(tasks), 1):
            student_id, status, from_cache = await future
            
            progress_prefix = f"[{i}/{total_count}]"
            refresh_status = "强制刷新" if student_id in force_refresh_ids else "缓存"
            
            if status.startswith("success"):
                logging.info(f"{progress_prefix} ID: {student_id} -> {status} ({refresh_status})")
                success_count += 1
            else:
                logging.info(f"{progress_prefix} ID: {student_id} -> 失败: {status}")
                fail_count += 1
        
        return success_count, fail_count
