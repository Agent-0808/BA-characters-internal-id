"""
爬虫工作流模块

职责：仅负责更新缓存，不做数据解析
"""

import asyncio
import logging
from typing import Any

from .cache import CacheManager
from .api import APIClient


async def process_page_id(
    page_id: int,
    client: APIClient,
    semaphore: asyncio.Semaphore,
    delay: float,
    force_refresh: bool = False,
    schools_map: dict[int, dict[str, Any]] | None = None
) -> tuple[int, str, bool, int | None]:
    """
    获取单个KivoWiki页面ID的数据并更新缓存。
    
    Returns:
        (page_id, status, from_cache, updated_at) - 状态为 "success" 或错误信息，
        updated_at 为页面数据的更新时间戳（Unix秒），失败时为 None
    """
    schools_map = schools_map or {}
    async with semaphore:
        # 获取数据
        json_data, fetch_reason, from_cache = await client.fetch_student_data(page_id)
        
        # 如果强制刷新且数据来自缓存，则重新获取
        if force_refresh and from_cache:
            logging.debug(f"ID {page_id}: 强制刷新，清除缓存并重新获取")
            json_data, fetch_reason, from_cache = await client.fetch_student_data(page_id, force_refresh=True)
        
        # 如果数据不是来自缓存，执行延迟
        if not from_cache:
            await asyncio.sleep(delay)

        if not json_data:
            return page_id, fetch_reason or "未知网络原因", from_cache, None

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
        name = ' '.join(name_parts) if name_parts else f"ID {page_id}"

        # 提取页面更新时间戳（供增量探测做水位线判断）
        updated_at = json_data.get('data', {}).get('updated_at')

        return page_id, f"success: {name} ({school_name})", from_cache, updated_at


class Crawler:
    """核心爬虫工作流 - 仅负责更新缓存"""

    def __init__(self, client: APIClient, cache_manager: CacheManager, max_concurrent: int, delay: float):
        self.client = client
        self.cache_manager = cache_manager
        self.max_concurrent = max_concurrent
        self.delay = delay

    async def run(
        self,
        page_ids: list[int],
        force_refresh_ids: set[int] | None = None,
        force_refresh_schools: bool | None = None
    ) -> tuple[int, int]:
        """
        执行爬取或缓存读取流程，仅更新缓存。

        Args:
            page_ids: 需要处理的KivoWiki页面ID列表
            force_refresh_ids: 需要强制刷新的页面ID集合
            force_refresh_schools: 是否强制刷新学校数据，None 时按 force_refresh_ids 是否非空判断

        Returns:
            (成功数量, 失败数量)
        """
        if force_refresh_ids is None:
            force_refresh_ids = set()
        if force_refresh_schools is None:
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
            process_page_id(
                page_id,
                self.client,
                semaphore,
                self.delay,
                force_refresh=(page_id in force_refresh_ids),
                schools_map=schools_map
            )
            for page_id in page_ids
        ]

        success_count = 0
        fail_count = 0

        action_name = f"处理 {len(page_ids)} 个页面数据"
        logging.info(f"开始{action_name}，其中 {len(force_refresh_ids)} 个需要强制刷新...")

        # 3. 执行并收集结果
        total_count = len(page_ids)
        for i, future in enumerate(asyncio.as_completed(tasks), 1):
            page_id, status, from_cache, _ = await future

            progress_prefix = f"[{i}/{total_count}]"
            refresh_status = "强制刷新" if page_id in force_refresh_ids else "缓存"

            if status.startswith("success"):
                logging.info(f"{progress_prefix} ID: {page_id} -> {status} ({refresh_status})")
                success_count += 1
            else:
                logging.info(f"{progress_prefix} ID: {page_id} -> 失败: {status}")
                fail_count += 1

        return success_count, fail_count

    async def probe_updated_pages(
        self,
        candidate_ids: list[int],
        watermark: int | None,
        window_size: int
    ) -> tuple[set[int], bool]:
        """
        增量探测最近更新的页面（探测即刷新）。

        候选列表按 updated_at 降序排列，逐个强制刷新并写缓存；一旦某页面的
        updated_at 不晚于水位线，可断定后续页面均无更新，立即停止探测。
        由此无新增ID时的内容修改也能被捕获，且请求数约等于实际更新数 + 1。

        Args:
            candidate_ids: 按更新时间降序排列的候选页面ID
            watermark: 上次运行记录的水位线（服务器Unix时间戳），None 表示首次探测
            window_size: 候选窗口大小，用于判断候选是否已覆盖全部最近更新

        Returns:
            (已刷新的页面ID集合, 是否可推进水位线)
            可推进 = 全部候选探测顺利，且（触及边界 或 候选数未满窗口）
        """
        if not candidate_ids:
            return set(), True

        refreshed: set[int] = set()
        failed = False
        boundary_hit = False
        semaphore = asyncio.Semaphore(1)

        logging.info(f"开始增量探测最近更新的页面（候选 {len(candidate_ids)} 个，水位线: {watermark}）...")

        for page_id in candidate_ids:
            _, status, _, updated_at = await process_page_id(
                page_id, self.client, semaphore, self.delay, force_refresh=True
            )
            if not status.startswith("success"):
                logging.warning(f"探测页面 {page_id} 失败: {status}，跳过")
                failed = True
                continue

            refreshed.add(page_id)

            # 触及边界：该页面自上次运行后无更新，后续页面必然更旧
            if watermark is not None and updated_at is not None and updated_at <= watermark:
                logging.info(f"页面 {page_id} 的 updated_at ({updated_at}) 不晚于水位线，探测结束")
                boundary_hit = True
                break

        # 存在失败时不推进水位线，已刷新页面下次运行会再次探测（无副作用）
        advance = not failed and (boundary_hit or len(candidate_ids) < window_size)
        logging.info(f"增量探测完成: 刷新 {len(refreshed)} 个页面, 水位线{'可' if advance else '不可'}推进")
        return refreshed, advance
