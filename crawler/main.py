"""
BA-characters-internal-id 爬虫入口

数据流程：
1. 爬虫更新缓存
2. 从缓存生成中间JSON（schools.json, students.json, spines.json）
3. 从中间JSON生成最终CSV（students_data.csv）
"""

import asyncio
import argparse
import logging
from dataclasses import fields
from typing import Any

import httpx

from .config import (
    MAX_CONCURRENT_REQUESTS,
    REQUEST_DELAY_SECONDS,
    OUTPUT_DIR,
)
from .cache import CacheManager
from .api import APIClient
from .output import OutputWriter, StudentAggregator, CsvGenerator
from .crawler import Crawler
from .models import School


# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)


# --- 从缓存生成中间文件 ---

async def generate_intermediate_files(cache_manager: CacheManager):
    """
    从缓存数据生成中间JSON文件
    """
    logging.info("正在从缓存生成中间文件...")
    
    # 1. 读取学校数据
    schools_map = await cache_manager.get_schools()
    schools_list = [
        School(id=sid, name=s.get("name", ""), name_cn=s.get("name_cn", ""), logo=s.get("logo", ""))
        for sid, s in schools_map.items()
    ]
    
    # 2. 读取所有学生缓存
    student_ids = cache_manager.list_student_ids()
    student_cache_data: dict[int, dict[str, Any]] = {}
    
    for sid in student_ids:
        data = await cache_manager.get_student(sid)
        if data:
            student_cache_data[sid] = data
    
    # 3. 读取所有Spine缓存
    spine_ids = cache_manager.list_spine_ids()
    spine_cache_data: dict[int, dict[str, Any]] = {}
    
    for sid in spine_ids:
        data = await cache_manager.get_spine(sid)
        if data:
            spine_cache_data[sid] = data
    
    # 4. 聚合数据
    aggregator = StudentAggregator(schools_map)
    students, spines = aggregator.aggregate(student_cache_data, spine_cache_data)
    
    # 5. 写入文件
    writer = OutputWriter()
    writer.write_schools_json(schools_list)
    writer.write_students_json(students)
    writer.write_spines_json(spines)
    
    logging.info(f"中间文件生成完成：{len(schools_list)} 学校, {len(students)} 学生, {len(spines)} Spine")


# --- 爬虫主流程 ---

async def run_crawler(
    check_mode: bool = False,
    max_concurrent: int = MAX_CONCURRENT_REQUESTS,
    delay: float = REQUEST_DELAY_SECONDS,
    test_id: int | None = None,
    no_cache_overwrite: bool = False
):
    """主执行函数"""
    cache_manager = CacheManager()
    
    async with httpx.AsyncClient() as http_client:
        client = APIClient(http_client, cache_manager)

        # 模式一：测试模式
        if test_id is not None:
            await run_test_mode(client, test_id, no_cache_overwrite)
            return

        # --- 正式运行流程 ---
        
        logging.info("正在初始化...")

        # 1. 获取远程状态
        remote_max_student_id, remote_max_spine_id = await client.get_remote_max_ids()
        logging.info(f"远程最新状态: Student ID {remote_max_student_id}, Spine ID {remote_max_spine_id}")

        if remote_max_student_id == 0:
            logging.error("无法获取远程数据，程序终止")
            return

        # 2. 获取本地状态
        local_state = await cache_manager.get_state()
        local_max_student_id = local_state.get("max_student_id", 0)
        local_max_spine_id = local_state.get("max_spine_id", 0)
        logging.info(f"本地缓存状态: Student ID {local_max_student_id}, Spine ID {local_max_spine_id}")
        
        logging.info(f"配置: 最大并发请求数 {max_concurrent}, 请求延迟 {delay} 秒")
        
        # 3. 比较状态并构建需要强制刷新的ID集合
        ids_to_force_refresh: set[int] = set()

        if remote_max_student_id > local_max_student_id:
            new_ids = set(range(local_max_student_id + 1, remote_max_student_id + 1))
            ids_to_force_refresh.update(new_ids)
            logging.info(f"检测到新增页面ID: {local_max_student_id + 1} ~ {remote_max_student_id} (共 {len(new_ids)} 个)")

        if remote_max_student_id > local_max_student_id or remote_max_spine_id > local_max_spine_id:
            recently_updated_ids = await client.fetch_recently_updated_ids()
            if recently_updated_ids:
                ids_to_force_refresh.update(recently_updated_ids)
                logging.info(f"获取到最近更新的页面ID: {len(recently_updated_ids)} 个")

        logging.info(f"需要强制刷新的页面ID总数: {len(ids_to_force_refresh)}")
        
        # 模式二：检查更新模式
        if check_mode:
            logging.info("检查更新模式已启用，跳过后续爬取和写入操作。")
            return
        
        # 模式三：执行爬取（仅更新缓存）
        crawler = Crawler(client, cache_manager, max_concurrent, delay)
        page_ids = list(range(1, remote_max_student_id + 1))

        if ids_to_force_refresh:
            logging.info("检测到更新，开始增量刷新数据...")
        else:
            logging.info("当前数据已是最新，从缓存加载。")

        success_count, fail_count = await crawler.run(page_ids, force_refresh_ids=ids_to_force_refresh)

        if ids_to_force_refresh:
            logging.info("更新完成，保存状态...")
            await cache_manager.save_state(remote_max_student_id, remote_max_spine_id)
        
        logging.info("-" * 40)
        logging.info(f"学生数据请求: {client.student_req_count}")
        logging.info(f"Spine 数据请求: {client.spine_req_count}")
        logging.info(f"成功: {success_count}, 失败: {fail_count}")

    # --- 文件写入部分 ---
    
    # 1. 从缓存生成中间文件
    await generate_intermediate_files(cache_manager)
    
    # 2. 从中间文件生成最终CSV
    csv_generator = CsvGenerator()
    all_student_forms, skipped_records = csv_generator.generate_student_forms()
    
    # 写入 CSV
    writer = OutputWriter()
    writer.write_students_csv(all_student_forms)
    writer.write_skipped_csv(skipped_records)


async def run_test_mode(client: APIClient, test_id: int, no_cache_overwrite: bool = False):
    """测试模式：获取并打印单个学生ID的数据"""
    logging.info(f"测试模式已启用，ID: {test_id}")
    
    # 获取学校列表
    schools_map, error = await client.fetch_schools_data()
    if error:
        logging.error(f"获取学校数据失败: {error}")
        schools_map = {}
    else:
        logging.info(f"成功获取 {len(schools_map)} 个学校数据")
    
    # 获取学生数据
    student_data, error_msg, from_cache = await client.fetch_student_data(test_id, force_refresh=not no_cache_overwrite)
    
    if not student_data or student_data.get('code') != 2000:
        logging.warning(f"学生ID {test_id} 不存在或获取失败: {error_msg}")
        print(f"\n学生ID {test_id}: 获取失败 - {error_msg or '未知错误'}")
        return

    # 打印结果
    data = student_data.get('data', {})
    school_id = data.get('school', 0)
    school_name = schools_map.get(school_id, {}).get('name', '') if isinstance(school_id, int) else ''
    
    print(f"\n=== 学生ID {test_id} ===")
    print(f"姓名: {data.get('family_name', '')} {data.get('given_name', '')}")
    print(f"学校: {school_name}")
    print(f"Spine IDs: {data.get('spine', [])}")
    print(f"数据来源: {'缓存' if from_cache else 'API'}")


async def list_info():
    """列出当前缓存信息"""
    cache_manager = CacheManager()
    
    local_state = await cache_manager.get_state()
    local_max_student_id = local_state.get("max_student_id", 0)
    local_max_spine_id = local_state.get("max_spine_id", 0)
    
    print("=== 当前缓存信息 ===")
    print(f"本地最大学生ID: {local_max_student_id}")
    print(f"本地最大Spine ID: {local_max_spine_id}")
    
    student_files = list(cache_manager.students_dir.glob("*.json"))
    spine_files = list(cache_manager.spines_dir.glob("*.json"))
    
    print(f"缓存学生文件数: {len(student_files)}")
    print(f"缓存Spine文件数: {len(spine_files)}")
    print("===================")


async def generate_only():
    """仅从缓存生成中间文件和最终CSV，不执行爬取"""
    cache_manager = CacheManager()
    
    # 1. 生成中间文件
    await generate_intermediate_files(cache_manager)
    
    # 2. 从中间文件生成最终CSV
    csv_generator = CsvGenerator()
    all_student_forms, skipped_records = csv_generator.generate_student_forms()
    
    # 写入 CSV
    writer = OutputWriter()
    writer.write_students_csv(all_student_forms)
    writer.write_skipped_csv(skipped_records)


def main():
    """命令行入口"""
    arg_parser = argparse.ArgumentParser(description="BA-characters-internal-id")
    arg_parser.add_argument("--check", "-c", action="store_true", help="检查更新模式")
    arg_parser.add_argument("--test", "-t", type=int, metavar="ID", help="测试模式")
    arg_parser.add_argument("--list", "-l", action="store_true", help="列出缓存信息")
    arg_parser.add_argument("--generate", "-g", action="store_true", help="仅生成文件")
    arg_parser.add_argument("--max-concurrent", "-m", type=int, default=3, help="最大并发数")
    arg_parser.add_argument("--delay", "-d", type=float, default=2.0, help="请求延迟")
    arg_parser.add_argument("--no-cache-overwrite", action="store_true", help="不覆盖缓存")
    args = arg_parser.parse_args()
    
    if args.list:
        asyncio.run(list_info())
    elif args.generate:
        asyncio.run(generate_only())
    else:
        asyncio.run(run_crawler(
            check_mode=args.check, 
            max_concurrent=args.max_concurrent, 
            delay=args.delay, 
            test_id=args.test, 
            no_cache_overwrite=args.no_cache_overwrite
        ))


if __name__ == "__main__":
    main()
