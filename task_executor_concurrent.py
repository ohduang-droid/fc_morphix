"""
任务执行脚本（并发版本）
包含四个步骤，每个步骤先进行校验
使用线程池并发处理多个Creator，并发数为3
"""
import os
import sys
import threading
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from steps import step_one, step_two, step_three, step_four
from steps.step_two import call_dify_api_and_save, update_creator_outreach_email_body
from utils.creator_status import (
    update_creator_status,
    PENDING,
    GENERATING,
    COMPLETED,
    FAILED
)
from utils.cache import load_cache


def load_env_file(path: str = ".env"):
    """Load environment variables from a .env file (supports `export KEY=value`)."""
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"\'')
            os.environ[key] = value


def process_single_creator(
    creator: Dict[str, Any],
    creator_idx: int,
    total_creators: int,
    step_one_result: Dict[str, Any],
    kwargs: Dict[str, Any],
    results_lock: threading.Lock,
    all_step_two_results: List[Any],
    all_step_three_results: List[Any],
    all_step_four_results: List[Any]
) -> Tuple[bool, Dict[str, Any]]:
    """
    处理单个Creator的所有步骤（第二步到第四步）
    
    Args:
        creator: Creator信息
        creator_idx: Creator索引（从1开始）
        total_creators: Creator总数
        step_one_result: 第一步的执行结果
        kwargs: 其他配置参数
        results_lock: 结果列表的线程锁
        all_step_two_results: 所有第二步结果的列表
        all_step_three_results: 所有第三步结果的列表
        all_step_four_results: 所有第四步结果的列表
    
    Returns:
        (是否成功, 结果字典)
    """
    creator_id = creator.get("creator_id") or creator.get("id")
    supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
    supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
    
    import time
    import threading
    thread_id = threading.current_thread().ident
    start_time = time.time()
    
    print(f"\n{'='*60}")
    print(f"[线程 {thread_id}] 开始处理 Creator {creator_idx}/{total_creators} (creator_id: {creator_id})")
    print(f"[调试] 线程 {thread_id} 开始时间: {time.strftime('%H:%M:%S', time.localtime(start_time))}")
    print(f"{'='*60}")
    
    # 更新状态为 GENERATING（生成中）
    if supabase_url and supabase_api_key:
        print(f"[Creator {creator_idx}] 🔄 更新状态为 GENERATING...")
        update_creator_status(
            creator_id=creator_id,
            status=GENERATING,
            supabase_url=supabase_url,
            supabase_api_key=supabase_api_key
        )
    
    try:
        # 为当前Creator准备参数
        creator_kwargs = kwargs.copy()
        creator_kwargs["creators"] = [creator]  # 只包含当前Creator
        creator_kwargs["step_one_result"] = step_one_result
        
        # 第二步：为当前Creator调用 Dify 接口生成 prompt，并写入 Supabase（如果需要）
        step_two_result = None
        creator_step_two_result = []
        
        # 检查是否需要执行第二步
        steps_to_execute = kwargs.get("steps_to_execute", {"1", "2", "3", "4"})
        
        if "2" in steps_to_execute:
            step_two_start = time.time()
            print(f"\n[Creator {creator_idx}] [线程 {thread_id}] 开始执行第二步：调用 Dify 接口生成 prompt...")
            
            # 直接调用 call_dify_api_and_save 以实现真正的并发，而不是通过 step_two.execute
            
            # 获取配置
            dify_url = kwargs.get("dify_url") or os.getenv("DIFY_URL")
            dify_api_key = kwargs.get("dify_api_key") or os.getenv("DIFY_API_KEY")
            dify_user = kwargs.get("dify_user") or os.getenv("DIFY_USER", "task-executor")
            use_cache = kwargs.get("use_cache", True)
            
            # 检查缓存
            if use_cache:
                cached_result = load_cache(creator_id, "step_two")
                if cached_result:
                    print(f"[Creator {creator_idx}] [线程 {thread_id}] ✓ 使用缓存结果，跳过 Dify API 调用")
                    result = cached_result
                else:
                    # 直接调用 call_dify_api_and_save 实现并发
                    result = call_dify_api_and_save(
                        creator,
                        dify_url,
                        dify_api_key,
                        dify_user,
                        supabase_url,
                        supabase_api_key,
                        use_cache=use_cache
                    )
            else:
                result = call_dify_api_and_save(
                    creator,
                    dify_url,
                    dify_api_key,
                    dify_user,
                    supabase_url,
                    supabase_api_key,
                    use_cache=False
                )
            
            # 更新 creator 表的 outreach_email_body 和 message 字段
            if supabase_url and supabase_api_key:
                dify_response = result.get("dify_response", {})
                outlook_text = dify_response.get("outlook", "")
                message_text = dify_response.get("message", "")
                if outlook_text:
                    update_creator_outreach_email_body(
                        creator_id,
                        outlook_text,
                        supabase_url,
                        supabase_api_key,
                        message_text=message_text if message_text else None
                    )
            
            # 构建 step_two_result 格式
            step_two_result = {
                "step": 2,
                "status": "completed",
                "message": f"成功处理 creator {creator_id}",
                "results": [result],
                "error_count": result.get("error_count", 0)
            }
            
            creator_step_two_result = [result]
            if creator_step_two_result:
                with results_lock:
                    all_step_two_results.extend(creator_step_two_result)
            
            step_two_elapsed = time.time() - step_two_start
            print(f"[Creator {creator_idx}] [线程 {thread_id}] 第二步执行完成，耗时 {step_two_elapsed:.2f} 秒")
            print(f"[Creator {creator_idx}] 第二步完成: {step_two_result['message']}")
        else:
            # 从缓存加载第二步结果
            cached_step_two = load_cache(creator_id, "step_two")
            if cached_step_two:
                step_two_result = {
                    "step": 2,
                    "status": "completed",
                    "message": "从缓存加载",
                    "results": [cached_step_two],
                    "cache_loaded": True
                }
                creator_step_two_result = [cached_step_two]
                print(f"[Creator {creator_idx}] ✓ 从缓存加载第二步结果")
            else:
                raise Exception(f"第二步缓存未找到，请先执行第1-2步")
        
        # 将第二步的结果传递给第三步
        creator_kwargs["step_two_result"] = step_two_result
        creator_kwargs["dify_results"] = creator_step_two_result
        
        # 第三步：为当前Creator的magnet生成图片（如果需要）
        step_three_result = None
        creator_step_three_result = []
        
        if "3" in steps_to_execute:
            step_three_start = time.time()
            print(f"\n[Creator {creator_idx}] [线程 {thread_id}] 开始执行第三步：为magnet生成图片...")
            step_three_result = step_three.execute(**creator_kwargs)
            step_three_elapsed = time.time() - step_three_start
            print(f"[Creator {creator_idx}] [线程 {thread_id}] 第三步执行完成，耗时 {step_three_elapsed:.2f} 秒")
            
            # 检查第三步是否有错误
            if step_three_result.get("status") != "completed" or len(step_three_result.get("errors", [])) > 0:
                raise Exception(f"第三步执行失败或存在错误: {step_three_result.get('message', '未知错误')}")
            
            creator_step_three_result = step_three_result.get("results", [])
            if creator_step_three_result:
                with results_lock:
                    all_step_three_results.extend(creator_step_three_result)
            print(f"[Creator {creator_idx}] 第三步完成: {step_three_result['message']}")
        else:
            # 如果不需要执行第三步，但需要执行第四步，则从缓存加载
            if "4" in steps_to_execute:
                cached_step_three = load_cache(creator_id, "step_three")
                if cached_step_three:
                    step_three_result = {
                        "step": 3,
                        "status": "completed",
                        "message": "从缓存加载",
                        "results": [cached_step_three],
                        "cache_loaded": True
                    }
                    creator_step_three_result = [cached_step_three]
                    print(f"[Creator {creator_idx}] ✓ 从缓存加载第三步结果")
                else:
                    raise Exception(f"第三步缓存未找到，请先执行第3步")
        
        # 将第三步的结果传递给第四步
        if step_three_result:
            creator_kwargs["step_three_result"] = step_three_result
        
        # 第四步：为当前Creator生成场景图（如果需要）
        step_four_result = None
        creator_step_four_result = []
        
        if "4" in steps_to_execute:
            step_four_start = time.time()
            print(f"\n[Creator {creator_idx}] [线程 {thread_id}] 开始执行第四步：生成场景图...")
            step_four_result = step_four.execute(**creator_kwargs)
            step_four_elapsed = time.time() - step_four_start
            print(f"[Creator {creator_idx}] [线程 {thread_id}] 第四步执行完成，耗时 {step_four_elapsed:.2f} 秒")
            
            # 检查第四步是否有错误
            if step_four_result.get("status") != "completed" or len(step_four_result.get("errors", [])) > 0:
                raise Exception(f"第四步执行失败或存在错误: {step_four_result.get('message', '未知错误')}")
            
            creator_step_four_result = step_four_result.get("results", [])
            if creator_step_four_result:
                with results_lock:
                    all_step_four_results.extend(creator_step_four_result)
            print(f"[Creator {creator_idx}] 第四步完成: {step_four_result['message']}")
        
        # 所有步骤成功完成，更新状态为 COMPLETED
        if supabase_url and supabase_api_key:
            print(f"[Creator {creator_idx}] 🔄 更新状态为 COMPLETED...")
            update_creator_status(
                creator_id=creator_id,
                status=COMPLETED,
                supabase_url=supabase_url,
                supabase_api_key=supabase_api_key
            )
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"\n[Creator {creator_idx}] ✓ 所有步骤执行完成！")
        print(f"[调试] 线程 {thread_id} 处理 Creator {creator_idx} 总耗时: {elapsed_time:.2f} 秒")
        
        return True, {
            "creator_id": creator_id,
            "creator_idx": creator_idx,
            "step_two_result": step_two_result,
            "step_three_result": step_three_result,
            "step_four_result": step_four_result,
            "thread_id": thread_id,
            "elapsed_time": elapsed_time
        }
        
    except Exception as creator_error:
        error_msg = f"Creator {creator_idx} (creator_id: {creator_id}) 处理失败: {str(creator_error)}"
        print(f"\n[Creator {creator_idx}] ❌ {error_msg}")
        
        # 更新状态为 FAILED（生成失败）
        if supabase_url and supabase_api_key:
            print(f"[Creator {creator_idx}] 🔄 更新状态为 FAILED...")
            update_creator_status(
                creator_id=creator_id,
                status=FAILED,
                supabase_url=supabase_url,
                supabase_api_key=supabase_api_key
            )
        
        return False, {
            "creator_id": creator_id,
            "creator_idx": creator_idx,
            "error": str(creator_error)
        }


class TaskExecutorConcurrent:
    """并发任务执行器"""
    
    def __init__(self, max_workers: int = 3):
        """
        初始化并发任务执行器
        
        Args:
            max_workers: 最大并发数，默认为3
        """
        self.step_results: Dict[str, Any] = {}
        self.max_workers = max_workers
    
    def execute_all_steps(self, creator_id: str = None, steps: str = "1,2,3,4", **kwargs) -> Dict[str, Any]:
        """
        执行所有步骤（并发版本）
        新的执行逻辑：
        1. 第一步：获取所有Creator
        2. 使用线程池并发处理每个Creator，依次执行2-4步，并发数为3
        
        Args:
            creator_id: 可选，如果指定则只处理该 creator_id 的 Creator
            steps: 要执行的步骤，格式为 "1,2" 或 "3,4" 或 "1,2,3,4"，默认为 "1,2,3,4"
        """
        # 解析步骤参数
        step_list = [s.strip() for s in steps.split(",")]
        step_set = set(step_list)
        
        # 添加调试信息
        print(f"[调试] 解析步骤参数: steps='{steps}', step_list={step_list}, step_set={step_set}")
        
        # 验证步骤参数
        valid_steps = {"1", "2", "3", "4"}
        if not step_set.issubset(valid_steps):
            return {
                "status": "failed",
                "error": f"无效的步骤参数: {steps}，有效值为 1, 2, 3, 4",
                "completed_steps": self.step_results
            }
        
        # 检查步骤依赖关系
        if "3" in step_set or "4" in step_set:
            # 如果执行第3步或第4步，需要先有第1步和第2步的结果
            if "1" not in step_set and "2" not in step_set:
                # 尝试从缓存加载
                print("⚠️  执行第3步或第4步需要第1步和第2步的结果，尝试从缓存加载...")
        try:
            # 第一步：从 Supabase 获取所有 creator 信息（如果需要）
            step_one_result = None
            if "1" in step_set:
                if creator_id:
                    print(f"开始执行第一步：从 Supabase 获取 creator 信息 (creator_id: {creator_id})...")
                else:
                    print("开始执行第一步：从 Supabase 获取 creator 信息...")
                step_one_result = step_one.execute(**kwargs)
                self.step_results["step_one"] = step_one_result
                print(f"第一步完成: {step_one_result['message']}")
            else:
                # 尝试从缓存加载第一步结果
                print("跳过第一步，尝试从缓存加载...")
                cached_step_one = load_cache("all_creators", "step_one")
                if cached_step_one:
                    step_one_result = cached_step_one
                    print(f"✓ 从缓存加载第一步结果: {step_one_result['message']}")
                else:
                    return {
                        "status": "failed",
                        "error": "第一步结果未找到，且缓存中也没有。请先执行第一步。",
                        "completed_steps": self.step_results
                    }
            
            # 获取所有Creator列表
            all_creators = step_one_result.get("creators", [])
            if not all_creators:
                return {
                    "status": "success",
                    "message": "没有找到任何Creator",
                    "steps": {
                        "step_one": step_one_result
                    }
                }
            
            # 按 paid_subscribers_est 和 free_subscribers_est 降序排序
            def get_creator_sort_key(creator: Dict[str, Any]) -> tuple:
                """获取用于排序的键值，按 paid_subscribers_est 和 free_subscribers_est 降序
                如果字段缺失或为 None，则视为 0
                """
                paid_subscribers = creator.get("paid_subscribers_est")
                free_subscribers = creator.get("free_subscribers_est")
                
                # 将 None 或无效值转换为 0
                paid_subscribers = paid_subscribers if paid_subscribers is not None else 0
                free_subscribers = free_subscribers if free_subscribers is not None else 0
                
                # 转换为数字类型（处理可能的字符串类型）
                try:
                    paid_subscribers = float(paid_subscribers) if paid_subscribers else 0
                except (ValueError, TypeError):
                    paid_subscribers = 0
                
                try:
                    free_subscribers = float(free_subscribers) if free_subscribers else 0
                except (ValueError, TypeError):
                    free_subscribers = 0
                
                # 返回元组用于排序，使用负值实现降序
                return (-paid_subscribers, -free_subscribers)
            
            all_creators.sort(key=get_creator_sort_key)
            
            # 如果指定了 creator_id，则只处理该 Creator
            if creator_id:
                all_creators = [
                    creator for creator in all_creators
                    if (creator.get("creator_id") == creator_id or creator.get("id") == creator_id)
                ]
                if not all_creators:
                    return {
                        "status": "failed",
                        "error": f"未找到 creator_id 为 {creator_id} 的 Creator",
                        "steps": {
                            "step_one": step_one_result
                        }
                    }
                print(f"已筛选出 1 个 Creator (creator_id: {creator_id})")
            
            # 存储所有Creator的执行结果（使用线程锁保护）
            all_step_two_results = []
            all_step_three_results = []
            all_step_four_results = []
            results_lock = threading.Lock()
            
            # 获取 Supabase 配置用于状态更新
            supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
            supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
            
            # 如果只需要执行第1步，直接返回
            if step_set == {"1"}:
                return {
                    "status": "success",
                    "message": f"第一步执行完成，共获取 {len(all_creators)} 个Creator",
                    "steps": {
                        "step_one": step_one_result
                    }
                }
            
            # 如果只需要执行第1-2步，也使用并发处理（不再使用特殊分支）
            # 注释掉原来的非并发分支，让它走下面的并发处理逻辑
            # if step_set == {"1", "2"}:
            #     print(f"\n只执行第1-2步，跳过第3-4步...")
            #     # 执行第二步（非并发，因为第二步本身已经处理了所有creator）
            #     print(f"\n开始执行第二步：调用 Dify 接口生成 prompt...")
            #     step_two_kwargs = kwargs.copy()
            #     step_two_kwargs["creators"] = all_creators
            #     step_two_kwargs["step_one_result"] = step_one_result
            #     step_two_result = step_two.execute(**step_two_kwargs)
            #     self.step_results["step_two"] = step_two_result
            #     print(f"第二步完成: {step_two_result['message']}")
            #     
            #     return {
            #         "status": "success",
            #         "message": f"第1-2步执行完成，共处理 {len(all_creators)} 个Creator",
            #         "steps": {
            #             "step_one": step_one_result,
            #             "step_two": step_two_result
            #         }
            #     }
            
            # 如果只执行第3-4步，需要从缓存加载第二步结果
            if step_set == {"3", "4"} or step_set == {"3"} or step_set == {"4"}:
                print(f"\n只执行第3-4步，从缓存加载第1-2步结果...")
                # 为每个creator加载第二步的缓存结果，只处理有缓存的Creator
                all_step_two_results = []
                missing_cache_count = 0
                creators_with_cache = []
                
                for creator in all_creators:
                    creator_id_for_cache = creator.get("creator_id") or creator.get("id")
                    cached_step_two = load_cache(creator_id_for_cache, "step_two")
                    if cached_step_two:
                        all_step_two_results.append(cached_step_two)
                        creators_with_cache.append(creator)
                    else:
                        missing_cache_count += 1
                        if missing_cache_count <= 10:  # 只打印前10个，避免日志过多
                            print(f"⚠️  Creator {creator_id_for_cache} 的第二步缓存未找到")
                
                if missing_cache_count > 0:
                    print(f"⚠️  共有 {missing_cache_count} 个Creator的第二步缓存未找到，将跳过这些Creator，只处理有缓存的 {len(creators_with_cache)} 个Creator")
                
                # 只处理有缓存的Creator
                if len(creators_with_cache) == 0:
                    return {
                        "status": "failed",
                        "error": f"所有Creator的第二步缓存都未找到，请先执行第1-2步",
                        "completed_steps": self.step_results
                    }
                
                # 更新 all_creators 为只包含有缓存的Creator
                all_creators = creators_with_cache
                
                # 构建第二步结果结构
                step_two_result = {
                    "step": 2,
                    "status": "completed",
                    "message": f"从缓存加载了 {len(all_step_two_results)} 个 creator 的第二步结果",
                    "results": all_step_two_results,
                    "total": len(all_creators),
                    "success_count": len(all_step_two_results),
                    "cache_loaded": True
                }
                self.step_results["step_two"] = step_two_result
                print(f"✓ 从缓存加载第二步结果: {len(all_step_two_results)} 个Creator，将并发处理这些Creator")
            
            # 使用线程池并发处理每个Creator
            print(f"\n{'='*80}")
            print(f"[调试] 进入并发处理逻辑")
            print(f"[调试] step_set = {step_set}")
            print(f"[调试] 需要执行的步骤: {step_set}")
            print(f"[调试] 总Creator数: {len(all_creators)}")
            print(f"{'='*80}")
            print(f"\n开始并发处理 {len(all_creators)} 个Creator（并发数: {self.max_workers}）...")
            print(f"[调试] 线程池配置: max_workers={self.max_workers}, 总Creator数={len(all_creators)}")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任务
                future_to_creator = {}
                import time
                start_time = time.time()
                
                for creator_idx, creator in enumerate(all_creators, 1):
                    creator_id = creator.get("creator_id") or creator.get("id")
                    # 将步骤信息传递给kwargs
                    creator_kwargs = kwargs.copy()
                    creator_kwargs["steps_to_execute"] = step_set
                    
                    print(f"[调试] 提交任务 {creator_idx}/{len(all_creators)} (creator_id: {creator_id}) 到线程池...")
                    future = executor.submit(
                        process_single_creator,
                        creator=creator,
                        creator_idx=creator_idx,
                        total_creators=len(all_creators),
                        step_one_result=step_one_result,
                        kwargs=creator_kwargs,
                        results_lock=results_lock,
                        all_step_two_results=all_step_two_results,
                        all_step_three_results=all_step_three_results,
                        all_step_four_results=all_step_four_results
                    )
                    future_to_creator[future] = (creator_idx, creator)
                
                submit_time = time.time() - start_time
                print(f"[调试] 所有任务提交完成，耗时 {submit_time:.2f} 秒，共提交 {len(future_to_creator)} 个任务")
                
                # 等待所有任务完成并收集结果
                completed_count = 0
                success_count = 0
                failed_count = 0
                
                import time
                first_completion_time = None
                
                for future in as_completed(future_to_creator):
                    creator_idx, creator = future_to_creator[future]
                    completed_count += 1
                    completion_time = time.time()
                    
                    if first_completion_time is None:
                        first_completion_time = completion_time
                        time_to_first = completion_time - start_time
                        print(f"[调试] 第一个任务完成，耗时 {time_to_first:.2f} 秒")
                    
                    try:
                        success, result = future.result()
                        thread_id = result.get("thread_id", "unknown")
                        elapsed_time = result.get("elapsed_time", 0)
                        
                        if success:
                            success_count += 1
                            print(f"\n[进度] {completed_count}/{len(all_creators)} 个Creator处理完成（成功: {success_count}, 失败: {failed_count}）")
                            print(f"[调试] Creator {creator_idx} 由线程 {thread_id} 处理，耗时 {elapsed_time:.2f} 秒")
                        else:
                            failed_count += 1
                            print(f"\n[进度] {completed_count}/{len(all_creators)} 个Creator处理完成（成功: {success_count}, 失败: {failed_count}）")
                    except Exception as e:
                        failed_count += 1
                        creator_id = creator.get("creator_id") or creator.get("id")
                        print(f"\n[Creator {creator_idx}] ❌ 处理异常: {str(e)}")
                        print(f"[进度] {completed_count}/{len(all_creators)} 个Creator处理完成（成功: {success_count}, 失败: {failed_count}）")
            
            print(f"\n所有Creator并发处理完成！成功: {success_count}, 失败: {failed_count}")
            
            # 构建汇总结果（只包含已执行的步骤）
            result_steps = {}
            if "1" in step_set:
                result_steps["step_one"] = step_one_result
            
            if "2" in step_set or "3" in step_set or "4" in step_set:
                if "2" in step_set:
                    final_step_two_result = {
                        "step": 2,
                        "status": "completed",
                        "message": f"成功处理 {len(all_step_two_results)} 个 creator 的第二步",
                        "results": all_step_two_results,
                        "total": len(all_creators),
                        "success_count": len(all_step_two_results)
                    }
                    self.step_results["step_two"] = final_step_two_result
                    result_steps["step_two"] = final_step_two_result
                elif step_two_result and step_two_result.get("cache_loaded"):
                    result_steps["step_two"] = step_two_result
            
            if "3" in step_set or "4" in step_set:
                if "3" in step_set:
                    final_step_three_result = {
                        "step": 3,
                        "status": "completed",
                        "message": f"成功处理 {len(all_step_three_results)} 个 creator 的第三步",
                        "results": all_step_three_results,
                        "total_creators": len(all_step_three_results)
                    }
                    self.step_results["step_three"] = final_step_three_result
                    result_steps["step_three"] = final_step_three_result
            
            if "4" in step_set:
                final_step_four_result = {
                    "step": 4,
                    "status": "completed",
                    "message": f"成功处理 {len(all_step_four_results)} 个 creator 的第四步",
                    "results": all_step_four_results,
                    "total_creators": len(all_step_four_results)
                }
                self.step_results["step_four"] = final_step_four_result
                result_steps["step_four"] = final_step_four_result
            
            return {
                "status": "success",
                "message": f"所有Creator处理完成！共处理 {len(all_creators)} 个Creator（成功: {success_count}, 失败: {failed_count}）",
                "steps": result_steps,
                "summary": {
                    "total": len(all_creators),
                    "success": success_count,
                    "failed": failed_count
                }
            }
        except Exception as e:
            return {
                "status": "failed",
                "error": str(e),
                "completed_steps": self.step_results
            }


def main():
    """主函数"""
    # 加载 .env 文件
    load_env_file()
    
    # 从命令行参数获取 creator_id 和 steps（如果提供）
    creator_id = None
    steps = "1,2,3,4"
    
    if len(sys.argv) > 1:
        creator_id = sys.argv[1]
        print(f"指定 creator_id: {creator_id}")
    
    if len(sys.argv) > 2:
        steps = sys.argv[2]
        print(f"指定执行步骤: {steps}")
    
    # 从环境变量获取 creator_id（如果命令行未提供）
    if not creator_id:
        creator_id = os.getenv("CREATOR_ID")
        if creator_id:
            print(f"从环境变量获取 creator_id: {creator_id}")
    
    # 从环境变量获取 steps（如果命令行未提供）
    if steps == "1,2,3,4":
        env_steps = os.getenv("STEPS")
        if env_steps:
            steps = env_steps
            print(f"从环境变量获取执行步骤: {steps}")
    
    # 从环境变量获取并发数，默认为3
    max_workers = int(os.getenv("MAX_WORKERS", "10"))
    print(f"并发数: {max_workers}")
    
    # 从环境变量获取 Supabase 配置，或使用默认值
    supabase_url = os.getenv(
        "SUPABASE_URL",
        "https://ifmtxstylwhasxmkkbby.supabase.co"
    )
    supabase_api_key = os.getenv(
        "SUPABASE_API_KEY",
        "sb_secret_oRvzMXZ_GReKQc9YvREjZg_9hs75FEB"
    )
    
    executor = TaskExecutorConcurrent(max_workers=max_workers)
    result = executor.execute_all_steps(
        creator_id=creator_id,
        steps=steps,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key
    )
    
    if result["status"] == "success":
        print("\n所有步骤执行成功！")
        if "summary" in result:
            summary = result["summary"]
            print(f"处理统计: 总计 {summary['total']} 个，成功 {summary['success']} 个，失败 {summary['failed']} 个")
        sys.exit(0)
    else:
        print(f"\n执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


if __name__ == "__main__":
    main()

