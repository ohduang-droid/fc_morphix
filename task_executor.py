"""
任务执行脚本
包含四个步骤，每个步骤先进行校验
"""
import os
import sys
from typing import Dict, Any

from steps import step_one, step_two, step_three, step_four
from utils.creator_status import (
    update_creator_status,
    PENDING,
    GENERATING,
    COMPLETED,
    FAILED
)


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


class TaskExecutor:
    """任务执行器"""
    
    def __init__(self):
        self.step_results: Dict[str, Any] = {}
    
    def execute_all_steps(self, creator_id: str = None, **kwargs) -> Dict[str, Any]:
        """
        执行所有步骤
        新的执行逻辑：
        1. 第一步：获取所有Creator
        2. 循环处理每个Creator，依次执行2-4步，然后进入下一个Creator
        
        Args:
            creator_id: 可选，如果指定则只处理该 creator_id 的 Creator
        """
        try:
            # 第一步：从 Supabase 获取所有 creator 信息
            if creator_id:
                print(f"开始执行第一步：从 Supabase 获取 creator 信息 (creator_id: {creator_id})...")
            else:
                print("开始执行第一步：从 Supabase 获取 creator 信息...")
            # 确保 use_cache 参数被正确传递
            use_cache = kwargs.get("use_cache", True)
            if not use_cache:
                print(f"  ℹ️  禁用缓存模式：use_cache={use_cache}")
            step_one_result = step_one.execute(**kwargs)
            self.step_results["step_one"] = step_one_result
            print(f"第一步完成: {step_one_result['message']}")
            
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
            
            # 存储所有Creator的执行结果
            all_step_two_results = []
            all_step_three_results = []
            all_step_four_results = []
            
            # 获取 Supabase 配置用于状态更新
            supabase_url = kwargs.get("supabase_url") or os.getenv("SUPABASE_URL")
            supabase_api_key = kwargs.get("supabase_api_key") or os.getenv("SUPABASE_API_KEY")
            
            # 循环处理每个Creator
            for creator_idx, creator in enumerate(all_creators, 1):
                creator_id = creator.get("creator_id") or creator.get("id")
                print(f"\n{'='*60}")
                print(f"开始处理 Creator {creator_idx}/{len(all_creators)} (creator_id: {creator_id})")
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
                    # 确保 use_cache 参数被正确传递
                    use_cache = creator_kwargs.get("use_cache", True)
                    if not use_cache:
                        print(f"[Creator {creator_idx}] ℹ️  禁用缓存模式：use_cache={use_cache}")
                    
                    # 第二步：为当前Creator调用 Dify 接口生成 prompt，并写入 Supabase
                    print(f"\n[Creator {creator_idx}] 开始执行第二步：调用 Dify 接口生成 prompt...")
                    step_two_result = step_two.execute(**creator_kwargs)
                    
                    # 检查第二步是否有错误
                    if step_two_result.get("status") != "completed" or step_two_result.get("error_count", 0) > 0:
                        raise Exception(f"第二步执行失败或存在错误: {step_two_result.get('message', '未知错误')}")
                    
                    creator_step_two_result = step_two_result.get("results", [])
                    if creator_step_two_result:
                        all_step_two_results.extend(creator_step_two_result)
                    print(f"[Creator {creator_idx}] 第二步完成: {step_two_result['message']}")
                    
                    # 将第二步的结果传递给第三步
                    creator_kwargs["step_two_result"] = step_two_result
                    creator_kwargs["dify_results"] = creator_step_two_result
                    
                    # 第三步：为当前Creator的magnet生成图片
                    print(f"\n[Creator {creator_idx}] 开始执行第三步：为magnet生成图片...")
                    step_three_result = step_three.execute(**creator_kwargs)
                    
                    # 检查第三步是否有错误
                    if step_three_result.get("status") != "completed" or len(step_three_result.get("errors", [])) > 0:
                        raise Exception(f"第三步执行失败或存在错误: {step_three_result.get('message', '未知错误')}")
                    
                    creator_step_three_result = step_three_result.get("results", [])
                    if creator_step_three_result:
                        all_step_three_results.extend(creator_step_three_result)
                    print(f"[Creator {creator_idx}] 第三步完成: {step_three_result['message']}")
                    
                    # 将第三步的结果传递给第四步
                    creator_kwargs["step_three_result"] = step_three_result
                    
                    # 第四步：为当前Creator生成场景图
                    print(f"\n[Creator {creator_idx}] 开始执行第四步：生成场景图...")
                    step_four_result = step_four.execute(**creator_kwargs)
                    
                    # 检查第四步是否有错误
                    if step_four_result.get("status") != "completed" or len(step_four_result.get("errors", [])) > 0:
                        raise Exception(f"第四步执行失败或存在错误: {step_four_result.get('message', '未知错误')}")
                    
                    creator_step_four_result = step_four_result.get("results", [])
                    if creator_step_four_result:
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
                    
                    print(f"\n[Creator {creator_idx}] ✓ 所有步骤执行完成！")
                    
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
                    
                    # 终止当前Creator的处理，继续处理下一个Creator
                    continue
            
            # 构建汇总结果
            final_step_two_result = {
                "step": 2,
                "status": "completed",
                "message": f"成功处理 {len(all_step_two_results)} 个 creator 的第二步",
                "results": all_step_two_results,
                "total": len(all_creators),
                "success_count": len(all_step_two_results)
            }
            
            final_step_three_result = {
                "step": 3,
                "status": "completed",
                "message": f"成功处理 {len(all_step_three_results)} 个 creator 的第三步",
                "results": all_step_three_results,
                "total_creators": len(all_step_three_results)
            }
            
            final_step_four_result = {
                "step": 4,
                "status": "completed",
                "message": f"成功处理 {len(all_step_four_results)} 个 creator 的第四步",
                "results": all_step_four_results,
                "total_creators": len(all_step_four_results)
            }
            
            self.step_results["step_two"] = final_step_two_result
            self.step_results["step_three"] = final_step_three_result
            self.step_results["step_four"] = final_step_four_result
            
            return {
                "status": "success",
                "message": f"所有Creator处理完成！共处理 {len(all_creators)} 个Creator",
                "steps": {
                    "step_one": step_one_result,
                    "step_two": final_step_two_result,
                    "step_three": final_step_three_result,
                    "step_four": final_step_four_result
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
    
    # 从命令行参数获取 creator_id（如果提供）
    creator_id = None
    if len(sys.argv) > 1:
        creator_id = sys.argv[1]
        print(f"指定 creator_id: {creator_id}")
    
    # 从环境变量获取 creator_id（如果命令行未提供）
    if not creator_id:
        creator_id = os.getenv("CREATOR_ID")
        if creator_id:
            print(f"从环境变量获取 creator_id: {creator_id}")
    
    # 从环境变量获取 Supabase 配置，或使用默认值
    supabase_url = os.getenv(
        "SUPABASE_URL",
        "https://ifmtxstylwhasxmkkbby.supabase.co"
    )
    supabase_api_key = os.getenv(
        "SUPABASE_API_KEY",
        "sb_secret_oRvzMXZ_GReKQc9YvREjZg_9hs75FEB"
    )
    
    executor = TaskExecutor()
    result = executor.execute_all_steps(
        creator_id=creator_id,
        supabase_url=supabase_url,
        supabase_api_key=supabase_api_key
    )
    
    if result["status"] == "success":
        print("\n所有步骤执行成功！")
        sys.exit(0)
    else:
        print(f"\n执行失败: {result.get('error', '未知错误')}")
        sys.exit(1)


if __name__ == "__main__":
    main()

