import os
import re
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Set
import argparse
import sys


def extract_query_results_from_file(file_path: Path) -> Tuple[str, Optional[str], bool]:
    """
    从单个文件(txt或log)中提取查询结果
    
    Args:
        file_path: 文件路径
        
    Returns:
        tuple: (查询名称, 时间字符串或失败标记, 是否成功)
    """
    query_name = ""
    time_result = None
    success = False
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 提取文件名中的查询编号
        file_name = file_path.stem
        
        # 支持多种文件名格式:
        # 1. q1.txt 或 q1_run.log -> 提取 q1
        # 2. Q1.txt 或 Q1_run.log -> 提取 Q1
        # 3. 其他格式的文件名
        
        # 首先尝试提取 qX 或 QX 格式
        match = re.search(r'[qQ](\d+)', file_name, re.IGNORECASE)
        if match:
            query_num = match.group(1)
            query_name = f"Q{query_num}"
        else:
            # 如果文件名中没有q+数字，则尝试其他模式
            # 例如: 直接提取数字
            num_match = re.search(r'(\d+)', file_name)
            if num_match:
                query_num = num_match.group(1)
                query_name = f"Q{query_num}"
            else:
                # 如果还是没有找到数字，则使用文件名（去掉可能的后缀如"_run"）
                clean_name = re.sub(r'_run$|_test$|_query$', '', file_name, flags=re.IGNORECASE)
                query_name = clean_name.upper()
        
        # 检查是否包含成功信息
        success_patterns = [
            f"Query {query_num} finished successfully",
            f"query {query_num} finished successfully",
            f"Query {query_name} finished successfully",
            "SUCCESS",
            "success",
            "finished successfully",
            "execution succeeded"
        ]
        
        content_upper = content.upper()
        for pattern in success_patterns:
            if pattern.upper() in content_upper:
                success = True
                break
        
        # 如果通过文件名无法确定查询编号，尝试从内容中提取
        if not success:
            # 从内容中查找查询编号
            content_query_match = re.search(r'Query\s+(\d+)\s+finished', content, re.IGNORECASE)
            if content_query_match:
                content_query_num = content_query_match.group(1)
                if not query_name.startswith('Q'):
                    query_name = f"Q{content_query_num}"
                # 再次检查成功信息
                for pattern in success_patterns:
                    if pattern.upper() in content_upper:
                        success = True
                        break
        
        if success:
            # 查找时间结果
            # 匹配 patterns like: duration: 0:00:00.090073
            time_patterns = [
                r'duration:\s*([\d:.]+)',  # duration: 0:00:00.090073
                r'time:\s*([\d:.]+)',      # time: 0:00:00.090073
                r'-\s*([\d:.]+)\)',        # - 0:00:00.090073)
                r'\(([\d:.]+)\)',          # (0:00:00.090073)
                r'execution time:\s*([\d:.]+)',  # execution time: 0:00:00.090073
                r'took\s*([\d:.]+)',       # took 0:00:00.090073
                r'elapsed:\s*([\d:.]+)',   # elapsed: 0:00:00.090073
                r'query_time=\s*([\d:.]+)', # query_time=0:00:00.090073
                r'execution_time=\s*([\d:.]+)', # execution_time=0:00:00.090073
            ]
            
            for pattern in time_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    time_result = match.group(1)
                    break
            
            # 如果没找到，尝试在results行中查找
            if not time_result:
                # 查找results=行
                results_match = re.search(r'results=\[\(.*?([\d:.]+)\)\]', content)
                if results_match:
                    time_result = results_match.group(1)
            
            # 如果还是没找到，尝试查找任何看起来像时间的模式
            if not time_result:
                time_match = re.search(r'(\d+:\d+:\d+\.\d+)', content)
                if time_match:
                    time_result = time_match.group(1)
        
        return query_name, time_result, success
        
    except Exception as e:
        print(f"读取文件 {file_path} 时出错: {e}")
        return query_name, None, False


def convert_duration_to_seconds(duration_str: str) -> float:
    """
    将时间字符串转换为秒数
    
    Args:
        duration_str: 时间字符串，如 "0:00:00.090073"
        
    Returns:
        float: 秒数
    """
    if not duration_str or duration_str in ["FAILED", "N/A", "NOT_FOUND"]:
        return None
    
    try:
        # 处理格式: 0:00:00.090073
        parts = duration_str.split(":")
        if len(parts) == 3:  # 时:分:秒.毫秒
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds_parts = parts[2].split(".")
            seconds = int(seconds_parts[0])
            milliseconds = float(f"0.{seconds_parts[1]}") if len(seconds_parts) > 1 else 0
            
            total_seconds = hours * 3600 + minutes * 60 + seconds + milliseconds
            return total_seconds
        elif len(parts) == 2:  # 分:秒.毫秒
            minutes = int(parts[0])
            seconds_parts = parts[1].split(".")
            seconds = int(seconds_parts[0])
            milliseconds = float(f"0.{seconds_parts[1]}") if len(seconds_parts) > 1 else 0
            
            total_seconds = minutes * 60 + seconds + milliseconds
            return total_seconds
        else:  # 只有秒.毫秒
            return float(duration_str)
    except:
        return None


def extract_results_from_run_folder(run_folder: Path, log_type: str = "perf_logs") -> Dict[str, Dict[str, str]]:
    """
    从单个轮次文件夹中提取所有log文件的结果
    
    Args:
        run_folder: 轮次文件夹路径（如 run_001）
        log_type: log类型，可选 "perf_logs" 或 "noperf_logs"
        
    Returns:
        dict: 查询结果字典，格式为 {query_name: {"duration": time_str, "status": status_str}}
    """
    results = {}
    run_name = run_folder.name
    
    # 构建log文件夹路径
    log_folder = run_folder / "multi_core" / log_type
    
    if not log_folder.exists():
        print(f"警告: {run_name} 中没有 {log_type} 文件夹")
        return results
    
    print(f"\n处理轮次: {run_name}, log类型: {log_type}")
    print(f"log文件夹: {log_folder}")
    
    # 查找所有log文件
    log_files = list(log_folder.glob("*.log")) + list(log_folder.glob("*.txt"))
    
    if not log_files:
        print(f"  未找到log文件")
        return results
    
    print(f"  找到 {len(log_files)} 个log文件")
    
    for file_path in log_files:
        query_name, time_result, success = extract_query_results_from_file(file_path)
        
        if query_name:
            if query_name in results:
                # 如果已经存在该查询，跳过或更新（这里选择更新为最新的）
                pass
            
            if success:
                if time_result:
                    results[query_name] = {
                        "duration": time_result,
                        "status": "SUCCESS",
                        "duration_seconds": convert_duration_to_seconds(time_result)
                    }
                else:
                    results[query_name] = {
                        "duration": "NOT_FOUND",
                        "status": "SUCCESS (但未找到时间)",
                        "duration_seconds": None
                    }
            else:
                results[query_name] = {
                    "duration": "FAILED",
                    "status": "FAILURE",
                    "duration_seconds": None
                }
            print(f"    {file_path.name}: {query_name} - {results[query_name]['status']} - {results[query_name]['duration']}")
    
    return results


def process_multiple_runs(root_folder: str, log_type: str = "perf_logs") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    处理多个轮次的log数据
    
    Args:
        root_folder: 根文件夹路径
        log_type: log类型，可选 "perf_logs" 或 "noperf_logs"
        
    Returns:
        tuple: (时间DataFrame, 状态DataFrame)
    """
    root_path = Path(root_folder)
    
    if not root_path.exists():
        print(f"错误: 根文件夹 '{root_folder}' 不存在!")
        return pd.DataFrame(), pd.DataFrame()
    
    # 查找所有轮次文件夹
    run_folders = sorted([d for d in root_path.iterdir() if d.is_dir() and d.name.startswith("run_")])
    
    if not run_folders:
        print(f"错误: 在 '{root_folder}' 中未找到任何 run_* 文件夹!")
        return pd.DataFrame(), pd.DataFrame()
    
    print(f"找到 {len(run_folders)} 个轮次文件夹:")
    for run_folder in run_folders:
        print(f"  - {run_folder.name}")
    
    # 收集所有轮次的数据
    all_results = {}
    all_queries = set()
    
    for run_folder in run_folders:
        run_name = run_folder.name
        run_results = extract_results_from_run_folder(run_folder, log_type)
        
        all_results[run_name] = run_results
        
        # 收集所有查询名称
        all_queries.update(run_results.keys())
    
    if not all_queries:
        print("没有找到任何查询数据!")
        return pd.DataFrame(), pd.DataFrame()
    
    # 创建DataFrame
    # 按查询编号排序
    def extract_query_number(query_str: str) -> int:
        match = re.search(r'Q?(\d+)', query_str, re.IGNORECASE)
        return int(match.group(1)) if match else float('inf')
    
    sorted_queries = sorted(all_queries, key=extract_query_number)
    
    # 创建时间DataFrame
    time_data = []
    for query in sorted_queries:
        row = {"Query": query}
        for run_name in sorted(all_results.keys()):
            if query in all_results[run_name]:
                row[run_name] = all_results[run_name][query]["duration"]
            else:
                row[run_name] = "NOT_FOUND"
        time_data.append(row)
    
    time_df = pd.DataFrame(time_data)
    
    # 创建状态DataFrame
    status_data = []
    for query in sorted_queries:
        row = {"Query": query}
        for run_name in sorted(all_results.keys()):
            if query in all_results[run_name]:
                row[run_name] = all_results[run_name][query]["status"]
            else:
                row[run_name] = "NOT_FOUND"
        status_data.append(row)
    
    status_df = pd.DataFrame(status_data)
    
    # 创建秒数DataFrame
    seconds_data = []
    for query in sorted_queries:
        row = {"Query": query}
        for run_name in sorted(all_results.keys()):
            if query in all_results[run_name]:
                row[run_name] = all_results[run_name][query]["duration_seconds"]
            else:
                row[run_name] = None
        seconds_data.append(row)
    
    seconds_df = pd.DataFrame(seconds_data)
    
    return time_df, status_df, seconds_df


def export_to_excel(time_df: pd.DataFrame, status_df: pd.DataFrame, seconds_df: pd.DataFrame, 
                   output_path: str, log_type: str = "perf_logs"):
    """
    将结果保存到Excel文件
    
    Args:
        time_df: 时间DataFrame
        status_df: 状态DataFrame
        seconds_df: 秒数DataFrame
        output_path: 输出Excel文件路径
        log_type: log类型
    """
    if time_df.empty:
        print("没有数据可保存")
        return
    
    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # 保存时间数据表
            time_df.to_excel(writer, sheet_name=f"{log_type}_时间", index=False)
            
            # 保存状态数据表
            status_df.to_excel(writer, sheet_name=f"{log_type}_状态", index=False)
            
            # 保存秒数数据表
            seconds_df.to_excel(writer, sheet_name=f"{log_type}_秒数", index=False)
            
            # 创建统计表
            stats_data = {
                "统计项": ["总查询数", "总轮次数", "log类型"],
                "值": [len(time_df), len(time_df.columns) - 1, log_type]
            }
            
            # 统计每个轮次的成功率
            stats_data["统计项"].append("各轮次成功率:")
            
            for run_name in time_df.columns[1:]:  # 跳过第一列的"Query"
                total_queries = len(time_df)
                success_count = len([1 for query in time_df[run_name] if query not in ["FAILED", "NOT_FOUND", "N/A"] and query != ""])
                success_rate = success_count / total_queries * 100 if total_queries > 0 else 0
                stats_data["统计项"].append(f"{run_name} 成功率")
                stats_data["值"].append(f"{success_rate:.2f}%")
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name=f"{log_type}_统计", index=False)
            
            # 调整列宽
            for sheet_name in writer.sheets:
                worksheet = writer.sheets[sheet_name]
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\n结果已保存到: {output_path}")
        print(f"包含的工作表:")
        print(f"  1. {log_type}_时间: 原始时间数据")
        print(f"  2. {log_type}_状态: 查询状态")
        print(f"  3. {log_type}_秒数: 转换为秒数的数据")
        print(f"  4. {log_type}_统计: 统计信息")
        
    except Exception as e:
        print(f"保存Excel文件时出错: {e}")


def print_summary(time_df: pd.DataFrame, status_df: pd.DataFrame, log_type: str = "perf_logs"):
    """
    打印汇总信息
    
    Args:
        time_df: 时间DataFrame
        status_df: 状态DataFrame
        log_type: log类型
    """
    if time_df.empty:
        return
    
    print(f"\n{'='*80}")
    print(f"{log_type.upper()} 数据汇总")
    print(f"{'='*80}")
    
    print(f"\n轮次: {', '.join(time_df.columns[1:])}")
    print(f"查询: {', '.join(time_df['Query'].tolist()[:10])}{'...' if len(time_df) > 10 else ''}")
    print(f"总查询数: {len(time_df)}")
    print(f"总轮次数: {len(time_df.columns) - 1}")
    
    # 打印前几个查询的数据
    print(f"\n前5个查询的时间数据:")
    print(time_df.head())
    
    # 统计每个轮次的成功率
    print(f"\n各轮次成功率:")
    for run_name in time_df.columns[1:]:  # 跳过第一列的"Query"
        total_queries = len(time_df)
        success_count = len([1 for query in time_df[run_name] if query not in ["FAILED", "NOT_FOUND", "N/A"] and query != ""])
        success_rate = success_count / total_queries * 100 if total_queries > 0 else 0
        print(f"  {run_name}: {success_count}/{total_queries} ({success_rate:.2f}%)")


def main():
    parser = argparse.ArgumentParser(description="从多个轮次的TPC-DS测试结果中提取时间结果并保存到Excel")
    parser.add_argument("root_folder", help="包含多个run_*文件夹的根文件夹路径")
    parser.add_argument("-o", "--output", default="query_results.xlsx", 
                       help="输出Excel文件路径 (默认: query_results.xlsx)")
    parser.add_argument("--perf-only", action="store_true",
                       help="只处理perf_logs")
    parser.add_argument("--noperf-only", action="store_true",
                       help="只处理noperf_logs")
    
    args = parser.parse_args()
    
    # 检查文件夹是否存在
    if not os.path.exists(args.root_folder):
        print(f"错误: 文件夹 '{args.root_folder}' 不存在!")
        sys.exit(1)
    
    print(f"正在处理根文件夹: {args.root_folder}")
    
    # 确定要处理的log类型
    log_types = []
    
    if args.perf_only:
        log_types.append("perf_logs")
    elif args.noperf_only:
        log_types.append("noperf_logs")
    else:
        log_types = ["perf_logs", "noperf_logs"]
    
    # 处理每个log类型
    for log_type in log_types:
        print(f"\n{'='*80}")
        print(f"处理 {log_type} 数据")
        print(f"{'='*80}")
        
        # 处理多个轮次
        time_df, status_df, seconds_df = process_multiple_runs(args.root_folder, log_type)
        
        if time_df.empty:
            print(f"没有找到 {log_type} 的数据")
            continue
        
        # 打印汇总
        print_summary(time_df, status_df, log_type)
        
        # 保存结果
        if len(log_types) == 1:
            output_path = args.output
        else:
            # 为多个log类型创建不同的文件名
            base_name = os.path.splitext(args.output)[0]
            ext = os.path.splitext(args.output)[1]
            output_path = f"{base_name}_{log_type}{ext}"
        
        export_to_excel(time_df, status_df, seconds_df, output_path, log_type)


if __name__ == "__main__":
    main()