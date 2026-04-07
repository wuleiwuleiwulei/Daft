import os
import re
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import argparse


class TpchResultsCollector:
    def __init__(self, base_folder: str):
        """
        初始化收集器
        
        Args:
            base_folder: 基准文件夹路径，如 "tpch_benchmark_runs_0114_01_920B"
        """
        self.base_folder = Path(base_folder)
        self.results = {}  # 存储所有结果
        self.run_folders = []  # 存储所有轮次文件夹
        self.configs = ['no_cpus_arg', 'num_cpus_1', 'num_cpus_40']  # 配置类型
        self.result_types = ['noperf_results', 'perf_results']  # 结果类型
        self.case_range = range(1, 23)  # case编号范围 1-22
        
    def find_run_folders(self) -> List[str]:
        """
        查找所有轮次文件夹
        
        Returns:
            轮次文件夹名称列表
        """
        run_folders = []
        
        if not self.base_folder.exists():
            print(f"错误: 基准文件夹 '{self.base_folder}' 不存在!")
            return run_folders
        
        # 查找所有run_开头的文件夹
        for item in self.base_folder.iterdir():
            if item.is_dir() and item.name.startswith('run_'):
                run_folders.append(item.name)
        
        # 按数字排序
        run_folders.sort(key=lambda x: int(x.split('_')[1]) if x.split('_')[1].isdigit() else float('inf'))
        
        print(f"找到 {len(run_folders)} 个轮次文件夹: {run_folders}")
        self.run_folders = run_folders
        return run_folders
    
    def extract_time_from_csv(self, csv_file: Path) -> Optional[float]:
        """
        从CSV文件中提取时间数据
        
        Args:
            csv_file: CSV文件路径
            
        Returns:
            时间值（浮点数），如果提取失败则返回None
        """
        try:
            # 读取CSV文件
            df = pd.read_csv(csv_file, header=None)
            
            # 第一行是case名，第二行是时间数据
            if len(df) >= 2:
                # 假设时间在第二行的第一列（或某个位置）
                # 我们可以尝试找到包含数字的列
                for col in df.columns:
                    if df[col].dtype in [np.float64, np.int64] or (df[col].dtype == object and 
                                                                   df.iloc[1][col] is not None and 
                                                                   isinstance(df.iloc[1][col], str) and
                                                                   any(c.isdigit() for c in str(df.iloc[1][col]))):
                        try:
                            # 尝试转换为浮点数
                            time_value = float(df.iloc[1][col])
                            return time_value
                        except (ValueError, TypeError):
                            continue
                
                # 如果上面的方法失败，尝试直接取第二行的值
                for val in df.iloc[1]:
                    if isinstance(val, (int, float)):
                        return float(val)
                    elif isinstance(val, str):
                        # 尝试从字符串中提取数字
                        match = re.search(r'(\d+\.?\d*)', val)
                        if match:
                            return float(match.group(1))
            
            print(f"警告: 文件 {csv_file.name} 中未找到有效的时间数据")
            return None
            
        except Exception as e:
            print(f"读取文件 {csv_file.name} 时出错: {e}")
            return None
    
    def collect_results_for_config(self, run_name: str, config: str, result_type: str) -> Dict[int, float]:
        """
        收集指定轮次、配置和结果类型的所有case结果
        
        Args:
            run_name: 轮次名称，如 "run_001"
            config: 配置类型，如 "no_cpus_arg"
            result_type: 结果类型，如 "noperf_results"
            
        Returns:
            字典，key为case编号，value为时间值
        """
        case_results = {}
        
        # 构造路径
        config_path = self.base_folder / run_name / config / result_type
        
        if not config_path.exists():
            print(f"警告: 路径不存在: {config_path}")
            return case_results
        
        # 遍历所有case文件
        for case_num in self.case_range:
            # 构造文件名
            filename = f"tpch_results{case_num}.csv"
            file_path = config_path / filename
            
            if file_path.exists():
                time_value = self.extract_time_from_csv(file_path)
                if time_value is not None:
                    case_results[case_num] = time_value
                else:
                    case_results[case_num] = np.nan  # 使用NaN表示缺失值
            else:
                case_results[case_num] = np.nan
                # print(f"警告: 文件不存在: {file_path}")
        
        return case_results
    
    def collect_all_results(self):
        """
        收集所有轮次、所有配置、所有结果类型的数据
        """
        print("开始收集所有结果...")
        
        # 首先查找所有轮次文件夹
        self.find_run_folders()
        
        if not self.run_folders:
            print("没有找到任何轮次文件夹!")
            return
        
        # 初始化数据结构
        for config in self.configs:
            for result_type in self.result_types:
                key = f"{config}_{result_type}"
                self.results[key] = {}
        
        # 遍历每个轮次
        for run_name in self.run_folders:
            print(f"\n处理轮次: {run_name}")
            
            # 遍历每种配置和结果类型
            for config in self.configs:
                for result_type in self.result_types:
                    key = f"{config}_{result_type}"
                    print(f"  收集 {config}/{result_type}...")
                    
                    # 收集该配置和结果类型的所有case结果
                    case_results = self.collect_results_for_config(run_name, config, result_type)
                    self.results[key][run_name] = case_results
        
        print(f"\n收集完成! 共收集 {len(self.run_folders)} 个轮次的数据")
    
    def export_to_excel(self, output_file: str = "tpch_benchmark_results.xlsx"):
        """
        导出结果到Excel文件
        
        Args:
            output_file: 输出文件名
        """
        if not self.results:
            print("没有数据可导出")
            return
        
        print(f"\n导出结果到: {output_file}")
        
        # 创建Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 1. 为每种配置和结果类型创建工作表
            for config in self.configs:
                for result_type in self.result_types:
                    key = f"{config}_{result_type}"
                    
                    if key in self.results and self.results[key]:
                        # 创建DataFrame
                        data = []
                        for run_name in self.run_folders:
                            if run_name in self.results[key]:
                                row = {'Run': run_name}
                                # 添加每个case的数据
                                for case_num in self.case_range:
                                    if case_num in self.results[key][run_name]:
                                        row[f'Case_{case_num}'] = self.results[key][run_name][case_num]
                                    else:
                                        row[f'Case_{case_num}'] = np.nan
                                data.append(row)
                        
                        df = pd.DataFrame(data)
                        
                        # 设置Run列为索引
                        df.set_index('Run', inplace=True)
                        
                        # 工作表名称（不能超过31个字符）
                        sheet_name = f"{config}_{result_type}"
                        if len(sheet_name) > 31:
                            sheet_name = sheet_name[:31]
                        
                        # 写入工作表
                        df.to_excel(writer, sheet_name=sheet_name)
                        print(f"  创建工作表: {sheet_name} ({len(df)} 行 × {len(df.columns)} 列)")
            
            # 2. 创建汇总工作表（所有配置的平均值）
            self.create_summary_sheet(writer)
            
            # 3. 创建数据统计工作表
            self.create_statistics_sheet(writer)
            
            # 4. 调整所有工作表的列宽
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
                    adjusted_width = min(max_length + 2, 30)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\nExcel文件已生成: {output_file}")
        
        # 打印简要统计
        self.print_summary()
    
    def create_summary_sheet(self, writer):
        """
        创建汇总工作表，显示每种配置的平均时间
        
        Args:
            writer: Excel writer对象
        """
        summary_data = []
        
        # 收集所有配置的平均时间
        for config in self.configs:
            for result_type in self.result_types:
                key = f"{config}_{result_type}"
                
                if key in self.results and self.results[key]:
                    # 计算每个case在所有轮次中的平均值
                    case_means = {}
                    for case_num in self.case_range:
                        values = []
                        for run_name in self.run_folders:
                            if (run_name in self.results[key] and 
                                case_num in self.results[key][run_name] and
                                not np.isnan(self.results[key][run_name][case_num])):
                                values.append(self.results[key][run_name][case_num])
                        
                        if values:
                            case_means[f'Case_{case_num}'] = np.mean(values)
                        else:
                            case_means[f'Case_{case_num}'] = np.nan
                    
                    # 添加行数据
                    row = {'Config_ResultType': key}
                    row.update(case_means)
                    summary_data.append(row)
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.set_index('Config_ResultType', inplace=True)
            summary_df.to_excel(writer, sheet_name='Summary_Means')
            print(f"  创建工作表: Summary_Means")
    
    def create_statistics_sheet(self, writer):
        """
        创建数据统计工作表
        
        Args:
            writer: Excel writer对象
        """
        stats_data = []
        
        for config in self.configs:
            for result_type in self.result_types:
                key = f"{config}_{result_type}"
                
                if key in self.results and self.results[key]:
                    # 统计信息
                    total_cases = 0
                    valid_cases = 0
                    all_values = []
                    
                    for run_name in self.run_folders:
                        if run_name in self.results[key]:
                            for case_num in self.case_range:
                                if case_num in self.results[key][run_name]:
                                    total_cases += 1
                                    value = self.results[key][run_name][case_num]
                                    if not np.isnan(value):
                                        valid_cases += 1
                                        all_values.append(value)
                    
                    if all_values:
                        min_val = np.min(all_values)
                        max_val = np.max(all_values)
                        mean_val = np.mean(all_values)
                        median_val = np.median(all_values)
                        std_val = np.std(all_values)
                    else:
                        min_val = max_val = mean_val = median_val = std_val = np.nan
                    
                    stats_data.append({
                        'Config_ResultType': key,
                        'Total_Runs': len([r for r in self.run_folders if r in self.results[key]]),
                        'Total_Cases': total_cases,
                        'Valid_Cases': valid_cases,
                        'Missing_Ratio': f"{(total_cases - valid_cases)/total_cases*100:.1f}%" if total_cases > 0 else "N/A",
                        'Min_Time': min_val,
                        'Max_Time': max_val,
                        'Mean_Time': mean_val,
                        'Median_Time': median_val,
                        'Std_Dev': std_val
                    })
        
        if stats_data:
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='Statistics', index=False)
            print(f"  创建工作表: Statistics")
    
    def print_summary(self):
        """
        打印收集结果摘要
        """
        if not self.results:
            print("没有收集到任何结果")
            return
        
        print("\n" + "="*80)
        print("TPC-H基准测试结果收集摘要")
        print("="*80)
        
        print(f"\n轮次数量: {len(self.run_folders)}")
        print(f"配置类型: {len(self.configs)} 种")
        print(f"结果类型: {len(self.result_types)} 种")
        print(f"总数据表: {len(self.configs) * len(self.result_types)} 个")
        
        print(f"\n各配置的结果统计:")
        print("-"*80)
        
        for config in self.configs:
            for result_type in self.result_types:
                key = f"{config}_{result_type}"
                
                if key in self.results and self.results[key]:
                    total_values = 0
                    valid_values = 0
                    
                    for run_name in self.results[key]:
                        for case_num in self.results[key][run_name]:
                            total_values += 1
                            if not np.isnan(self.results[key][run_name][case_num]):
                                valid_values += 1
                    
                    missing_ratio = (total_values - valid_values) / total_values * 100 if total_values > 0 else 0
                    
                    print(f"{key:30s}: {valid_values:3d}/{total_values:3d} 有效值 ({missing_ratio:5.1f}% 缺失)")
        
        print("\n" + "="*80)
    
    def run_collection(self, output_file: str = "tpch_benchmark_results.xlsx"):
        """
        运行完整收集流程
        
        Args:
            output_file: 输出文件名
        """
        print(f"开始收集TPC-H基准测试结果...")
        print(f"基准文件夹: {self.base_folder}")
        
        self.collect_all_results()
        self.export_to_excel(output_file)


def main():
    parser = argparse.ArgumentParser(description='收集TPC-H基准测试结果并导出到Excel')
    parser.add_argument('base_folder', help='基准文件夹路径（如tpch_benchmark_runs_0114_01_920B）')
    parser.add_argument('--output', '-o', default='tpch_benchmark_results.xlsx', 
                       help='输出Excel文件名称（默认: tpch_benchmark_results.xlsx）')
    
    args = parser.parse_args()
    
    collector = TpchResultsCollector(base_folder=args.base_folder)
    collector.run_collection(output_file=args.output)


if __name__ == "__main__":
    main()