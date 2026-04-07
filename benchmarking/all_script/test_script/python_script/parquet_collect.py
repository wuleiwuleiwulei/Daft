import os
import json
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import argparse
import sys

class BenchmarkDataExtractor:
    def __init__(self, base_path: str):
        """
        初始化数据提取器
        
        Args:
            base_path: 基础路径，包含run_xxx文件夹
        """
        self.base_path = Path(base_path)
        # 修改数据结构：folder_name -> test_name -> run_id -> mean_time
        self.all_data = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))  
        self.folder_names = set()
        self.test_names = set()
        self.run_ids = set()
        
    def find_all_runs(self):
        """
        查找所有run_xxx文件夹
        
        Returns:
            run文件夹路径列表
        """
        runs = []
        for item in self.base_path.iterdir():
            if item.is_dir() and item.name.startswith("run_"):
                runs.append(item)
        
        # 按数字排序
        runs.sort(key=lambda x: int(x.name.split("_")[1]) if len(x.name.split("_")) > 1 else 0)
        
        print(f"找到 {len(runs)} 个run文件夹: {[r.name for r in runs]}")
        return runs
    
    def extract_from_json_file(self, json_file: Path, run_id: str, folder_key: str):
        """
        从单个JSON文件中提取测试数据
        
        Args:
            json_file: JSON文件路径
            run_id: 运行ID（run_001等）
            folder_key: 文件夹标识（用于区分不同的搜索文件夹）
        """
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查是否有benchmarks字段
            if "benchmarks" not in data:
                print(f"警告: 文件 {json_file} 中没有'benchmarks'字段")
                return
            
            for benchmark in data["benchmarks"]:
                # 提取测试名称和平均耗时
                test_name = benchmark.get("name", "")
                mean_time = benchmark.get("stats", {}).get("mean", None)
                
                if test_name and mean_time is not None:
                    # 添加到数据集中，按folder_key分组
                    self.all_data[folder_key][test_name][run_id] = mean_time
                    self.folder_names.add(folder_key)
                    self.test_names.add(test_name)
                    self.run_ids.add(run_id)
        
        except json.JSONDecodeError as e:
            print(f"错误: 无法解析JSON文件 {json_file}: {e}")
        except Exception as e:
            print(f"错误: 处理文件 {json_file} 时出错: {e}")
    
    def get_folder_key(self, folder_path: Path, run_folder: Path) -> str:
        """
        获取文件夹的唯一标识
        
        Args:
            folder_path: 文件夹路径
            run_folder: run文件夹路径
            
        Returns:
            文件夹标识字符串
        """
        # 计算相对于run_folder的相对路径
        relative_path = folder_path.relative_to(run_folder)
        # 将路径转换为字符串，用'_'替换路径分隔符
        folder_key = str(relative_path).replace(os.sep, '_')
        return folder_key
    
    def process_run_folder(self, run_folder: Path):
        """
        处理单个run文件夹
        
        Args:
            run_folder: run文件夹路径
        """
        run_id = run_folder.name
        print(f"\n处理 {run_id}...")
        
        # 定义要搜索的文件夹
        search_folders = [
            run_folder / "no_binding" / "noperf_results",
            run_folder / "no_binding" / "perf_results",
            run_folder / "multi_core" / "noperf_results",  # 备用路径
            run_folder / "multi_core" / "perf_results"     # 备用路径
        ]
        
        found_files = False
        
        for folder in search_folders:
            if folder.exists() and folder.is_dir():
                print(f"  搜索文件夹: {folder.relative_to(self.base_path)}")
                
                # 获取文件夹标识
                folder_key = self.get_folder_key(folder, run_folder)
                
                # 查找所有JSON文件
                json_files = list(folder.glob("*.json"))
                
                if json_files:
                    found_files = True
                    print(f"    找到 {len(json_files)} 个JSON文件")
                    
                    for json_file in json_files:
                        self.extract_from_json_file(json_file, run_id, folder_key)
                else:
                    print(f"    该文件夹中没有JSON文件")
        
        if not found_files:
            print(f"  警告: {run_id} 中没有找到JSON文件")
    
    def process_all_runs(self):
        """
        处理所有run文件夹
        """
        print("=" * 60)
        print("开始提取测试数据")
        print("=" * 60)
        
        # 查找所有run文件夹
        runs = self.find_all_runs()
        
        if not runs:
            print(f"错误: 在 {self.base_path} 中没有找到run_xxx文件夹")
            return False
        
        # 处理每个run文件夹
        for run_folder in runs:
            self.process_run_folder(run_folder)
        
        print(f"\n数据处理完成!")
        print(f"共处理 {len(self.folder_names)} 个不同的文件夹")
        print(f"共提取 {len(self.test_names)} 个不同的测试")
        print(f"共处理 {len(self.run_ids)} 个运行轮次")
        
        # 打印文件夹统计
        print(f"\n文件夹统计:")
        for folder_key in sorted(self.folder_names):
            test_count = len(self.all_data[folder_key])
            print(f"  {folder_key}: {test_count} 个测试")
        
        return True
    
    def create_dataframe_for_folder(self, folder_key: str):
        """
        为特定文件夹创建DataFrame
        
        Args:
            folder_key: 文件夹标识
            
        Returns:
            pandas DataFrame
        """
        if folder_key not in self.all_data or not self.all_data[folder_key]:
            print(f"警告: 文件夹 {folder_key} 没有数据")
            return None
        
        # 获取该文件夹下的数据
        folder_data = self.all_data[folder_key]
        
        # 将run_ids排序
        sorted_run_ids = sorted(list(self.run_ids), 
                                key=lambda x: int(x.split("_")[1]) if len(x.split("_")) > 1 else 0)
        
        # 获取该文件夹下的测试名称并排序
        folder_test_names = sorted(list(folder_data.keys()))
        
        # 创建空的DataFrame
        df = pd.DataFrame(index=folder_test_names, columns=sorted_run_ids)
        
        # 填充数据
        for test_name in folder_test_names:
            for run_id in sorted_run_ids:
                if run_id in folder_data[test_name]:
                    df.at[test_name, run_id] = folder_data[test_name][run_id]
                else:
                    df.at[test_name, run_id] = np.nan  # 使用NaN表示缺失数据
        
        return df
    
    def clean_sheet_name(self, name: str, max_length: int = 31) -> str:
        """
        清理工作表名称，确保符合Excel要求
        
        Args:
            name: 原始名称
            max_length: 最大长度（Excel限制为31）
            
        Returns:
            清理后的名称
        """
        if not name:
            return "Unknown"
        
        # 移除或替换Excel不允许的字符
        invalid_chars = r'[]:*?/\\'
        for char in invalid_chars:
            name = name.replace(char, '_')
        
        # 限制长度
        if len(name) > max_length:
            name = name[:max_length-3] + '...'
        
        return name
    
    def export_to_excel(self, output_file: str = "benchmark_results.xlsx"):
        """
        导出结果到Excel文件
        
        Args:
            output_file: 输出Excel文件路径
        """
        if not self.all_data:
            print("没有数据可导出")
            return
        
        print(f"\n创建DataFrame...")
        
        try:
            # 创建Excel写入器
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                # 为每个文件夹创建一个工作表
                folder_count = 0
                
                for folder_key in sorted(self.folder_names):
                    # 清理工作表名称
                    sheet_name = self.clean_sheet_name(folder_key)
                    
                    # 为该文件夹创建DataFrame
                    df = self.create_dataframe_for_folder(folder_key)
                    
                    if df is not None:
                        print(f"  创建工作表: {sheet_name} ({df.shape[0]}行, {df.shape[1]}列)")
                        
                        # 写入Excel工作表
                        df.to_excel(writer, sheet_name=sheet_name, index_label='Query')
                        
                        # 创建该文件夹的汇总信息
                        self.create_folder_summary_sheet(writer, df, folder_key, sheet_name)
                        
                        folder_count += 1
                
                # 创建全局汇总工作表
                if folder_count > 0:
                    self.create_global_summary_sheet(writer)
                
                # 调整所有工作表的列宽
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
                        adjusted_width = min(max_length + 2, 100)  # 设置最大宽度为100
                        worksheet.column_dimensions[column_letter].width = adjusted_width
            
            print(f"\n结果已导出到: {output_file}")
            print(f"共创建 {folder_count} 个文件夹工作表")
            
            # 打印一些统计信息
            self.print_summary()
            
        except Exception as e:
            print(f"导出Excel文件时出错: {e}")
            import traceback
            traceback.print_exc()
    
    def create_folder_summary_sheet(self, writer, df, folder_key: str, sheet_name: str):
        """
        为单个文件夹创建汇总统计表
        
        Args:
            writer: Excel写入器
            df: 文件夹的DataFrame
            folder_key: 文件夹标识
            sheet_name: 工作表名称
        """
        # 创建汇总工作表名称
        summary_sheet_name = f"{sheet_name}_汇总"[:31]  # 限制长度
        
        # 计算基本统计
        total_tests = df.shape[0]
        total_runs = df.shape[1]
        total_data_points = df.size
        valid_data_points = df.count().sum()
        missing_data_points = df.isna().sum().sum()
        missing_rate = missing_data_points / total_data_points * 100 if total_data_points > 0 else 0
        
        # 创建汇总数据
        summary_data = {
            '统计项': [
                '文件夹名称',
                '总测试数量', 
                '总轮次数', 
                '总数据点', 
                '有效数据点', 
                '缺失数据点', 
                '缺失率',
                '平均耗时(所有测试)',
                '最小耗时(所有测试)',
                '最大耗时(所有测试)'
            ],
            '数值': [
                folder_key,
                total_tests,
                total_runs,
                total_data_points,
                valid_data_points,
                missing_data_points,
                f"{missing_rate:.2f}%",
                f"{df.mean().mean():.4f}" if valid_data_points > 0 else "N/A",
                f"{df.min().min():.4f}" if valid_data_points > 0 else "N/A",
                f"{df.max().max():.4f}" if valid_data_points > 0 else "N/A"
            ]
        }
        
        summary_df = pd.DataFrame(summary_data)
        
        try:
            summary_df.to_excel(writer, sheet_name=summary_sheet_name, index=False)
        except:
            # 如果工作表名称重复，添加后缀
            try:
                summary_sheet_name = f"{sheet_name}_统计"[:31]
                summary_df.to_excel(writer, sheet_name=summary_sheet_name, index=False)
            except:
                # 如果还是失败，跳过这个工作表
                pass
    
    def create_global_summary_sheet(self, writer):
        """
        创建全局汇总工作表
        
        Args:
            writer: Excel写入器
        """
        summary_data = []
        
        for folder_key in sorted(self.folder_names):
            df = self.create_dataframe_for_folder(folder_key)
            if df is not None:
                total_tests = df.shape[0]
                total_runs = df.shape[1]
                valid_data_points = df.count().sum()
                missing_data_points = df.isna().sum().sum()
                avg_time = df.mean().mean() if valid_data_points > 0 else np.nan
                min_time = df.min().min() if valid_data_points > 0 else np.nan
                max_time = df.max().max() if valid_data_points > 0 else np.nan
                
                summary_data.append({
                    '文件夹': folder_key,
                    '测试数量': total_tests,
                    '轮次数量': total_runs,
                    '有效数据点': valid_data_points,
                    '缺失数据点': missing_data_points,
                    '平均耗时': f"{avg_time:.4f}" if not pd.isna(avg_time) else "N/A",
                    '最小耗时': f"{min_time:.4f}" if not pd.isna(min_time) else "N/A",
                    '最大耗时': f"{max_time:.4f}" if not pd.isna(max_time) else "N/A"
                })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='全局汇总', index=False)
    
    def print_summary(self):
        """
        打印结果摘要
        """
        print("\n" + "="*80)
        print("测试结果摘要")
        print("="*80)
        
        print(f"\n总文件夹数量: {len(self.folder_names)}")
        print(f"总测试数量: {len(self.test_names)}")
        print(f"总轮次数量: {len(self.run_ids)}")
        
        # 显示每个文件夹的统计
        print(f"\n各文件夹统计:")
        for folder_key in sorted(self.folder_names):
            df = self.create_dataframe_for_folder(folder_key)
            if df is not None:
                test_count = df.shape[0]
                run_count = df.shape[1]
                valid_count = df.count().sum()
                print(f"  {folder_key}: {test_count}个测试, {run_count}个轮次, {valid_count}个有效数据点")
        
        # 显示每个轮次的数据统计
        print(f"\n各轮次统计:")
        for run_id in sorted(self.run_ids, key=lambda x: int(x.split("_")[1]) if len(x.split("_")) > 1 else 0):
            total_tests = 0
            for folder_key in self.folder_names:
                df = self.create_dataframe_for_folder(folder_key)
                if df is not None and run_id in df.columns:
                    test_count = df[run_id].count()
                    total_tests += test_count
            print(f"  {run_id}: {total_tests} 个测试")
    
    def run_extraction(self, output_file: str = "benchmark_results.xlsx"):
        """
        运行完整的数据提取流程
        
        Args:
            output_file: 输出Excel文件路径
        """
        print(f"基准测试数据提取器")
        print(f"基础路径: {self.base_path}")
        print(f"输出文件: {output_file}")
        print("=" * 60)
        
        # 处理所有run文件夹
        success = self.process_all_runs()
        
        if not success:
            return
        
        # 导出到Excel
        self.export_to_excel(output_file)


def main():
    parser = argparse.ArgumentParser(description='从多个run文件夹中提取benchmark测试数据并生成Excel报表')
    parser.add_argument('base_path', help='包含run_xxx文件夹的基础路径')
    parser.add_argument('-o', '--output', default='benchmark_results.xlsx', 
                       help='输出Excel文件路径 (默认: benchmark_results.xlsx)')
    
    args = parser.parse_args()
    
    # 检查基础路径是否存在
    if not os.path.exists(args.base_path):
        print(f"错误: 路径 '{args.base_path}' 不存在!")
        sys.exit(1)
    
    # 创建提取器并运行
    extractor = BenchmarkDataExtractor(args.base_path)
    extractor.run_extraction(args.output)


if __name__ == "__main__":
    main()