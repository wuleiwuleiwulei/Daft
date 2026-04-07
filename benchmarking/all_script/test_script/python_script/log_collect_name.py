import os
import re
import pandas as pd
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import argparse
import sys


class TpchLogDataCollector:
    def __init__(self, base_folder: str):
        """
        初始化收集器
        
        Args:
            base_folder: 基准文件夹路径，包含多个run_*文件夹
        """
        self.base_folder = Path(base_folder)
        self.all_results = []  # 存储所有结果
        self.run_folders = []  # 存储所有轮次文件夹
        self.configs = ['no_cpus_arg', 'num_cpus_40', 'no_binding', 'multi_core']  # 配置类型
        self.test_types = ['noperf_logs', 'perf_logs']  # 测试类型
        
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
    
    def extract_time_from_log_file(self, log_file: Path) -> Optional[float]:
        """
        从log文件中提取耗时数据
        
        Args:
            log_file: log文件路径
            
        Returns:
            耗时（秒），如果提取失败则返回None
        """
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 使用正则表达式查找"ALL PYTHON Time taken:"后面的秒数
            # 格式: ALL PYTHON Time taken: 1.9784560999833047 seconds
            pattern = r'ALL PYTHON Time taken:\s*([\d\.]+)\s*seconds'
            match = re.search(pattern, content)
            
            if match:
                time_str = match.group(1)
                try:
                    return float(time_str)
                except ValueError:
                    print(f"警告: 无法转换时间字符串: {time_str}")
                    return None
            else:
                # 尝试其他可能的模式
                patterns = [
                    r'Time taken:\s*([\d\.]+)\s*seconds',
                    r'耗时:\s*([\d\.]+)\s*秒',
                    r'duration:\s*([\d\.]+)\s*seconds',
                    r'execution time:\s*([\d\.]+)\s*seconds',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, content, re.IGNORECASE)
                    if match:
                        time_str = match.group(1)
                        try:
                            return float(time_str)
                        except ValueError:
                            continue
                
                print(f"警告: 在文件 {log_file.name} 中未找到耗时数据")
                return None
                
        except Exception as e:
            print(f"读取文件 {log_file.name} 时出错: {e}")
            return None
    
    def extract_query_info_from_filename(self, filename: str) -> Tuple[Optional[str], Optional[str]]:
        """
        从文件名中提取查询编号和测试类型
        
        Args:
            filename: 文件名
            
        Returns:
            (查询编号, 测试类型)
        """
        # 多种可能的文件名格式:
        # 1. q17_noperf_output.log 或 q17_perf_output.log
        # 2. query17.log 或 query_17.log
        # 3. tpch17.log 或 tpch_17.log
        # 4. Q17.log 或 Q_17.log
        # 5. 17.log (纯数字)
        # 6. 其他包含数字的文件名
        
        query_num = None
        test_type = None
        
        # 清理文件名，去掉扩展名和常见的后缀
        clean_name = filename.lower()
        if clean_name.endswith('.log'):
            clean_name = clean_name[:-4]
        
        # 去掉常见的后缀
        clean_name = re.sub(r'_(noperf|perf|output|result|log|run|test|query)$', '', clean_name)
        clean_name = re.sub(r'_(noperf|perf|output|result|log|run|test|query)_', '_', clean_name)
        
        # 方法1: 尝试提取 qX 或 QX 格式
        match = re.search(r'[qQ]_?(\d+)', clean_name)
        if not match:
            # 方法2: 尝试提取 queryX 或 query_X 格式
            match = re.search(r'query_?(\d+)', clean_name)
        
        if not match:
            # 方法3: 尝试提取 tpchX 或 tpch_X 格式
            match = re.search(r'tpch_?(\d+)', clean_name)
        
        if not match:
            # 方法4: 尝试提取纯数字
            match = re.search(r'^(\d+)$', clean_name)
        
        if not match:
            # 方法5: 尝试提取任何位置的数字
            match = re.search(r'(\d+)', clean_name)
        
        if match:
            query_num = match.group(1)
        
        # 从原始文件名中提取测试类型
        if 'noperf' in filename.lower():
            test_type = 'noperf'
        elif 'perf' in filename.lower():
            test_type = 'perf'
        
        return query_num, test_type
    
    def collect_all_results(self):
        """
        收集所有轮次、所有配置、所有测试类型的log数据
        """
        print("开始收集所有log数据...")
        
        # 首先查找所有轮次文件夹
        self.find_run_folders()
        
        if not self.run_folders:
            print("没有找到任何轮次文件夹!")
            return
        
        # 遍历每个轮次
        for run_name in self.run_folders:
            print(f"\n处理轮次: {run_name}")
            
            # 遍历每种配置
            for config in self.configs:
                print(f"  配置: {config}")
                
                # 遍历每种测试类型
                for test_type in self.test_types:
                    print(f"    测试类型: {test_type}")
                    
                    # 构造log文件夹路径
                    log_folder = self.base_folder / run_name / config / test_type
                    
                    if not log_folder.exists():
                        print(f"      警告: 路径不存在: {log_folder}")
                        continue
                    
                    # 查找所有log文件
                    log_files = list(log_folder.glob("*.log"))
                    
                    if not log_files:
                        print(f"      在 {log_folder} 中未找到log文件")
                        continue
                    
                    print(f"      找到 {len(log_files)} 个log文件")
                    
                    # 处理每个log文件
                    for log_file in log_files:
                        # 从文件名中提取查询编号和测试类型
                        query_num, file_test_type = self.extract_query_info_from_filename(log_file.name)
                        
                        # 如果无法从文件名提取测试类型，使用文件夹的测试类型
                        if not file_test_type:
                            file_test_type = test_type.replace('_logs', '')
                        
                        # 如果无法提取查询编号，使用文件名（去掉扩展名）作为查询编号
                        if not query_num:
                            # 尝试多种方式生成查询编号
                            base_name = log_file.stem  # 去掉扩展名的文件名
                            
                            # 去掉常见的后缀
                            clean_name = base_name.lower()
                            clean_name = re.sub(r'_(noperf|perf|output|result|log|run|test|query)$', '', clean_name)
                            clean_name = re.sub(r'_(noperf|perf|output|result|log|run|test|query)_', '_', clean_name)
                            
                            # 如果清理后的名字是纯数字，使用它
                            if clean_name.isdigit():
                                query_num = clean_name
                            else:
                                # 尝试提取任何数字
                                match = re.search(r'(\d+)', clean_name)
                                if match:
                                    query_num = match.group(1)
                                else:
                                    # 如果还是没有数字，使用原始文件名
                                    query_num = base_name
                        
                        if query_num:
                            # 确保查询编号是一个字符串
                            query_num = str(query_num)
                            
                            # 提取耗时数据
                            time_seconds = self.extract_time_from_log_file(log_file)
                            
                            if time_seconds is not None:
                                # 标准化查询编号格式为 QXX
                                query_display = f"Q{query_num.zfill(2)}" if query_num.isdigit() else query_num
                                
                                result = {
                                    'Run': run_name,
                                    'Config': config,
                                    'Query': query_display,
                                    'TestType': file_test_type,
                                    'Time (seconds)': time_seconds,
                                    'Filename': log_file.name
                                }
                                self.all_results.append(result)
                                
                                print(f"        提取: {query_display} - {time_seconds:.6f} 秒 (来自: {log_file.name})")
                            else:
                                print(f"        警告: 无法从 {log_file.name} 提取耗时数据")
                        else:
                            print(f"        警告: 无法从文件名 {log_file.name} 提取查询编号")
        
        print(f"\n收集完成! 总共收集 {len(self.all_results)} 条耗时数据")
    
    def export_to_excel(self, output_file: str = "tpch_log_times.xlsx"):
        """
        导出结果到Excel文件
        
        Args:
            output_file: 输出文件名
        """
        if not self.all_results:
            print("没有数据可导出")
            return
        
        print(f"\n导出结果到: {output_file}")
        
        # 创建DataFrame
        df = pd.DataFrame(self.all_results)
        
        # 重新排列列顺序
        df = df[['Run', 'Config', 'Query', 'TestType', 'Time (seconds)', 'Filename']]
        
        # 创建Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 1. 所有结果汇总表
            df.to_excel(writer, sheet_name='All Results', index=False)
            
            # 2. 按配置和测试类型分组的工作表
            for config in self.configs:
                for test_type in ['noperf', 'perf']:
                    # 过滤该配置和测试类型的数据
                    subset_df = df[(df['Config'] == config) & (df['TestType'] == test_type)].copy()
                    
                    if not subset_df.empty:
                        # 移除Config和TestType列，因为工作表名称已经包含这些信息
                        subset_df = subset_df.drop(['Config', 'TestType'], axis=1)
                        
                        # 透视表：行是Query，列是Run
                        pivot_df = subset_df.pivot_table(
                            index='Query', 
                            columns='Run', 
                            values='Time (seconds)',
                            aggfunc='mean'  # 如果有多个相同Query，取平均值
                        )
                        
                        # 重置索引，使Query成为列
                        pivot_df = pivot_df.reset_index()
                        
                        # 重命名列，使Run列更易读
                        pivot_df.columns.name = None
                        
                        # 按Query编号排序
                        # 提取数字部分进行排序
                        pivot_df['Query_Num'] = pivot_df['Query'].apply(
                            lambda x: int(re.search(r'(\d+)', str(x)).group(1)) if re.search(r'(\d+)', str(x)) else float('inf')
                        )
                        pivot_df = pivot_df.sort_values('Query_Num')
                        pivot_df = pivot_df.drop('Query_Num', axis=1)
                        
                        # 工作表名称
                        sheet_name = f"{config}_{test_type}"
                        # Excel工作表名称不能超过31个字符
                        if len(sheet_name) > 31:
                            sheet_name = sheet_name[:31]
                        
                        # 写入工作表
                        pivot_df.to_excel(writer, sheet_name=sheet_name, index=False)
                        print(f"  创建工作表: {sheet_name}")
            
            # 3. 按轮次汇总的工作表
            for run_name in self.run_folders:
                # 过滤该轮次的数据
                run_df = df[df['Run'] == run_name].copy()
                
                if not run_df.empty:
                    # 移除Run列
                    run_df = run_df.drop('Run', axis=1)
                    
                    # 透视表：行是Query，列是Config和TestType的组合
                    pivot_df = run_df.pivot_table(
                        index='Query', 
                        columns=['Config', 'TestType'], 
                        values='Time (seconds)',
                        aggfunc='mean'
                    )
                    
                    # 扁平化多级列索引
                    pivot_df.columns = [f"{config}_{test_type}" for config, test_type in pivot_df.columns]
                    pivot_df = pivot_df.reset_index()
                    
                    # 按Query编号排序
                    pivot_df['Query_Num'] = pivot_df['Query'].apply(
                        lambda x: int(re.search(r'(\d+)', str(x)).group(1)) if re.search(r'(\d+)', str(x)) else float('inf')
                    )
                    pivot_df = pivot_df.sort_values('Query_Num')
                    pivot_df = pivot_df.drop('Query_Num', axis=1)
                    
                    # 工作表名称
                    sheet_name = run_name
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]
                    
                    # 写入工作表
                    pivot_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"  创建工作表: {sheet_name}")
            
            # 4. 统计摘要工作表
            self.create_summary_sheet(writer, df)
            
            # 5. 按查询汇总的工作表
            self.create_query_summary_sheet(writer, df)
            
            # 6. 调整所有工作表的列宽
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
        
        # 打印统计信息
        self.print_summary()
    
    def create_summary_sheet(self, writer, df):
        """
        创建统计摘要工作表
        
        Args:
            writer: Excel writer对象
            df: 数据DataFrame
        """
        summary_data = []
        
        # 按轮次、配置和测试类型分组统计
        for run_name in df['Run'].unique():
            run_df = df[df['Run'] == run_name]
            
            for config in run_df['Config'].unique():
                config_df = run_df[run_df['Config'] == config]
                
                for test_type in config_df['TestType'].unique():
                    test_df = config_df[config_df['TestType'] == test_type]
                    
                    if not test_df.empty:
                        min_time = test_df['Time (seconds)'].min()
                        max_time = test_df['Time (seconds)'].max()
                        mean_time = test_df['Time (seconds)'].mean()
                        median_time = test_df['Time (seconds)'].median()
                        std_time = test_df['Time (seconds)'].std()
                        query_count = len(test_df['Query'].unique())
                        
                        summary_data.append({
                            'Run': run_name,
                            'Config': config,
                            'TestType': test_type,
                            'Query Count': query_count,
                            'Min Time (s)': f"{min_time:.6f}",
                            'Max Time (s)': f"{max_time:.6f}",
                            'Mean Time (s)': f"{mean_time:.6f}",
                            'Median Time (s)': f"{median_time:.6f}",
                            'Std Dev (s)': f"{std_time:.6f}" if not pd.isna(std_time) else "N/A"
                        })
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary Statistics', index=False)
            print(f"  创建工作表: Summary Statistics")
    
    def create_query_summary_sheet(self, writer, df):
        """
        创建按查询汇总的工作表
        
        Args:
            writer: Excel writer对象
            df: 数据DataFrame
        """
        # 找出所有唯一的查询
        # 先提取数字部分进行排序
        queries = df['Query'].unique()
        # 按数字部分排序
        sorted_queries = sorted(queries, key=lambda x: (
            int(re.search(r'(\d+)', str(x)).group(1)) if re.search(r'(\d+)', str(x)) else float('inf'),
            str(x)
        ))
        
        query_summary_data = []
        
        for query in sorted_queries:
            query_df = df[df['Query'] == query]
            
            if not query_df.empty:
                # 统计该查询在不同轮次、配置和测试类型中的表现
                min_time = query_df['Time (seconds)'].min()
                max_time = query_df['Time (seconds)'].max()
                mean_time = query_df['Time (seconds)'].mean()
                median_time = query_df['Time (seconds)'].median()
                std_time = query_df['Time (seconds)'].std()
                count = len(query_df)
                
                # 收集统计信息
                runs = ', '.join(sorted(query_df['Run'].unique()))
                configs = ', '.join(sorted(query_df['Config'].unique()))
                test_types = ', '.join(sorted(query_df['TestType'].unique()))
                
                query_summary_data.append({
                    'Query': query,
                    'Count': count,
                    'Min Time (s)': f"{min_time:.6f}",
                    'Max Time (s)': f"{max_time:.6f}",
                    'Mean Time (s)': f"{mean_time:.6f}",
                    'Median Time (s)': f"{median_time:.6f}",
                    'Std Dev (s)': f"{std_time:.6f}" if not pd.isna(std_time) else "N/A",
                    'Runs': runs,
                    'Configs': configs,
                    'Test Types': test_types
                })
        
        if query_summary_data:
            query_summary_df = pd.DataFrame(query_summary_data)
            query_summary_df.to_excel(writer, sheet_name='Query Summary', index=False)
            print(f"  创建工作表: Query Summary")
    
    def print_summary(self):
        """
        打印收集结果摘要
        """
        if not self.all_results:
            print("没有收集到任何结果")
            return
        
        # 创建DataFrame用于统计
        df = pd.DataFrame(self.all_results)
        
        print("\n" + "="*80)
        print("TPC-H LOG耗时数据收集摘要")
        print("="*80)
        
        print(f"\n总计: {len(self.all_results)} 条耗时数据")
        print(f"轮次数量: {len(df['Run'].unique())} 个")
        print(f"配置类型: {len(df['Config'].unique())} 种")
        print(f"测试类型: {len(df['TestType'].unique())} 种")
        print(f"唯一查询数量: {len(df['Query'].unique())} 个")
        
        # 按轮次统计
        print(f"\n按轮次统计:")
        for run_name in df['Run'].unique():
            run_df = df[df['Run'] == run_name]
            print(f"  {run_name}: {len(run_df)} 条数据")
        
        # 按配置统计
        print(f"\n按配置统计:")
        for config in df['Config'].unique():
            config_df = df[df['Config'] == config]
            print(f"  {config}: {len(config_df)} 条数据")
            
            # 按测试类型统计
            for test_type in config_df['TestType'].unique():
                type_df = config_df[config_df['TestType'] == test_type]
                print(f"    {test_type}: {len(type_df)} 条数据")
        
        # 按查询统计
        print(f"\n按查询统计 (前20个):")
        # 按数字部分排序
        sorted_queries = sorted(df['Query'].unique(), key=lambda x: (
            int(re.search(r'(\d+)', str(x)).group(1)) if re.search(r'(\d+)', str(x)) else float('inf'),
            str(x)
        ))
        
        for query in sorted_queries[:20]:
            query_df = df[df['Query'] == query]
            min_time = query_df['Time (seconds)'].min()
            max_time = query_df['Time (seconds)'].max()
            mean_time = query_df['Time (seconds)'].mean()
            print(f"  {query}: {len(query_df)} 条数据, 时间范围: {min_time:.3f}-{max_time:.3f}s, 平均: {mean_time:.3f}s")
        
        if len(sorted_queries) > 20:
            print(f"  ... 还有 {len(sorted_queries) - 20} 个查询")
        
        # 显示文件名格式示例
        print(f"\n处理的文件名格式示例:")
        sample_files = df['Filename'].unique()[:10]
        for filename in sample_files:
            print(f"  - {filename}")
        
        if len(df['Filename'].unique()) > 10:
            print(f"  ... 还有 {len(df['Filename'].unique()) - 10} 种不同的文件名格式")
        
        print("\n" + "="*80)
    
    def run_collection(self, output_file: str = "tpch_log_times.xlsx"):
        """
        运行完整收集流程
        
        Args:
            output_file: 输出文件名
        """
        print(f"开始收集TPC-H日志耗时数据...")
        print(f"基准文件夹: {self.base_folder}")
        
        self.collect_all_results()
        self.export_to_excel(output_file)


def main():
    parser = argparse.ArgumentParser(description='收集TPC-H日志耗时数据并导出到Excel')
    parser.add_argument('base_folder', help='基准文件夹路径（包含多个run_*文件夹）')
    parser.add_argument('--output', '-o', default='tpch_log_times.xlsx', 
                       help='输出Excel文件名称（默认: tpch_log_times.xlsx）')
    
    args = parser.parse_args()
    
    collector = TpchLogDataCollector(base_folder=args.base_folder)
    collector.run_collection(output_file=args.output)


if __name__ == "__main__":
    main()