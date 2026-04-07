import re
import pandas as pd
import os
from pathlib import Path
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
import argparse

class PerfRustLibAnalyzer:
    def __init__(self, perf_file: str):
        """
        初始化分析器
        
        Args:
            perf_file: perf结果文件路径
        """
        self.perf_file = perf_file
        self.results = []
        self.library_stats = defaultdict(lambda: {
            'total_time': 0.0,
            'call_count': 0,
            'calls': []
        })
        self.function_stats = defaultdict(lambda: {
            'total_time': 0.0,
            'call_count': 0,
            'calls': []
        })
        self.total_processed_time = 0.0
        self.total_function_time = 0.0
        
    def clean_symbol(self, symbol: str) -> str:
        """
        清理符号，去除前缀如[.]、[k]等
        
        Args:
            symbol: 原始符号
            
        Returns:
            清理后的符号
        """
        # 匹配 [.]、[k]、[.] 等前缀
        # 例如: [.] std::sys::pal::unix::thread::Thread::new::thread_start
        # 或者: [k] el0_svc
        match = re.match(r'^\[[^\]]+\]\s*(.+)$', symbol)
        if match:
            return match.group(1).strip()
        return symbol.strip()
    
    def extract_library_name(self, symbol: str) -> Tuple[str, bool]:
        """
        从符号中提取库名
        
        Args:
            symbol: 函数符号
            
        Returns:
            (库名, 是否是Rust函数)
        """
        # 先清理符号，去除[.]、[k]等前缀
        cleaned_symbol = self.clean_symbol(symbol)
        
        # 规则1: 检查是否是Rust函数调用形式
        # 普通形式: tokio::runtime::task::raw::poll
        # 泛型形式: <arrow2::io::parquet::read::deserialize::primitive::basic::Iter<T,I,P,F> as core::iter::traits::iterator::Iterator>::next
        
        # 先处理泛型形式
        if cleaned_symbol.startswith('<'):
            # 提取as前面的部分
            if ' as ' in cleaned_symbol:
                # 找到第一个' as '的位置
                as_index = cleaned_symbol.find(' as ')
                before_as = cleaned_symbol[1:as_index].strip()  # 跳过'<'
                
                # 提取第一个单词（直到第一个::）
                if '::' in before_as:
                    first_part = before_as.split('::')[0].strip()
                    # 检查是否是有效的Rust标识符（只包含字母数字和下划线）
                    if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', first_part):
                        return first_part, True
        
        # 处理普通形式
        if '::' in cleaned_symbol:
            # 提取第一个单词（直到第一个::）
            first_part = cleaned_symbol.split('::')[0].strip()
            # 检查是否是有效的Rust标识符
            if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', first_part):
                return first_part, True
        
        # 不是Rust函数形式
        return None, False
    
    def get_function_category(self, symbol: str, shared_object: str, library_name: Optional[str] = None, is_rust: bool = False) -> str:
        """
        获取函数类别
        
        Args:
            symbol: 原始符号
            shared_object: 共享对象
            library_name: 库名（如果已知）
            is_rust: 是否是Rust函数
            
        Returns:
            函数类别
        """
        # 清理符号，去除[.]、[k]等前缀
        cleaned_symbol = self.clean_symbol(symbol)
        
        # 规则1: 检查是否是Rust函数
        if is_rust and library_name:
            if 'daft' in library_name:
                return 'daft'
            if 'parquet' in library_name:
                return 'parquet'
            if 'arrow2' in library_name:
                return 'arrow2'
            if 'arrow_' in library_name:
                return 'arrow_rs'
            if 'pyo3' in library_name:
                return 'pyo3'
            if 'tokio' in library_name:
                return 'tokio'
            if 'parking_lot' in library_name:
                return 'parking_lot'
            return library_name
        
        # 规则2: 检查是否以[k]开头
        if symbol.strip().startswith('[k]'):
            return 'kernel'
        
        # 规则3: 检查特定函数名
        if cleaned_symbol == 'pthread_mutex_trylock':
            return 'libc'
        elif cleaned_symbol == 'syscall':
            return 'Syscall'
        
        # 规则4: 检查以特定前缀开头
        if (cleaned_symbol.startswith('_rjem') or 
            cleaned_symbol.startswith('do_rallocx') or 
            cleaned_symbol.startswith('tcache_')):
            return 'Rust Jem'
        
        # 规则5: 检查以0x开头（裸地址）
        # if cleaned_symbol.startswith('0x'):
        #     return 'libc'
        
        # 规则6: 检查是否是其他特定模式
        # 检查是否为libc函数（常见的libc函数）
        # libc_functions = ['malloc', 'free', 'calloc', 'realloc', 'memcpy', 'memset', 'strlen', 'strcpy']
        # for func in libc_functions:
        #     if func in cleaned_symbol:
        #         return 'Libc'
        
        # 检查是否为系统调用
        syscall_patterns = ['sys_', 'do_syscall', '__x64_sys']
        for pattern in syscall_patterns:
            if pattern in cleaned_symbol:
                return 'Syscall'
        
        # 规则7: 使用共享对象作为类别
        # 从共享对象中提取基础名称
        if shared_object:
            # 移除可能的路径和扩展名
            base_name = os.path.basename(shared_object)
            # 移除扩展名
            if '.' in base_name:
                base_name = base_name.split('.')[0]
                # if 'kernel' in base_name:
                #     return 'kernel'
                # if 'libc' in base_name:
                #     return 'libc'
                # if 'python' in base_name:
                #     return 'Python'
                # if 'libm' in base_name:
                #     return 'libm'
                # if 'daft' in base_name:
                #     return 'daft'
            return base_name
        
        # 规则8: 如果以上都不匹配，返回"Unknown"
        return 'Unknown'
    
    def parse_perf_file(self) -> List[Dict]:
        """
        解析perf结果文件
        
        Returns:
            解析后的结果列表
        """
        results = []
        
        # 新格式的正则表达式，包含Period列
        # 示例: 32.62%     0.00%             0  python           libc.so.6
        new_pattern = re.compile(r'^\s*(\d+\.\d+%?)\s+(\d+\.\d+%?)\s+(\d+)\s+(\S+)\s+(\S+)\s+(.+)$')
        
        # 旧格式的正则表达式，不包含Period列（为了向后兼容）
        # 示例: 22.95%     0.00%  python           libc.so.6
        old_pattern = re.compile(r'^\s*(\d+\.\d+%?)\s+(\d+\.\d+%?)\s+(\S+)\s+(\S+)\s+(.+)$')
        
        with open(self.perf_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # 跳过注释行和表头行
                if line.startswith('#') or line.startswith('Children') or line.startswith('...'):
                    continue
                
                # 先尝试匹配新格式（包含Period列）
                match = new_pattern.match(line)
                if match:
                    children, self_percent, period, command, shared_obj, symbol = match.groups()
                    has_period = True
                else:
                    # 尝试匹配旧格式
                    match = old_pattern.match(line)
                    if match:
                        children, self_percent, command, shared_obj, symbol = match.groups()
                        period = None
                        has_period = False
                    else:
                        # 如果都不匹配，跳过这一行
                        continue
                
                # 转换百分比为浮点数
                try:
                    self_percent_float = float(self_percent.rstrip('%'))
                except ValueError:
                    continue
                
                # 清理symbol
                cleaned_symbol = self.clean_symbol(symbol)
                
                # 提取库名
                library_name, is_rust = self.extract_library_name(cleaned_symbol)
                
                # 获取函数类别
                category = self.get_function_category(symbol, shared_obj, library_name, is_rust)
                
                result = {
                    'children': children,
                    'self_percent': self_percent,
                    'self_percent_float': self_percent_float,
                    'period': period,
                    'command': command,
                    'shared_object': shared_obj,
                    'symbol': symbol,  # 原始symbol
                    'cleaned_symbol': cleaned_symbol,  # 清理后的symbol
                    'library_name': library_name,
                    'is_rust': is_rust,
                    'has_period': has_period,
                    'category': category  # 新增：函数类别
                }
                results.append(result)
        
        self.results = results
        return results
    
    def analyze_libraries(self):
        """
        分析各个库的调用情况
        """
        rust_calls = [r for r in self.results if r['is_rust']]
        non_rust_calls = [r for r in self.results if not r['is_rust']]
        
        print(f"Total perf entries parsed: {len(self.results)}")
        
        # 检查是否有Period列
        has_period_any = any(r['has_period'] for r in self.results)
        if has_period_any:
            print("Detected new format with Period column")
        else:
            print("Detected old format without Period column")
        
        print(f"Rust function calls: {len(rust_calls)}")
        print(f"Non-Rust function calls: {len(non_rust_calls)}")
        
        # 统计各个库的信息
        for result in rust_calls:
            if result['library_name']:
                lib_name = result['library_name']
                self.library_stats[lib_name]['total_time'] += result['self_percent_float']
                self.library_stats[lib_name]['call_count'] += 1
                self.library_stats[lib_name]['calls'].append(result)
                self.total_processed_time += result['self_percent_float']
        
        # 按总时间降序排序
        sorted_libs = sorted(
            self.library_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        print(f"\nFound {len(self.library_stats)} Rust libraries")
        print(f"Total processed Rust function time: {self.total_processed_time:.2f}%")
        
        return sorted_libs
    
    def analyze_functions(self):
        """
        分析各个函数的总耗时情况（包括所有函数，不局限于Rust函数）
        """
        # 统计所有函数的信息
        for result in self.results:
            function_name = result['cleaned_symbol']
            if function_name:  # 确保函数名不为空
                # 如果该函数已经存在，更新统计信息
                if function_name in self.function_stats:
                    self.function_stats[function_name]['total_time'] += result['self_percent_float']
                    self.function_stats[function_name]['call_count'] += 1
                    self.function_stats[function_name]['calls'].append(result)
                else:
                    # 新函数，初始化统计信息
                    self.function_stats[function_name] = {
                        'total_time': result['self_percent_float'],
                        'call_count': 1,
                        'calls': [result],
                        'category': result.get('category', 'Unknown'),
                        'shared_object': result.get('shared_object', 'Unknown'),
                        'is_rust': result.get('is_rust', False)
                    }
                
                self.total_function_time += result['self_percent_float']
        
        # 按总时间降序排序
        sorted_functions = sorted(
            self.function_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        print(f"\nFound {len(self.function_stats)} unique functions")
        print(f"Total function time: {self.total_function_time:.2f}%")
        
        return sorted_functions
    
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
    
    def export_to_excel(self, output_file: str = "rust_libs_analysis.xlsx"):
        """
        导出结果到Excel文件
        
        Args:
            output_file: 输出文件名
        """
        # 如果没有任何结果，直接返回
        if not self.results:
            print("No results to export")
            return
        
        # 检查是否有Period列
        has_period_any = any(r['has_period'] for r in self.results)
        
        # 获取排序后的库列表
        sorted_libs = sorted(
            self.library_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        # 获取排序后的函数列表
        sorted_functions = sorted(
            self.function_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        # 创建Excel writer
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 1. 创建库汇总表
            if self.library_stats:
                summary_data = []
                for lib_name, stats in sorted_libs:
                    avg_time = stats['total_time'] / stats['call_count'] if stats['call_count'] > 0 else 0
                    summary_data.append({
                        'Library': lib_name,
                        'Total Time (%)': f"{stats['total_time']:.2f}",
                        'Call Count': stats['call_count'],
                        'Average Time per Call (%)': f"{avg_time:.4f}",
                        'Total Time Float': stats['total_time']  # 用于排序的隐藏列
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df = summary_df.sort_values('Total Time Float', ascending=False)
                summary_df = summary_df.drop('Total Time Float', axis=1)
                summary_df.to_excel(writer, sheet_name='Summary', index=False)
            else:
                # 如果没有Rust库数据，创建一个空表
                empty_summary = pd.DataFrame({'Message': ['No Rust library data found']})
                empty_summary.to_excel(writer, sheet_name='Summary', index=False)
            
            # 2. 创建函数汇总表 (Summary_function) - 修改了Is Rust Function列的逻辑
            if self.function_stats:
                function_summary_data = []
                for func_name, stats in sorted_functions:
                    avg_time = stats['total_time'] / stats['call_count'] if stats['call_count'] > 0 else 0
                    
                    # 修改后的Is Rust Function判断逻辑
                    # 原来的判断：stats['is_rust']
                    # 新逻辑：原来判断为True且Shared Object是daft.abi3.so才为TRUE
                    is_rust_original = stats['is_rust']
                    shared_object = stats.get('shared_object', '')
                    
                    # 新判断逻辑
                    is_rust_final = False
                    if is_rust_original:
                        # 提取共享对象的基础名称进行比较
                        if shared_object:
                            # 获取基础文件名，不包含路径
                            base_name = os.path.basename(shared_object)
                            if base_name == 'daft.abi3.so':
                                is_rust_final = True
                    
                    function_summary_data.append({
                        'Function Name': func_name,
                        'Category': stats['category'],
                        'Shared Object': shared_object,
                        'Total Time (%)': f"{stats['total_time']:.2f}",
                        'Call Count': stats['call_count'],
                        'Average Time per Call (%)': f"{avg_time:.4f}",
                        'Total Time Float': stats['total_time'],  # 用于排序的隐藏列
                        'Is Rust Function': is_rust_final,  # 使用修改后的判断逻辑
                        'Is Rust Original': is_rust_original  # 保留原始判断用于调试
                    })
                
                function_summary_df = pd.DataFrame(function_summary_data)
                function_summary_df = function_summary_df.sort_values('Total Time Float', ascending=False)
                function_summary_df = function_summary_df.drop('Total Time Float', axis=1)
                # 可以选择是否显示Is Rust Original列，这里为了调试暂时保留
                function_summary_df.to_excel(writer, sheet_name='Summary_function', index=False)
            else:
                # 如果没有函数数据，创建一个空表
                empty_function_summary = pd.DataFrame({'Message': ['No function data found']})
                empty_function_summary.to_excel(writer, sheet_name='Summary_function', index=False)
            
            # 3. 为每个库创建详细工作表
            for lib_name, stats in sorted_libs:
                # 清理工作表名称
                sheet_name = self.clean_sheet_name(lib_name)
                
                # 创建该库的详细调用数据
                detailed_data = []
                for call in stats['calls']:
                    if has_period_any and call.get('period') is not None:
                        detailed_data.append({
                            'Self Percentage (%)': call['self_percent'],
                            'Children Percentage (%)': call['children'],
                            'Period': call['period'],
                            'Command': call['command'],
                            'Shared Object': call['shared_object'],
                            'Original Symbol': call['symbol'],
                            'Cleaned Symbol': call['cleaned_symbol'],
                            'Library': call['library_name'],
                            'Category': call['category'],
                            'Self Percent Float': call['self_percent_float']  # 用于排序
                        })
                    else:
                        detailed_data.append({
                            'Self Percentage (%)': call['self_percent'],
                            'Children Percentage (%)': call['children'],
                            'Command': call['command'],
                            'Shared Object': call['shared_object'],
                            'Original Symbol': call['symbol'],
                            'Cleaned Symbol': call['cleaned_symbol'],
                            'Library': call['library_name'],
                            'Category': call['category'],
                            'Self Percent Float': call['self_percent_float']  # 用于排序
                        })
                
                if detailed_data:
                    detailed_df = pd.DataFrame(detailed_data)
                    detailed_df = detailed_df.sort_values('Self Percent Float', ascending=False)
                    detailed_df = detailed_df.drop('Self Percent Float', axis=1)
                    
                    # 写入工作表
                    detailed_df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            # 4. 创建非Rust函数汇总表
            non_rust_results = [r for r in self.results if not r['is_rust']]
            if non_rust_results:
                non_rust_data = []
                for result in non_rust_results:
                    if has_period_any and result.get('period') is not None:
                        non_rust_data.append({
                            'Self Percentage (%)': result['self_percent'],
                            'Children Percentage (%)': result['children'],
                            'Period': result['period'],
                            'Command': result['command'],
                            'Shared Object': result['shared_object'],
                            'Original Symbol': result['symbol'],
                            'Cleaned Symbol': result['cleaned_symbol'],
                            'Category': result['category'],
                            'Self Percent Float': result['self_percent_float']
                        })
                    else:
                        non_rust_data.append({
                            'Self Percentage (%)': result['self_percent'],
                            'Children Percentage (%)': result['children'],
                            'Command': result['command'],
                            'Shared Object': result['shared_object'],
                            'Original Symbol': result['symbol'],
                            'Cleaned Symbol': result['cleaned_symbol'],
                            'Category': result['category'],
                            'Self Percent Float': result['self_percent_float']
                        })
                
                non_rust_df = pd.DataFrame(non_rust_data)
                non_rust_df = non_rust_df.sort_values('Self Percent Float', ascending=False)
                non_rust_df = non_rust_df.drop('Self Percent Float', axis=1)
                non_rust_df.to_excel(writer, sheet_name='Non_Rust_Functions', index=False)
            
            # 5. 创建类别汇总表
            self.create_category_summary_sheet(writer)
            
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
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
        
        print(f"\nResults exported to {output_file}")
        
        # 计算工作表总数
        total_sheets = 3  # Summary, Summary_function 和 Category_Summary 总是存在
        total_sheets += len(self.library_stats)  # 各个库的工作表
        if non_rust_results:
            total_sheets += 1  # Non_Rust_Functions 工作表
        
        print(f"Total worksheets created: {total_sheets}")
        print(f"Data format: {'New format with Period column' if has_period_any else 'Old format without Period column'}")
    
    def create_category_summary_sheet(self, writer):
        """
        创建类别汇总工作表
        
        Args:
            writer: Excel writer对象
        """
        # 统计各个类别的总耗时
        category_stats = defaultdict(lambda: {'total_time': 0.0, 'call_count': 0, 'functions': set()})
        
        for result in self.results:
            category = result.get('category', 'Unknown')
            category_stats[category]['total_time'] += result['self_percent_float']
            category_stats[category]['call_count'] += 1
            category_stats[category]['functions'].add(result['cleaned_symbol'])
        
        # 准备数据
        category_data = []
        for category, stats in category_stats.items():
            avg_time = stats['total_time'] / stats['call_count'] if stats['call_count'] > 0 else 0
            unique_functions = len(stats['functions'])
            category_data.append({
                'Category': category,
                'Total Time (%)': stats['total_time'],
                'Call Count': stats['call_count'],
                'Unique Functions': unique_functions,
                'Average Time per Call (%)': avg_time,
                'Total Time Float': stats['total_time']  # 用于排序的隐藏列
            })
        
        # 按总时间降序排序
        category_data.sort(key=lambda x: x['Total Time Float'], reverse=True)
        
        # 创建DataFrame
        category_df = pd.DataFrame(category_data)
        category_df['Total Time (%)'] = category_df['Total Time (%)'].apply(lambda x: f"{x:.2f}")
        category_df['Average Time per Call (%)'] = category_df['Average Time per Call (%)'].apply(lambda x: f"{x:.4f}")
        category_df = category_df.drop('Total Time Float', axis=1)
        
        # 写入工作表
        category_df.to_excel(writer, sheet_name='Category_Summary', index=False)
    
    def print_summary(self):
        """
        打印分析摘要
        """
        if not self.results:
            print("No results found")
            return
        
        sorted_libs = sorted(
            self.library_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        sorted_functions = sorted(
            self.function_stats.items(),
            key=lambda x: x[1]['total_time'],
            reverse=True
        )
        
        print("\n" + "="*80)
        print("PERFORMANCE ANALYSIS SUMMARY")
        print("="*80)
        
        print(f"\nTotal perf entries parsed: {len(self.results)}")
        print(f"Total function time: {self.total_function_time:.2f}%")
        
        # 检查是否有Period列
        has_period_any = any(r['has_period'] for r in self.results)
        print(f"Data format: {'New format with Period column' if has_period_any else 'Old format without Period column'}")
        
        # 库统计摘要
        if sorted_libs:
            print(f"\nRUST LIBRARY STATISTICS:")
            print(f"  Total Rust function calls: {sum(stats['call_count'] for _, stats in sorted_libs)}")
            print(f"  Total Rust function time: {self.total_processed_time:.2f}%")
            print(f"  Number of unique Rust libraries: {len(sorted_libs)}")
            
            print(f"\n{'Rank':<6} {'Library':<20} {'Total Time (%)':<15} {'Call Count':<12} {'Avg Time (%)':<15}")
            print("-"*70)
            
            for i, (lib_name, stats) in enumerate(sorted_libs[:10], 1):
                avg_time = stats['total_time'] / stats['call_count'] if stats['call_count'] > 0 else 0
                print(f"{i:<6} {lib_name:<20} {stats['total_time']:>12.2f}% {stats['call_count']:>12} {avg_time:>14.4f}%")
        
        # 函数统计摘要
        if sorted_functions:
            print(f"\nFUNCTION STATISTICS:")
            print(f"  Total unique functions: {len(sorted_functions)}")
            
            print(f"\n{'Rank':<6} {'Function':<40} {'Category':<20} {'Total Time (%)':<12} {'Call Count':<10}")
            print("-"*90)
            
            for i, (func_name, stats) in enumerate(sorted_functions[:10], 1):
                func_display = func_name[:38] + "..." if len(func_name) > 38 else func_name
                category_display = stats['category'][:18] + "..." if len(stats['category']) > 18 else stats['category']
                print(f"{i:<6} {func_display:<40} {category_display:<20} {stats['total_time']:>11.2f}% {stats['call_count']:>10}")
        
        # 打印前5个最耗时的库
        if sorted_libs:
            print("\n" + "="*80)
            print("TOP 5 MOST TIME-CONSUMING LIBRARIES:")
            print("="*80)
            
            for i, (lib_name, stats) in enumerate(sorted_libs[:5], 1):
                percentage_of_total = (stats['total_time'] / self.total_function_time * 100) if self.total_function_time > 0 else 0
                print(f"\n{i}. {lib_name}:")
                print(f"   Total Time: {stats['total_time']:.2f}% ({percentage_of_total:.1f}% of total function time)")
                print(f"   Call Count: {stats['call_count']}")
                if stats['calls']:
                    top_symbol = stats['calls'][0]['cleaned_symbol']
                    if len(top_symbol) > 80:
                        top_symbol = top_symbol[:77] + "..."
                    print(f"   Top Call: {top_symbol}")
    
    def run_analysis(self, output_file: str = "rust_libs_analysis.xlsx"):
        """
        运行完整分析流程
        """
        print(f"Processing file: {self.perf_file}")
        print("Parsing perf file...")
        self.parse_perf_file()
        
        print("\nAnalyzing Rust libraries...")
        self.analyze_libraries()
        
        print("\nAnalyzing all functions...")
        self.analyze_functions()
        
        print("\nExporting results...")
        self.export_to_excel(output_file)
        
        self.print_summary()


def find_nosort_files(folder_path: str) -> List[str]:
    """
    查找文件夹中所有以"_nosort.txt"结尾的文件
    
    Args:
        folder_path: 文件夹路径
        
    Returns:
        找到的文件路径列表
    """
    nosort_files = []
    
    if not os.path.exists(folder_path):
        print(f"错误: 文件夹 '{folder_path}' 不存在!")
        return nosort_files
    
    # 遍历文件夹及其子文件夹
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith("_nosort.txt"):
                file_path = os.path.join(root, file)
                nosort_files.append(file_path)
    
    return nosort_files


def batch_analyze_perf_files(input_folder: str, output_folder: str):
    """
    批量分析perf文件
    
    Args:
        input_folder: 输入文件夹路径
        output_folder: 输出文件夹路径
    """
    print("="*80)
    print("开始批量分析perf文件")
    print("="*80)
    
    # 查找所有_nosort.txt文件
    print(f"在文件夹中查找_nosort.txt文件: {input_folder}")
    perf_files = find_nosort_files(input_folder)
    
    if not perf_files:
        print("未找到任何_nosort.txt文件!")
        return
    
    print(f"找到 {len(perf_files)} 个_nosort.txt文件:")
    for i, file_path in enumerate(perf_files, 1):
        print(f"  {i:3d}. {os.path.basename(file_path)}")
    
    # 创建输出文件夹
    os.makedirs(output_folder, exist_ok=True)
    print(f"\n输出文件夹: {output_folder}")
    
    # 处理每个文件
    success_count = 0
    fail_count = 0
    
    for i, perf_file in enumerate(perf_files, 1):
        print(f"\n{'='*60}")
        print(f"处理文件 {i}/{len(perf_files)}")
        print(f"文件名: {os.path.basename(perf_file)}")
        print('='*60)
        
        try:
            # 生成输出文件名
            base_name = os.path.basename(perf_file)
            excel_name = base_name.replace("_nosort.txt", ".xlsx")
            output_file = os.path.join(output_folder, excel_name)
            
            # 创建分析器并运行分析
            analyzer = PerfRustLibAnalyzer(perf_file=perf_file)
            analyzer.run_analysis(output_file=output_file)
            
            success_count += 1
            
        except Exception as e:
            print(f"处理文件 {perf_file} 时出错: {e}")
            fail_count += 1
    
    # 打印总结
    print("\n" + "="*80)
    print("批量分析完成!")
    print("="*80)
    print(f"总共处理文件: {len(perf_files)}")
    print(f"成功处理: {success_count}")
    print(f"处理失败: {fail_count}")
    print(f"输出文件夹: {output_folder}")
    
    # 列出所有生成的Excel文件
    if success_count > 0:
        excel_files = [f for f in os.listdir(output_folder) if f.endswith('.xlsx')]
        if excel_files:
            print(f"\n生成的Excel文件 ({len(excel_files)} 个):")
            for i, excel_file in enumerate(excel_files, 1):
                print(f"  {i:3d}. {excel_file}")


def main():
    parser = argparse.ArgumentParser(description='分析perf结果，统计Rust库时间消耗')
    
    # 创建互斥组，只能选择单个文件或批量处理
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--input-folder', '-i', help='输入文件夹路径，批量处理所有_nosort.txt文件')
    group.add_argument('--perf_file', nargs='?', help='单个perf文件路径')
    
    parser.add_argument('--output', '-o', default='rust_libs_analysis.xlsx', 
                       help='输出文件或文件夹路径（默认: rust_libs_analysis.xlsx）')
    
    args = parser.parse_args()
    
    # 判断是批量处理还是单个文件处理
    if args.input_folder:
        # 批量处理模式
        batch_analyze_perf_files(args.input_folder, args.output)
    else:
        # 单个文件处理模式（保持原有功能）
        if not os.path.exists(args.perf_file):
            print(f"错误: 文件 '{args.perf_file}' 不存在!")
            return
        
        # 确保输出文件夹存在
        output_dir = os.path.dirname(args.output)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        
        # 处理单个文件
        analyzer = PerfRustLibAnalyzer(perf_file=args.perf_file)
        analyzer.run_analysis(output_file=args.output)


if __name__ == "__main__":
    main()