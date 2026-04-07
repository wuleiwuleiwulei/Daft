import pandas as pd
import numpy as np
import argparse
import os
from pathlib import Path
import sys

def analyze_test_runs(input_file, output_dir, sheet_name='no_cpus_arg_perf'):
    """
    分析测试轮次数据，计算标准差，找出最稳定的轮次
    
    Args:
        input_file: 输入Excel文件路径
        output_dir: 输出目录
        sheet_name: 工作表名称
    """
    
    print(f"正在读取文件: {input_file}")
    print(f"工作表: {sheet_name}")
    
    try:
        # 读取Excel文件
        df = pd.read_excel(input_file, sheet_name=sheet_name)
    except Exception as e:
        print(f"读取文件时出错: {e}")
        return None
    
    print(f"数据形状: {df.shape}")
    print(f"列名: {list(df.columns)}")
    
    # 设置Query列为索引
    if 'Query' in df.columns:
        df.set_index('Query', inplace=True)
    else:
        print("错误: 未找到'Query'列")
        return None
    
    # 提取轮次列名（排除非轮次列）
    # 假设轮次列名以'run_'开头
    run_columns = [col for col in df.columns if col.startswith('run_')]
    
    if not run_columns:
        print("错误: 未找到以'run_'开头的轮次列")
        return None
    
    print(f"找到 {len(run_columns)} 个轮次: {run_columns}")
    
    # 提取测试名（行名）
    test_names = df.index.tolist()
    print(f"找到 {len(test_names)} 个测试: {test_names}")
    
    # 1. 计算每个测试的标准差（跨轮次）
    print("\n正在计算每个测试的标准差...")
    
    test_std_dict = {}
    for test in test_names:
        # 获取该测试在所有轮次中的耗时
        test_times = df.loc[test, run_columns].values
        
        # 计算标准差
        std_value = np.std(test_times)
        test_std_dict[test] = std_value
        
        print(f"  {test}: 标准差 = {std_value:.6f}")
    
    # 2. 计算每个轮次的所有测试标准差之和
    print("\n正在计算每个轮次的标准差之和...")
    
    run_std_sum_dict = {}
    for run in run_columns:
        # 获取该轮次所有测试的耗时
        run_times = df[run].values
        
        # 计算标准差
        std_value = np.std(run_times)
        run_std_sum_dict[run] = std_value
        
        print(f"  {run}: 标准差 = {std_value:.6f}")
    
    # 3. 找出标准差之和最小的轮次
    if run_std_sum_dict:
        min_std_run = min(run_std_sum_dict, key=run_std_sum_dict.get)
        min_std_value = run_std_sum_dict[min_std_run]
        
        print(f"\n标准差最小的轮次: {min_std_run}")
        print(f"标准差值: {min_std_value:.6f}")
    else:
        print("错误: 无法计算标准差")
        return None
    
    # 4. 提取该轮次的数据
    print(f"\n提取轮次 {min_std_run} 的数据...")
    
    # 创建新的DataFrame
    result_df = pd.DataFrame({
        'Query': df.index,
        'Duration': df[min_std_run]
    })
    
    # 确保输出目录存在
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # 生成输出文件名
    output_file = output_path / f"{min_std_run}.xlsx"
    
    # 5. 保存到Excel文件
    print(f"正在保存结果到: {output_file}")
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 保存主要数据
        result_df.to_excel(writer, sheet_name='Test_Durations', index=False)
        
        # 保存标准差统计信息
        std_stats = pd.DataFrame({
            'Test': list(test_std_dict.keys()),
            'Std_Dev_Across_Runs': list(test_std_dict.values())
        })
        std_stats = std_stats.sort_values('Std_Dev_Across_Runs', ascending=False)
        std_stats.to_excel(writer, sheet_name='Std_Dev_by_Test', index=False)
        
        # 保存轮次标准差信息
        run_stats = pd.DataFrame({
            'Run': list(run_std_sum_dict.keys()),
            'Std_Dev': list(run_std_sum_dict.values())
        })
        run_stats = run_stats.sort_values('Std_Dev')
        run_stats.to_excel(writer, sheet_name='Std_Dev_by_Run', index=False)
        
        # 保存原始数据（供参考）
        df.reset_index().to_excel(writer, sheet_name='Original_Data', index=False)
    
    print(f"完成! 结果已保存到: {output_file}")
    
    # 显示结果预览
    print(f"\n轮次 {min_std_run} 的数据预览:")
    print(result_df.head(10).to_string(index=False))
    
    return {
        'min_std_run': min_std_run,
        'min_std_value': min_std_value,
        'result_df': result_df,
        'test_std_dict': test_std_dict,
        'run_std_sum_dict': run_std_sum_dict,
        'output_file': output_file
    }

def main():
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(
        description='分析测试轮次数据，计算标准差，找出最稳定的轮次并输出',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python analyze_test_runs.py -i input.xlsx -o ./output/
  python analyze_test_runs.py --input test_data.xlsx --output ./results/
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='输入Excel文件路径'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='输出目录路径'
    )
    
    parser.add_argument(
        '-s', '--sheet',
        default='no_cpus_arg_perf',
        help='工作表名称（默认: no_cpus_arg_perf）'
    )
    
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='显示详细输出'
    )
    
    # 解析参数
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input):
        print(f"错误: 输入文件 '{args.input}' 不存在")
        sys.exit(1)
    
    # 运行分析
    result = analyze_test_runs(args.input, args.output, args.sheet)
    
    if result:
        print("\n" + "="*50)
        print("分析摘要")
        print("="*50)
        print(f"输入文件: {args.input}")
        print(f"输出目录: {args.output}")
        print(f"工作表: {args.sheet}")
        print(f"最稳定轮次: {result['min_std_run']}")
        print(f"标准差值: {result['min_std_value']:.6f}")
        print(f"输出文件: {result['output_file']}")
        
        # 显示额外统计信息
        if args.verbose:
            print(f"\n详细统计:")
            print(f"  测试数量: {len(result['test_std_dict'])}")
            print(f"  轮次数量: {len(result['run_std_sum_dict'])}")
            
            # 显示标准差最大的3个测试
            sorted_tests = sorted(result['test_std_dict'].items(), key=lambda x: x[1], reverse=True)
            print(f"\n标准差最大的3个测试:")
            for test, std in sorted_tests[:3]:
                print(f"  {test}: {std:.6f}")
            
            # 显示标准差最小的3个测试
            print(f"\n标准差最小的3个测试:")
            for test, std in sorted_tests[-3:]:
                print(f"  {test}: {std:.6f}")

if __name__ == "__main__":
    main()