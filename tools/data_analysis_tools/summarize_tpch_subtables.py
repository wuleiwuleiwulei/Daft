import pandas as pd
import os
import numpy as np

def merge_daft_benchmarks(input_file):
    """
    整合 TPCH 数据：
    1. 仅保留 Rust 函数实现
    2. 剔除 920B 和 9654 同时为 0 或为空的无效行
    3. 按照鲲鹏920B时延(ms)进行降序排列
    """
    if not os.path.exists(input_file):
        print(f"错误: 找不到文件 '{input_file}'")
        return

    print(f"正在读取文件: {input_file} ...")
    try:
        # 使用 ExcelFile 提高读取多 sheet 的效率
        excel_obj = pd.ExcelFile(input_file)
    except Exception as e:
        print(f"无法读取 Excel: {e}")
        return

    all_sheets = excel_obj.sheet_names
    # 只处理包含 TPCH_Q 的子表
    target_sheets = [s for s in all_sheets if 'TPCH_Q' in s.upper()]
    
    if not target_sheets:
        print("错误: 未在 Excel 中发现 TPCH_Q 系列子表。")
        return

    summary_list = []

    for sheet in target_sheets:
        df = pd.read_excel(excel_obj, sheet_name=sheet)
        
        # 1. 强力清洗列名：去除首尾空格，统一中间空格
        df.columns = [" ".join(str(c).strip().split()) for c in df.columns]
        
        temp_df = pd.DataFrame()
        
        # 2. 基础信息提取
        temp_df['benchmark测试套'] = ['TPCH'] * len(df)
        temp_df['benchmark子项名'] = sheet
        temp_df['接口'] = df.get('Function Name', "未知接口")
        temp_df['接口归类'] = "" 
        
        # 3. 提取 'Is Rust Function' (增加容错)
        rust_col_name = 'Is Rust Function'
        if rust_col_name not in df.columns:
            matched_cols = [c for c in df.columns if 'Rust' in c]
            if matched_cols:
                rust_col_name = matched_cols[0]
        
        # 统一转为字符串并清洗，确保后续过滤逻辑准确
        temp_df['是否是Rust'] = pd.Series(df.get(rust_col_name)).astype(str).str.strip()
        
        # 4. 时延数据映射 (修复 AttributeError: 'numpy.float64' object has no attribute 'fillna')
        # 通过 pd.Series 确保对象始终是 Pandas 序列，从而安全使用 fillna
        s_920 = pd.Series(df.get('920B Self耗时(ms)')).fillna(0)
        temp_df['鲲鹏920B时延(ms)'] = pd.to_numeric(s_920, errors='coerce').fillna(0)
        
        s_9654 = pd.Series(df.get('9654 Self耗时(ms)')).fillna(0)
        temp_df['AMD9654时延(ms)'] = pd.to_numeric(s_9654, errors='coerce').fillna(0)
        
        # 5. 时延差距映射 (劣化比)
        if '920B耗时劣化比' in df.columns:
            temp_df['鲲鹏920B VS AMD9654时延差距'] = df['920B耗时劣化比']
        else:
            liehua_cols = [c for c in df.columns if '劣化比' in c]
            temp_df['鲲鹏920B VS AMD9654时延差距'] = df[liehua_cols[0]] if liehua_cols else ""

        # 6. 耗时占比与调用次数映射
        temp_df['920B耗时占比'] = df.get('Self Time (%)_920', 0)
        temp_df['AMD9654耗时占比'] = df.get('Self Time (%)_9654', 0)
        temp_df['920B调用次数'] = df.get('Call Count_920', 0)
        temp_df['AMD9654调用次数'] = df.get('Call Count_9654', 0)
        
        # 7. 预留列
        temp_df['原因分析'] = ""
        temp_df['优化方案'] = ""
        
        summary_list.append(temp_df)

    # 合并所有子表提取的数据
    if not summary_list:
        print("未提取到任何有效数据。")
        return
        
    final_df = pd.concat(summary_list, ignore_index=True)

    # --- 核心过滤逻辑 ---
    # A. 只保留 Rust 函数行
    valid_rust_values = ['True', '1', '1.0', 'TRUE', 'true']
    final_df = final_df[final_df['是否是Rust'].isin(valid_rust_values)]

    # B. 剔除 920B 和 9654 时延同时为 0 或为空的行 (逻辑：只要有一个大于 0 就保留)
    final_df = final_df[(final_df['鲲鹏920B时延(ms)'] > 0) | (final_df['AMD9654时延(ms)'] > 0)]

    # --- 核心排序逻辑 ---
    # 按照 鲲鹏920B时延(ms) 降序排列 (从耗时最高的算子开始看)
    final_df = final_df.sort_values(by='鲲鹏920B时延(ms)', ascending=False)

    # 写入结果到新的 Sheet
    output_sheet = 'Daft benchmark接口性能'
    try:
        with pd.ExcelWriter(input_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            final_df.to_excel(writer, sheet_name=output_sheet, index=False)
        print(f"\n--- 整合完成 ---")
        print(f"1. 筛选范围: 仅限 Rust 接口实现")
        print(f"2. 数据清洗: 已剔除双零/双空无效耗时数据")
        print(f"3. 排序策略: 已按鲲鹏920B时延降序排列")
        print(f"4. 最终结果: 合计 {len(final_df)} 行数据已写入工作表 [{output_sheet}]")
    except Exception as e:
        print(f"保存失败: {e}\n提示: 运行前请务必先关闭 Excel 文件 '{input_file}'。")

if __name__ == "__main__":
    # 请确保文件名与你本地文件一致
    TARGET_FILENAME = "111.xlsx" 
    
    if os.path.exists(TARGET_FILENAME):
        merge_daft_benchmarks(TARGET_FILENAME)
    else:
        print(f"未找到文件: {TARGET_FILENAME}，请检查路径。")