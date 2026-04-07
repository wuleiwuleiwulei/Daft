import pandas as pd
import numpy as np
import os

def process_and_merge_perf_data():
    # 1. 核心参数 (9654 总运行时间: 0.485345s, 920B: 0.547945s)
    T_9654 = 0.485345
    T_920B = 0.547945
    SHEET = 'Summary_function'
    OUT = 'perf_combined_Summary_v3.xlsx'

    print(">>> 启动多维度数据合并分析 (基于 Self Time 排序)...")

    # 2. 检查文件
    f920, f9654 = 'perf_13ns_ch_920B.xlsx', 'perf_13ns_ch_9654.xlsx'
    for f in [f920, f9654]:
        if not os.path.exists(f):
            print(f"!!! 找不到文件: {f}"); return

    try:
        d920_raw = pd.read_excel(f920, sheet_name=SHEET)
        d9654_raw = pd.read_excel(f9654, sheet_name=SHEET)
    except Exception as e:
        print(f"!!! 读取失败: {e}"); return

    # 3. 列名清洗与映射逻辑
    def prepare_df(df):
        df.columns = [str(c).strip().replace('\n', '') for c in df.columns]
        mapping = {}
        
        for c in df.columns:
            low_c = c.lower()
            if not mapping.get('Function Name') and any(k == low_c for k in ['function name', 'symbol', 'name']):
                mapping[c] = 'Function Name'
            if not mapping.get('Self Time (%)') and 'self' in low_c:
                mapping[c] = 'Self Time (%)'
            if not mapping.get('Total Time (%)') and 'total' in low_c:
                mapping[c] = 'Total Time (%)'
            if not mapping.get('Call Count') and 'call' in low_c:
                mapping[c] = 'Call Count'
            if not mapping.get('Is Rust Function') and 'rust' in low_c:
                mapping[c] = 'Is Rust Function'
        
        res = df.rename(columns=mapping)
        
        required_cols = ['Function Name', 'Self Time (%)', 'Total Time (%)', 'Call Count', 'Is Rust Function']
        for col in required_cols:
            if col not in res.columns:
                res[col] = np.nan
        
        for col in ['Self Time (%)', 'Total Time (%)']:
            if res[col].dtype == object:
                res[col] = pd.to_numeric(res[col].astype(str).str.replace('%', ''), errors='coerce')
        
        return res

    d920 = prepare_df(d920_raw)
    d9654 = prepare_df(d9654_raw)

    # 4. 聚合重复行
    def aggregate_duplicates(df):
        agg_rules = {
            'Self Time (%)': 'sum',
            'Total Time (%)': 'sum',
            'Call Count': 'sum',
            'Is Rust Function': 'first'
        }
        return df.groupby('Function Name', as_index=False).agg(agg_rules)

    d920 = aggregate_duplicates(d920)
    d9654 = aggregate_duplicates(d9654)

    # 5. 计算绝对耗时 (ms)
    d920['920B Self耗时(ms)'] = (d920['Self Time (%)'] / 100 * T_920B * 1000).round(4)
    d9654['9654 Self耗时(ms)'] = (d9654['Self Time (%)'] / 100 * T_9654 * 1000).round(4)

    # 6. 数据合并
    merged = pd.merge(
        d920, d9654,
        on='Function Name', how='outer', suffixes=('_920', '_9654')
    )

    # 7. 修改：计算 Self 耗时比 ( (9654 - 920B) / 9654 )
    def calc_self_ratio(r):
        v_arm, v_x86 = r['920B Self耗时(ms)'], r['9654 Self耗时(ms)']
        if pd.notna(v_arm) and pd.notna(v_x86) and v_x86 > 0:
            # 逻辑：(X86耗时 - ARM耗时) / X86耗时
            # 正数表示 ARM 更快，负数表示 ARM 更慢
            ratio = (v_x86 - v_arm) / v_x86
            return f"{ratio:+.2%}" 
        return "N/A"
    
    merged['Self耗时比(920B VS 9654)'] = merged.apply(calc_self_ratio, axis=1)
    
    merged['Is Rust Function'] = merged['Is Rust Function_920'].fillna(merged['Is Rust Function_9654'])

    # 8. 整理最终列并排序
    final_cols = [
        'Function Name', 
        '920B Self耗时(ms)', 
        '9654 Self耗时(ms)', 
        'Self耗时比(920B VS 9654)',
        'Self Time (%)_920', 
        'Self Time (%)_9654',
        'Total Time (%)_920', 
        'Total Time (%)_9654',
        'Call Count_920', 
        'Call Count_9654',
        'Is Rust Function'
    ]
    
    output_df = merged[final_cols].sort_values(by='920B Self耗时(ms)', ascending=False)

    # 9. 格式化百分号
    pct_cols_to_format = [
        'Self Time (%)_920', 'Self Time (%)_9654', 
        'Total Time (%)_920', 'Total Time (%)_9654'
    ]
    
    for col in pct_cols_to_format:
        if col in output_df.columns:
            output_df[col] = output_df[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "0.00%")

    # 10. 输出保存
    output_df.to_excel(OUT, index=False)
    
    print("-" * 50)
    print(f">>> 处理完成！")
    print(f">>> 结果文件: {OUT}")
    print(f">>> 耗时比公式: (9654_ms - 920B_ms) / 9654_ms")

if __name__ == "__main__":
    process_and_merge_perf_data()