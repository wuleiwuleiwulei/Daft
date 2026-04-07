import pandas as pd
import re

# 1. 配置
file_path = 'daft benchmark_0.7.5_v2.xlsx'
sheet_func = 'daft bench func_0312'
sheet_case = 'daft bench case _0312'
output_file = 'daft_performance_full_report_0312_0.7.5_v2.xlsx'

def get_match_key(name, suite):
    name_str = str(name).strip()
    suite_lower = str(suite).lower()
    if "tpch" in suite_lower or "tpcds" in suite_lower:
        match = re.search(r'(Q\d+)', name_str, re.IGNORECASE)
        return match.group(1).upper() if match else name_str
    ai_prefixes = ["audio_transcription", "document_embedding", "image_classification", "video_object_detection"]
    if "ai" in suite_lower:
        for pfx in ai_prefixes:
            if name_str.startswith(pfx): return pfx
    vllm_prefixes = ["naive-batch-sorted_", "naive-batch_"]
    if "vllm" in suite_lower:
        for pfx in vllm_prefixes:
            if name_str.startswith(pfx): return pfx
    return name_str.replace("_perf_combined", "").strip()

def write_sheet_with_formulas(writer, df_data, sheet_name):
    """向指定的Sheet写入数据、计算公式和统计行"""
    df = df_data.copy().reset_index(drop=True)
    
    # 转换类型以兼容 Excel 公式字符串
    cols_to_formula = [
        "920B优化标准库到持平后的最终时间", "920B与9654性能比（改之前的）", 
        "920B与9654性能比（改之后的）", "920B与9654性能比提升绝对值", "920B VS 9654耗时劣化比"
    ]
    for col in cols_to_formula:
        df[col] = df[col].astype(object)

    # 识别 Case 汇总行及对应的明细行范围
    case_ranges = []
    for i in range(len(df)):
        if df.iloc[i]['is_summary']:
            itf_start = None
            itf_end = None
            for j in range(i + 1, len(df)):
                if not df.iloc[j]['is_summary']:
                    if itf_start is None: itf_start = j + 2
                    itf_end = j + 2
                else:
                    break
            case_ranges.append((i, itf_start, itf_end))

    # 填充公式 (E:920B前, F:9654, G:920B后, H:比前, I:比后, J:提升, K:劣化比)
    for s_idx, r_start, r_end in case_ranges:
        row_num = s_idx + 2
        # 汇总行 G列：维持公式逻辑
        if r_start and r_end:
            df.at[s_idx, "920B优化标准库到持平后的最终时间"] = f"=E{row_num}-SUM(E{r_start}:E{r_end})+SUM(G{r_start}:G{r_end})"
        else:
            df.at[s_idx, "920B优化标准库到持平后的最终时间"] = f"=E{row_num}"
        
        # 汇总行 H, I, J, K 列比率公式
        df.at[s_idx, "920B与9654性能比（改之前的）"] = f"=(F{row_num}/E{row_num})"
        df.at[s_idx, "920B与9654性能比（改之后的）"] = f"=(F{row_num}/G{row_num})"
        df.at[s_idx, "920B与9654性能比提升绝对值"] = f"=(I{row_num}-H{row_num})"
        df.at[s_idx, "920B VS 9654耗时劣化比"] = f"=(F{row_num}-E{row_num})/F{row_num}"

    # 写入数据并处理 Sheet
    df_output = df.drop(columns=['is_summary'])
    df_output.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # 添加几何平均数统计
    worksheet = writer.sheets[sheet_name]
    last_data_row = len(df_output) + 1
    stat_row = last_data_row + 2
    worksheet.cell(row=stat_row, column=3, value="几何平均数统计 (Case级)")
    worksheet.cell(row=stat_row, column=8, value=f"=GEOMEAN(H2:H{last_data_row})")
    worksheet.cell(row=stat_row, column=9, value=f"=GEOMEAN(I2:I{last_data_row})")
    worksheet.cell(row=stat_row, column=10, value=f"=I{stat_row}-H{stat_row}")

def process_data():
    df_func_raw = pd.read_excel(file_path, sheet_name=sheet_func)
    df_case_raw = pd.read_excel(file_path, sheet_name=sheet_case)

    col_diff_raw = "鲲鹏920B VS AMD9654时延差距 (9654-920B)/9654，负值表示劣化"
    df_func_pool = df_func_raw[
        (df_func_raw["是否是Rust"] == True) & 
        (df_func_raw[col_diff_raw] < 0) & 
        (df_func_raw["Shared Object"] == "daft.abi3.so")
    ].copy()

    df_func_pool['match_key'] = df_func_pool.apply(lambda x: get_match_key(x["benchmark子项名"], x["benchmark测试套"]), axis=1)
    df_case_raw['match_key'] = df_case_raw.apply(lambda x: get_match_key(x["benchmark子项名"], x["benchmark测试套"]), axis=1)
    
    df_case_raw['kp_case_ms'] = df_case_raw["鲲鹏920B时延（单位：s）"] * 1000
    df_case_raw['amd_case_ms'] = df_case_raw["AMD9654时延（单位：s）"] * 1000

    suite_order = {'tpch': 0, 'tpcds': 1, 'parquet': 2, 'ai': 3, 'vllm': 4}
    df_case_raw['suite_rank'] = df_case_raw['benchmark测试套'].str.lower().map(lambda x: suite_order.get(x, 99))
    df_case_sorted = df_case_raw.sort_values(by=['suite_rank', '鲲鹏920B VS AMD9654差距'], ascending=[True, True])

    all_data_list = []
    for _, case_row in df_case_sorted.iterrows():
        suite = case_row['benchmark测试套']
        m_key = case_row['match_key']
        group = df_func_pool[(df_func_pool['benchmark测试套'] == suite) & (df_func_pool['match_key'] == m_key)]
        
        # 汇总行：最终时间初始设为优化前耗时
        all_data_list.append({
            "Benchmark": suite, "Case": m_key, "劣化接口": "性能用例", "920B中耗时占比": "",
            "920B耗时(ms)优化前的": case_row['kp_case_ms'], "9654耗时(ms)": case_row['amd_case_ms'],
            "920B优化标准库到持平后的最终时间": case_row['kp_case_ms'], 
            "920B与9654性能比（改之前的）": None, "920B与9654性能比（改之后的）": None,
            "920B与9654性能比提升绝对值": None, "920B VS 9654耗时劣化比": None, "is_summary": True
        })
        # 接口明细行：按照要求，G列初值填入 920B 优化前耗时 (即 E 列值)
        for _, row in group.iterrows():
            all_data_list.append({
                "Benchmark": suite, "Case": m_key, "劣化接口": row["接口"], "920B中耗时占比": row["920B耗时占比"],
                "920B耗时(ms)优化前的": row["鲲鹏920B时延(ms)"], "9654耗时(ms)": row["AMD9654时延(ms)"],
                "920B优化标准库到持平后的最终时间": row["鲲鹏920B时延(ms)"], # 这里改成了原耗时，不再是AMD耗时
                "920B与9654性能比（改之前的）": "", "920B与9654性能比（改之后的）": "",
                "920B与9654性能比提升绝对值": "", "920B VS 9654耗时劣化比": "", "is_summary": False
            })

    df_total = pd.DataFrame(all_data_list)

    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # 写入总表
        write_sheet_with_formulas(writer, df_total, "All_Benchmarks")
        # 按类型拆分分表
        for benchmark_name, df_group in df_total.groupby('Benchmark'):
            sheet_name = str(benchmark_name).lower()[:31]
            write_sheet_with_formulas(writer, df_group, sheet_name)

    print(f"处理完成！")
    print(f"接口明细行(G列)已设为920B原耗时，汇总行(G列)维持动态SUM公式。")
    print(f"输出路径: {output_file}")

if __name__ == "__main__":
    process_data()