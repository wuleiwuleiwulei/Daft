import os
import glob
import csv
import re
import subprocess
from collections import defaultdict
from pathlib import Path


# 输出文件名
OUTPUT_CSV = "symbol_period_analysis.csv"
INPUT_CSV = "symbol_period_analysis.csv"
OUTPUT_MD = "perf_report2.md"

def clean_perf_symbol(s):
    pattern = r"^(\[.+?\])\s+(.+?)\s{2,}([^\s]+)"
    match = re.search(pattern, s.strip())
    if match:
        tag = match.group(1)      
        symbol = match.group(2)   
        dso = match.group(3)      
        return f"{tag} {symbol} ({dso})"
    return s.strip()

def read_and_aggregate_csv(path):
    # 聚合后的数据结构: {(Classify, Symbol): Total_Period}
    aggregated_data = defaultdict(int)
    file_total_periods = defaultdict(int)
    total_system_period = 0
    rows = []
    
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f) # 使用 DictReader 更安全
        for row in reader:
            clean_name = clean_perf_symbol(row["Symbol"])
            cls = row["Classify"]
            period = int(row["Period"])
            filename = row["File"]
            
            rows.append({
                "File": row["File"],
                "Self %": float(row["Self %"]),
                "Period": period,
                "Classify": cls,
                "Symbol": clean_name,
            })
            aggregated_data[(cls, clean_name)] += period
            file_total_periods[(filename, clean_name)] += period # 根据文件和符号分类
            total_system_period += period
            
    return aggregated_data, total_system_period, rows, file_total_periods

def generate_markdown(aggregated_data, total_system_period, rows, file_total_periods):
    md = []
    md.append("# Perf Symbol Analysis Report (Global Aggregated)\n")
    md.append(f"**Total System Period:** `{total_system_period:,}`\n")

    # 按 Classify 分组
    by_classify = defaultdict(list)
    classify_total_period = defaultdict(int)

    for (cls, sym), period in aggregated_data.items():
        by_classify[cls].append({"Symbol": sym, "Period": period})
        classify_total_period[cls] += period

    # =========================
    # Part 1: Summary Table (每个分类在全局的占比)
    # =========================
    md.append("## Part 1: Classification Summary\n")
    md.append("| Classify | Total Period | Global % |")
    md.append("|:---------|-------------:|---------:|")
    
    # 按全局占比降序排
    sorted_cls = sorted(classify_total_period.items(), key=lambda x: x[1], reverse=True)
    for cls, p in sorted_cls:
        global_pct = (p / total_system_period) * 100
        md.append(f"| **{cls}** | {p:,} | {global_pct:.2f}% |")
    md.append("\n---\n")

    # =========================
    # Part 2: By Case
    # =========================
    md.append("## Part 2: By Case\n")

    by_file = defaultdict(list)
    for r in rows:
        by_file[r["File"]].append(r)

    for file, items in sorted(by_file.items()):
        symbol_dict = dict()
        md.append(f"### Case: `{file}`\n")
        md.append("| Self % | Period | Classify | Symbol |")
        md.append("|------:|--------:|----------:|--------|")

        for r in sorted(items, key=lambda x: x["Self %"], reverse=True):
            md.append(
                f"| {r['Self %']:.2f} | `{r['Period']}` | {r['Classify']} | {r['Symbol']}"
            )

        md.append("")

    # =========================
    # Part 3: Detailed Breakdown (每个分类内部 Symbol 占比)
    # =========================
    md.append("## Part 3: Symbol Breakdown by Classify\n")
    md.append("> 这里的 `Group %` 表示该 Symbol 在所属分类中的开销占比。\n")

    for cls, _ in sorted_cls:
        items = by_classify[cls]
        cls_total = classify_total_period[cls]
        global_cls_pct = (cls_total / total_system_period) * 100

        md.append(f"### `{cls}` (Global: {global_cls_pct:.2f}%)\n")
        md.append("| Symbol | Period | Group % | Global % |")
        md.append("|:-------|-------:|--------:|---------:|")

        # 组内按 Period 降序排
        sorted_items = sorted(items, key=lambda x: x["Period"], reverse=True)
        for item in sorted_items:
            group_pct = (item["Period"] / cls_total) * 100
            global_sym_pct = (item["Period"] / total_system_period) * 100
            md.append(
                f"| `{item['Symbol']}` | {item['Period']:,} | {group_pct:.2f}% | {global_sym_pct:.2f}% |"
            )
        md.append("")

    return "\n".join(md)

def extract_rust_library_name(symbol: str) -> str:
    """
    从Rust函数符号中提取库名
    
    Args:
        symbol: 函数符号
        
    Returns:
        库名，如果不是Rust函数则返回None
    """
    # 清理符号，去除[.]、[k]等前缀
    match = re.match(r'^\[[^\]]+\]\s*(.+)$', symbol)
    if match:
        cleaned_symbol = match.group(1).strip()
    else:
        cleaned_symbol = symbol.strip()
    
    # 处理泛型形式: <arrow2::io::parquet::read::deserialize::primitive::basic::Iter<T,I,P,F> as core::iter::traits::iterator::Iterator>::next
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
                    return first_part
    
    # 处理普通形式: tokio::runtime::task::raw::poll
    if '::' in cleaned_symbol:
        # 提取第一个单词（直到第一个::）
        first_part = cleaned_symbol.split('::')[0].strip()
        # 检查是否是有效的Rust标识符
        if re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', first_part):
            return first_part
    
    # 不是Rust函数形式
    return None

def get_category(symbol: str, self_percent, period) -> str:
    """
    根据函数符号判断类别
    
    规则:
    1. 函数名前缀是[k]的接口被认为是[kernel.kallsyms]这个类别
    2. 函数名为pthread_mutex_trylock的被认为是Libc
    3. 函数名为syscall被认为是Syscall
    4. 函数名用_rjem、do_rallocx、tcache_开头的，如_rjem_sdallocx被认为是Rust Jem
    5. 函数名用0x开头的，如Bare address被认为是Bare address
    6. Rust函数：按照规则提取库名（第一个单词）作为类别
    7. 如果以上都不匹配，使用Shared Object作为类别
    
    Args:
        symbol: 函数符号
        self_percent: Self百分比
        period: Period值
        
    Returns:
        类别字符串
    """
    # 首先尝试提取Rust库名
    rust_library = extract_rust_library_name(symbol)
    if rust_library:
        if 'daft' in rust_library:
            return 'daft'
        if 'parquet' in rust_library:
            return 'parquet'
        if 'arrow' in rust_library:
            return 'arrow2'
        if 'pyo3' in rust_library:
            return 'pyo3'
        if 'tokio' in rust_library:
            return 'tokio'
        if 'parking_lot' in rust_library:
            return 'parking_lot'
        return rust_library
    
    # 清理符号，去除[.]、[k]等前缀
    match = re.match(r'^\[[^\]]+\]\s*(.+)$', symbol)
    if match:
        cleaned_symbol = match.group(1).strip()
        prefix = symbol.split(']')[0] + ']'
    else:
        cleaned_symbol = symbol.strip()
        prefix = ""
    
    # 规则1: 检查是否以[k]开头
    if prefix and prefix.lower() == '[k]':
        return 'kernel'
    
    # 规则2: 检查特定函数名
    if cleaned_symbol == 'pthread_mutex_trylock':
        return 'libc'
    
    if cleaned_symbol == 'syscall':
        return 'Syscall'
    
    # 规则3: 检查以特定前缀开头
    if (cleaned_symbol.startswith('_rjem') or 
        cleaned_symbol.startswith('do_rallocx') or 
        cleaned_symbol.startswith('tcache_')):
        return 'Rust Jem'
    
    # 规则4: 检查以0x开头（裸地址）
    if cleaned_symbol.startswith('0x'):
        return 'libc'
    
    # # 规则5: 检查是否为其他常见libc函数
    # libc_functions = ['malloc', 'free', 'calloc', 'realloc', 'memcpy', 'memset', 'strlen', 'strcpy', 
    #                   'pthread_', 'fopen', 'fclose', 'read', 'write', 'open', 'close']
    # for func in libc_functions:
    #     if func in cleaned_symbol.lower():
    #         return 'Libc'
    
    # 规则6: 检查是否为系统调用
    syscall_patterns = ['sys_', 'do_syscall', '__x64_sys', '__ia32_sys']
    for pattern in syscall_patterns:
        if pattern in cleaned_symbol:
            return 'Syscall'
    
    # 规则7: 从symbol中提取Shared Object信息
    # 尝试从symbol字符串中提取DSO（共享对象）
    # 格式通常是: [.] function_name  libc.so.6
    # 或者: [k] function_name  [kernel.kallsyms]
    
    # 使用正则表达式提取可能的DSO
    dso_match = re.search(r'\s{2,}(.+)$', symbol.strip())
    if dso_match:
        dso = dso_match.group(1).strip()
        # 如果DSO是[kernel.kallsyms]，则返回该类别
        if dso == '[kernel.kallsyms]':
            return 'kernel'
        if 'libc' in dso:
            return 'libc'
        if 'python' in dso:
            return 'Python'
        if 'libm' in dso:
            return 'libm'
        if 'daft' in dso:
            return 'daft'
        # 否则，使用DSO作为类别
        return dso
    
    # 规则8: 根据symbol内容判断
    symbol_lower = symbol.lower()
    
    # 检查是否为Python相关
    if 'python' in symbol_lower:
        return 'Python'
    
    # 检查是否为libcrypto相关
    if 'libcrypto' in symbol_lower:
        return 'Libcrypto'
    
    # 检查是否为ld-linux相关
    if 'ld-linux' in symbol_lower:
        return 'ELF interpreter'
    
    # 检查是否为libc相关
    if 'libc' in symbol_lower:
        return 'Libc'
    
    # 检查是否为daft相关
    if 'daft' in symbol_lower:
        return 'daft'
    
    # 检查是否为snap相关
    if 'snap' in symbol_lower:
        return 'Snap'
    
    # 检查是否为parquet相关
    if 'parquet' in symbol_lower:
        return 'Parquet'
    
    # 检查是否为arrow相关
    if 'arrow' in symbol_lower:
        return 'Arrow'
    
    # 检查是否为hashbrown相关
    if 'hashbrown' in symbol_lower:
        return 'Hashbrown'
    
    # 检查是否为alloc相关
    if 'alloc' in symbol_lower:
        return 'Alloc'
    
    # 检查是否为tokio相关
    if 'tokio' in symbol_lower:
        return 'Tokio'
    
    # 检查是否为core相关
    if 'core' in symbol_lower:
        return 'Rust Core'
    
    # 如果以上都不匹配，返回Unknown
    print(f"未匹配到类别: {symbol}, 占比 {self_percent:.5%}")
    return 'Unknown'

def parse_perf_symbols(filename):
    print(f"正在处理: {filename} ...")
    cmd = [
        "perf", "report", "-i", filename, "--stdio", "-g", "none", "--show-total-period", "--sort", "pid,symbol,dso"
    ]
    try:
        result = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True)
    except subprocess.CalledProcessError as e:
        print(f"  [Error] 无法解析 {filename}: {e}")
        return None

    # 存储结果：{(Command, Symbol): [Self_Total, Period_Total]}
    data_map = {}

    # 正则表达式说明：
    # ^\s+           : 匹配行首空格
    # ([\d\.]+)%+    : 组1 - Children %
    # \s+([\d\.]+)%+ : 组2 - Self %
    # \s+(\d+)       : 组3 - Period
    # \s+\d+:(.+?)   : 组4 - Command (排除 PID)
    # \s+(.+)$       : 组5 - Symbol (直到行尾)
    pattern = re.compile(r"^\s+[\d\.]+%+\s+([\d\.]+)%+\s+(\d+)\s+\d+:([^\s]+)\s+(.+)$")

    lines = result.splitlines()
    for line in lines:
        # 跳过注释行和空行
        if not line.strip() or line.strip().startswith("#") or line.strip().startswith("."):
            continue
            
        match = pattern.match(line)
        if match:
            self_percent = float(match.group(1))
            period = int(match.group(2))
            symbol = match.group(4).strip()
            # 过滤条件：只统计 period > 0 且 self_percent > 0 的数据
            if period > 0 and self_percent > 0:
                classify = get_category(symbol, self_percent, period)
                key = (symbol, classify)
                if key not in data_map:
                    data_map[key] = [0.0, 0]
                data_map[key][0] += self_percent
                data_map[key][1] += period

    # 转换为列表并按 Self 占比降序排列
    sorted_data = []
    for (symbol, classify), values in data_map.items():
        sorted_data.append({
            "File": filename,
            "Symbol": symbol,
            "Self %": round(values[0], 2),
            "Period": values[1],
            "Classify": classify,
        })
    
    return sorted(sorted_data, key=lambda x: x["Self %"], reverse=True)

def main():
    data_files = glob.glob("*.data")
    if not data_files:
        print("未发现 .data 文件。")
        return

    all_results = []
    for f in data_files:
        res = parse_perf_symbols(f)
        if res:
            all_results.extend(res)

    # 写入 CSV
    fields = ["File", "Self %", "Period", "Classify", "Symbol"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(all_results)

    print(f"\n解析完成！结果已存入: {OUTPUT_CSV}")

    if not Path(INPUT_CSV).exists():
        print(f"找不到输入文件: {INPUT_CSV}")
        return

    # 1. 读取并聚合（不再区分 File）
    aggregated_data, total_period, rows, total_system_period = read_and_aggregate_csv(INPUT_CSV)
    
    if not aggregated_data:
        print("未解析到有效数据")
        return

    # 2. 生成报告内容
    md_content = generate_markdown(aggregated_data, total_period, rows, total_system_period)
    
    # 3. 保存
    Path(OUTPUT_MD).write_text(md_content, encoding="utf-8")
    print(f"Markdown 报告已生成: {OUTPUT_MD}")


if __name__ == "__main__":
    main()