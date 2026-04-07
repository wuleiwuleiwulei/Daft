#!/bin/bash

# ========== 配置区域 ==========
# 放置perf数据的路径
TARGET_DIR="/home/Daft/benchmarking/vllm/vllm_benchmark_runs_data_20260201_045412/run_003/no_binding/perf_data"

# FlameGraph工具路径（请根据实际情况修改）
FLAMEGRAPH_PATH="./FlameGraph"

# 创建输出目录结构
OUTPUT_DIR="perf_report"
# ===============================

# 检查perf命令是否可用
if ! command -v perf &> /dev/null; then
    echo "错误: perf命令未找到，请安装perf工具"
    exit 1
fi

# 检查目标目录是否存在
if [ ! -d "$TARGET_DIR" ]; then
    echo "错误: 找不到目标目录 '$TARGET_DIR'"
    echo "请检查文件夹结构"
    exit 1
fi

# 切换到目标目录
cd "$TARGET_DIR" || exit 1
echo "切换到目录: $(pwd)"

# 检查FlameGraph工具是否可用
if [ ! -d "$FLAMEGRAPH_PATH" ]; then
    echo "警告: FlameGraph工具路径 '$FLAMEGRAPH_PATH' 不存在"
    echo "将跳过火焰图生成"
    GENERATE_FLAMEGRAPHS=false
else
    # 检查必要的FlameGraph脚本
    if [ ! -f "$FLAMEGRAPH_PATH/stackcollapse-perf.pl" ] || [ ! -f "$FLAMEGRAPH_PATH/flamegraph.pl" ]; then
        echo "警告: FlameGraph工具不完整，缺少必要的脚本"
        echo "将跳过火焰图生成"
        GENERATE_FLAMEGRAPHS=false
    else
        GENERATE_FLAMEGRAPHS=true
        echo "FlameGraph工具检测成功，将生成火焰图"
    fi
fi

# 获取perf_*.data文件列表
echo "正在查找perf_*.data文件..."
DATA_FILES=($(find . -maxdepth 1 -name "perf_*.data" -type f | sort))

if [ ${#DATA_FILES[@]} -eq 0 ]; then
    echo "错误: 未找到perf_*.data文件"
    exit 1
fi

echo "找到 ${#DATA_FILES[@]} 个data文件:"
for data_file in "${DATA_FILES[@]}"; do
    echo "  - $(basename "$data_file")"
done

TEXT_REPORTS_DIR="$OUTPUT_DIR/text_reports"
FLAMEGRAPHS_DIR="$OUTPUT_DIR/flamegraphs"
TEMP_DIR="$OUTPUT_DIR/temp"

mkdir -p "$TEXT_REPORTS_DIR"
mkdir -p "$FLAMEGRAPHS_DIR"
mkdir -p "$TEMP_DIR"

echo "创建输出目录结构:"
echo "  文本报告目录: $TEXT_REPORTS_DIR"
echo "  火焰图目录: $FLAMEGRAPHS_DIR"
echo "  临时目录: $TEMP_DIR"

# 处理每个data文件
PROCESSED_COUNT=0
FAILED_FILES=()
FLAMEGRAPH_FAILED_FILES=()

for data_file in "${DATA_FILES[@]}"; do
    # 获取文件名（不带路径）
    DATA_FILENAME=$(basename "$data_file")
    # 提取基础名称（去掉扩展名）
    BASE_NAME="${DATA_FILENAME%.data}"
    
    echo ""
    echo "正在处理: $DATA_FILENAME"
    echo "  基础名称: $BASE_NAME"
    
    # 1. 生成文本版perf报告
    echo "  1. 生成文本报告..."
    
    # 定义输出文件名
    TXT_FILE="$TEXT_REPORTS_DIR/${BASE_NAME}.txt"
    NOSORT_FILE="$TEXT_REPORTS_DIR/${BASE_NAME}_nosort.txt"
    
    # 生成排序版perf报告
    echo "    生成排序版报告: $(basename "$TXT_FILE")"
    if perf report -i "$data_file" --stdio -g fractal,0.01 \
           --show-total-period --sort=dso > "$TXT_FILE" 2>&1; then
        TXT_SIZE=$(stat -c%s "$TXT_FILE" 2>/dev/null || echo "未知")
        echo "    ✓ 排序版报告生成成功 (${TXT_SIZE}字节)"
    else
        echo "    ✗ 排序版报告生成失败"
        FAILED_FILES+=("$DATA_FILENAME (排序版)")
    fi
    
    # 生成非排序版perf报告
    echo "    生成非排序版报告: $(basename "$NOSORT_FILE")"
    if perf report -i "$data_file" --stdio \
           --show-total-period > "$NOSORT_FILE" 2>&1; then
        NOSORT_SIZE=$(stat -c%s "$NOSORT_FILE" 2>/dev/null || echo "未知")
        echo "    ✓ 非排序版报告生成成功 (${NOSORT_SIZE}字节)"
    else
        echo "    ✗ 非排序版报告生成失败"
        FAILED_FILES+=("$DATA_FILENAME (非排序版)")
    fi
    
    # 2. 生成火焰图（如果FlameGraph工具可用）
    if [ "$GENERATE_FLAMEGRAPHS" = true ]; then
        echo "  2. 生成火焰图..."
        
        # 创建临时工作目录
        TEMP_WORK_DIR="$TEMP_DIR/$BASE_NAME"
        mkdir -p "$TEMP_WORK_DIR"
        
        # 定义中间文件和最终输出文件
        PERF_SCRIPT_FILE="$TEMP_WORK_DIR/${BASE_NAME}.perf"
        FOLDED_FILE="$TEMP_WORK_DIR/${BASE_NAME}.folded"
        SVG_FILE="$FLAMEGRAPHS_DIR/${BASE_NAME}.svg"
        
        # 步骤1: perf.data -> 文本栈
        echo "    步骤1: 生成perf脚本输出..."
        if perf script -i "$data_file" > "$PERF_SCRIPT_FILE" 2>&1; then
            PERF_SCRIPT_SIZE=$(stat -c%s "$PERF_SCRIPT_FILE" 2>/dev/null || echo "未知")
            echo "    ✓ perf脚本输出生成成功 (${PERF_SCRIPT_SIZE}字节)"
        else
            echo "    ✗ perf脚本输出生成失败"
            FLAMEGRAPH_FAILED_FILES+=("$DATA_FILENAME (步骤1)")
            continue
        fi
        
        # 步骤2: 折叠栈
        echo "    步骤2: 折叠栈..."
        if "$FLAMEGRAPH_PATH/stackcollapse-perf.pl" "$PERF_SCRIPT_FILE" > "$FOLDED_FILE" 2>&1; then
            FOLDED_SIZE=$(stat -c%s "$FOLDED_FILE" 2>/dev/null || echo "未知")
            echo "    ✓ 栈折叠成功 (${FOLDED_SIZE}字节)"
        else
            echo "    ✗ 栈折叠失败"
            FLAMEGRAPH_FAILED_FILES+=("$DATA_FILENAME (步骤2)")
            continue
        fi
        
        # 步骤3: 生成火焰图
        echo "    步骤3: 生成SVG火焰图..."
        if "$FLAMEGRAPH_PATH/flamegraph.pl" "$FOLDED_FILE" > "$SVG_FILE" 2>&1; then
            SVG_SIZE=$(stat -c%s "$SVG_FILE" 2>/dev/null || echo "未知")
            echo "    ✓ 火焰图生成成功 (${SVG_SIZE}字节)"
            echo "    ✓ 火焰图保存到: $(basename "$SVG_FILE")"
        else
            echo "    ✗ 火焰图生成失败"
            FLAMEGRAPH_FAILED_FILES+=("$DATA_FILENAME (步骤3)")
            continue
        fi
        
        # 清理临时文件
        rm -rf "$TEMP_WORK_DIR"
        echo "    ✓ 清理临时文件"
    else
        echo "  2. 跳过火焰图生成 (FlameGraph工具不可用)"
    fi
    
    PROCESSED_COUNT=$((PROCESSED_COUNT + 1))
done

# 清理临时目录
rm -rf "$TEMP_DIR"

# 输出处理结果摘要
echo ""
echo "==================== 处理完成 ===================="
echo "总计处理文件数: $PROCESSED_COUNT/${#DATA_FILES[@]}"
echo ""

mkdir -p "$TEXT_REPORTS_DIR/xlsx" 

# 开始生成perf的xlsx报告
python /home/Daft/benchmarking/alltest/perf_xlsx.py -i "$TEXT_REPORTS_DIR" -o "$TEXT_REPORTS_DIR/xlsx"