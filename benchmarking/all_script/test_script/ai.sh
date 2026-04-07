#!/bin/bash

# ========== 用户配置区域 ==========
# 设置基准测试根目录 (请根据实际情况修改)
# BENCHMARK_ROOT="/home/Daft/benchmarking/ai"
# SCRIPT_PATH="/home/Daft/benchmarking/all_script/test_script"
# ========== 参数检查 ==========
if [ $# -ne 2 ]; then
    echo "用法: $0 <BENCHMARK_ROOT> <SCRIPT_PATH>"
    exit 1
fi

BENCHMARK_ROOT="$1"
SCRIPT_PATH="$2"
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# 验证目录是否存在
if [ ! -d "$BENCHMARK_ROOT" ]; then
    echo "错误：BENCHMARK_ROOT 目录不存在: $BENCHMARK_ROOT"
    exit 1
fi

if [ ! -d "$SCRIPT_PATH" ]; then
    echo "错误：SCRIPT_PATH 目录不存在: $SCRIPT_PATH"
    exit 1
fi



# 基准测试配置
PERF_FREQ=249

# 测试循环次数
RUN_TIMES=5

# 定义要测试的AI应用目录和对应的主脚本
# 格式: "目录名称:主脚本名称"
AI_BENCHMARKS=(
    "audio_transcription:daft_main.py"
    "document_embedding:daft_main.py"
    "image_classification:daft_main.py"
    "video_object_detection:daft_main.py"
)

# 定义CPU绑定配置 (格式: "配置名称:绑核命令前缀:num_cpus参数值:CPU核心范围")
ARCH=$(uname -m)
CPU_COUNT=$(nproc --all)
echo "检测系统信息:"
echo "  架构: $ARCH"
echo "  CPU核心数: $CPU_COUNT"

# 根据架构设置绑核配置
if [ "$ARCH" = "x86_64" ]; then
    echo "  x86_64架构检测到"
    CPU_BINDING_CONFIGS=(
        # "single_core:numactl --cpunodebind=0 --membind=0 taskset -c 192:--num_cpus 1:CPU192"
        "multi_core:numactl --cpunodebind=2 --membind=2 taskset -c 48-71::CPU48-71"
        # "no_binding:::无"  # 空字符串表示不绑核，也不添加--num_cpus参数
    )
elif [ "$ARCH" = "aarch64" ]; then
    echo "  ARM64架构检测到"
    CPU_BINDING_CONFIGS=(
        # "single_core:numactl --cpunodebind=2 --membind=2 taskset -c 80:--num_cpus 1:CPU80"
        "multi_core:numactl --cpunodebind=2 --membind=2 taskset -c 160-183::CPU160-183"
        # "no_binding:::无"  # 空字符串表示不绑核，也不添加--num_cpus参数
    )
fi

# 基础输出目录
BASE_OUTPUT_DIR="$BENCHMARK_ROOT/ai_benchmark_runs_$(date +%Y%m%d_%H%M%S)"
BASE_OUTPUT_DIR_DATA="$BENCHMARK_ROOT/ai_benchmark_runs_data_$(date +%Y%m%d_%H%M%S)"
# ================================

echo "开始 Daft AI 基准测试..."
echo "总计划：运行 $RUN_TIMES 轮，每轮测试 ${#AI_BENCHMARKS[@]} 个AI应用，"
echo "每个应用测试 ${#CPU_BINDING_CONFIGS[@]} 种CPU绑定配置。"
echo "所有结果将保存在: $BASE_OUTPUT_DIR/"
echo "注意：完整运行可能耗时较长。"
echo "========================================="
sleep 3

# 主运行循环
for ((run=1; run<=RUN_TIMES; run++)); do
    RUN_DIR=$(printf "%s/run_%03d" "$BASE_OUTPUT_DIR" "$run")
    RUN_DIR_DATA=$(printf "%s/run_%03d" "$BASE_OUTPUT_DIR_DATA" "$run")
    echo ""
    echo ">>>>>> 开始第 $run/$RUN_TIMES 轮运行 <<<<<<"
    echo "结果目录: $RUN_DIR"
    
    # 针对每个AI应用进行测试
    for benchmark in "${AI_BENCHMARKS[@]}"; do
        # 解析AI应用信息
        IFS=':' read -r dir_name main_script <<< "$benchmark"
        BENCH_DIR="$BENCHMARK_ROOT/$dir_name"
        
        echo ""
        echo "  ==== AI应用: $dir_name ===="
        echo "  工作目录: $BENCH_DIR"
        
        # 检查目录是否存在
        if [ ! -d "$BENCH_DIR" ]; then
            echo "  警告: 目录 $BENCH_DIR 不存在，跳过此应用"
            continue
        fi
        
        # 针对每种CPU绑定配置进行测试
        for binding_config in "${CPU_BINDING_CONFIGS[@]}"; do
            # 解析配置信息 (格式: "配置名称:绑核命令前缀:num_cpus参数值:CPU核心范围")
            IFS=':' read -r config_name bind_prefix num_cpus_arg cpu_range <<< "$binding_config"
            CONFIG_DIR="$RUN_DIR/$config_name"
            CONFIG_DIR_DATA="$RUN_DIR_DATA/$config_name"

            echo ""
            echo "    ==== CPU绑定配置: $config_name ===="
            echo "    绑核命令: $bind_prefix"
            echo "    --num_cpus 参数: $num_cpus_arg"
            echo "    CPU核心范围: $cpu_range"
            
            # 创建本配置的输出目录
            # mkdir -p "$CONFIG_DIR/perf_data"
            mkdir -p "$CONFIG_DIR/perf_results"
            mkdir -p "$CONFIG_DIR/perf_logs"
            mkdir -p "$CONFIG_DIR/noperf_logs"
            mkdir -p "$CONFIG_DIR/xlsx"
            mkdir -p "$CONFIG_DIR_DATA/perf_data"
            
            # 构建测试名称
            test_name="${dir_name}_${config_name}"
            
            # 1. 首先运行 perf 版本测试
            echo "      运行 perf 版本测试..."
            
            # 构建输出文件名
            # PERF_DATA_FILE="$CONFIG_DIR/perf_data/perf_${test_name}.data"
            PERF_DATA_FILE="$CONFIG_DIR_DATA/perf_data/perf_${test_name}.data"
            PERF_REPORT_FILE="$CONFIG_DIR/perf_results/perf_${test_name}.txt"
            PERF_REPORT_NOSORT_FILE="$CONFIG_DIR/perf_results/perf_${test_name}_nosort.txt"
            PERF_LOG_FILE="$CONFIG_DIR/perf_logs/${test_name}_output_perf.log"  # perf命令行输出
            
            # 进入测试目录
            cd "$BENCH_DIR" || { echo "无法切换到目录 $BENCH_DIR"; continue; }
            echo "      当前目录: $(pwd)"
            
            # 构建命令
            BASE_CMD="python -X perf $main_script"
            
            # 如果有--num_cpus参数，添加到命令中
            if [ -n "$num_cpus_arg" ]; then
                BASE_CMD="$BASE_CMD $num_cpus_arg"
            fi
            
            echo "        执行命令: $bind_prefix perf record -F $PERF_FREQ -g --call-graph dwarf -o $PERF_DATA_FILE $BASE_CMD"
            echo "        命令行输出将保存到: $PERF_LOG_FILE"
            
            # 记录开始时间
            echo "=== AI应用测试开始: $(date) ===" > "$PERF_LOG_FILE"
            echo "AI应用: $dir_name" >> "$PERF_LOG_FILE"
            echo "工作目录: $(pwd)" >> "$PERF_LOG_FILE"
            echo "主脚本: $main_script" >> "$PERF_LOG_FILE"
            echo "CPU绑定配置: $config_name" >> "$PERF_LOG_FILE"
            echo "绑核命令: $bind_prefix" >> "$PERF_LOG_FILE"
            echo "--num_cpus 参数: $num_cpus_arg" >> "$PERF_LOG_FILE"
            echo "CPU核心范围: $cpu_range" >> "$PERF_LOG_FILE"
            echo "命令: perf record -F $PERF_FREQ -g --call-graph dwarf -o $PERF_DATA_FILE $BASE_CMD" >> "$PERF_LOG_FILE"
            echo "==========================================" >> "$PERF_LOG_FILE"
            
            # 使用绑核命令和perf记录运行性能数据
            if [ -n "$bind_prefix" ]; then
                # 有绑核命令
                $bind_prefix perf record \
                    -F $PERF_FREQ \
                    -g \
                    -e cpu-clock \
                    --call-graph fp \
                    -o "$PERF_DATA_FILE" \
                    $BASE_CMD >> "$PERF_LOG_FILE" 2>&1
            else
                # 无绑核命令
                perf record \
                    -F $PERF_FREQ \
                    -g \
                    -e cpu-clock \
                    --call-graph fp \
                    -o "$PERF_DATA_FILE" \
                    $BASE_CMD >> "$PERF_LOG_FILE" 2>&1
            fi
            
            PERF_EXIT_CODE=$?
            echo "==========================================" >> "$PERF_LOG_FILE"
            echo "=== 测试结束: $(date) ===" >> "$PERF_LOG_FILE"
            echo "退出码: $PERF_EXIT_CODE" >> "$PERF_LOG_FILE"
            
            # 2. 运行 noperf (无性能分析) 版本测试
            echo "      运行 noperf (无性能分析) 版本测试..."
            
            OUTPUT_LOG_FILE="$CONFIG_DIR/noperf_logs/${test_name}_output_noperf.log"  # 命令行输出
            
            # 构建命令
            NOPERF_CMD="python $main_script"
            
            # 如果有--num_cpus参数，添加到命令中
            if [ -n "$num_cpus_arg" ]; then
                NOPERF_CMD="$NOPERF_CMD $num_cpus_arg"
            fi
            
            echo "        执行命令: $bind_prefix $NOPERF_CMD"
            echo "        命令行输出将保存到: $OUTPUT_LOG_FILE"
            
            # 记录开始时间
            echo "=== AI应用测试开始: $(date) ===" > "$OUTPUT_LOG_FILE"
            echo "AI应用: $dir_name" >> "$OUTPUT_LOG_FILE"
            echo "工作目录: $(pwd)" >> "$OUTPUT_LOG_FILE"
            echo "主脚本: $main_script" >> "$OUTPUT_LOG_FILE"
            echo "CPU绑定配置: $config_name" >> "$OUTPUT_LOG_FILE"
            echo "绑核命令: $bind_prefix" >> "$OUTPUT_LOG_FILE"
            echo "--num_cpus 参数: $num_cpus_arg" >> "$OUTPUT_LOG_FILE"
            echo "CPU核心范围: $cpu_range" >> "$OUTPUT_LOG_FILE"
            echo "命令: $NOPERF_CMD" >> "$OUTPUT_LOG_FILE"
            echo "==========================================" >> "$OUTPUT_LOG_FILE"
            
            # 运行无perf的基准测试
            if [ -n "$bind_prefix" ]; then
                # 有绑核命令
                $bind_prefix $NOPERF_CMD >> "$OUTPUT_LOG_FILE" 2>&1
            else
                # 无绑核命令
                $NOPERF_CMD >> "$OUTPUT_LOG_FILE" 2>&1
            fi
            
            NOPERF_EXIT_CODE=$?
            echo "==========================================" >> "$OUTPUT_LOG_FILE"
            echo "=== 测试结束: $(date) ===" >> "$OUTPUT_LOG_FILE"
            echo "退出码: $NOPERF_EXIT_CODE" >> "$OUTPUT_LOG_FILE"
            
            # 根据退出码判断测试结果
            if [ $NOPERF_EXIT_CODE -eq 0 ]; then
                echo "        AI应用 $dir_name ($config_name) 运行成功"
            else
                echo "        警告: AI应用 $dir_name ($config_name) 运行失败，退出码: $NOPERF_EXIT_CODE"
                echo "        请检查 $OUTPUT_LOG_FILE"
            fi
            
            echo "      AI应用 $dir_name ($config_name) 完成"
            echo "      perf详细输出: $PERF_LOG_FILE"
            echo "      noperf详细输出: $OUTPUT_LOG_FILE"
            
            # 返回基准测试根目录
            cd "$BENCHMARK_ROOT" || echo "警告: 无法返回基准测试根目录"
        done
        
        echo "    AI应用 [$dir_name] 完成。"
    done
    
    echo ">>>>>> 第 $run/$RUN_TIMES 轮运行完成 <<<<<<"
    
    # 每轮运行后添加延迟
    if [ $run -lt $RUN_TIMES ]; then
        echo "等待2秒后开始下一轮..."
        sleep 1
    fi
done

echo ""
echo "========================================="
echo "所有AI基准测试运行完成！"
echo "结果保存在: $BASE_OUTPUT_DIR/"
echo "运行统计摘要:"
echo "总运行轮次: $RUN_TIMES"
echo "每轮AI应用数: ${#AI_BENCHMARKS[@]}"
echo "每应用绑定配置数: ${#CPU_BINDING_CONFIGS[@]}"
echo "总测试执行次数: $((RUN_TIMES * ${#AI_BENCHMARKS[@]} * ${#CPU_BINDING_CONFIGS[@]} * 2))"
echo "========================================="

# 以下开始得出对应的perf xlsx表格和耗时数据表格
python "$SCRIPT_PATH/log_collect_name.py" "$BASE_OUTPUT_DIR" -o "$BASE_OUTPUT_DIR/ai_all_times.xlsx"

python "$SCRIPT_PATH/log_collect_name_case.py" "$BASE_OUTPUT_DIR" -o "$BASE_OUTPUT_DIR/ai_case_times.xlsx"

python "$SCRIPT_PATH/xlsx_process.py" -i "$BASE_OUTPUT_DIR/ai_all_times.xlsx" -o "$BASE_OUTPUT_DIR" -s "multi_core_perf"


# ========== 用户配置区域 ==========
# 测试耗时数据表格的路径
TIME_DATA_PATH="$BASE_OUTPUT_DIR"
# 所有轮次perf数据的路径
PERF_DATA_PATH="$BASE_OUTPUT_DIR_DATA"
# 输出txt文件的路径
OUTPUT_PATH="$BASE_OUTPUT_DIR/ai_perf"

# 需要处理的perf数据文件前缀模式（可修改）
PERF_FILE_PATTERN="perf_*.data"
# ================================

echo "开始处理 perf 数据..."
echo "测试耗时数据路径: $TIME_DATA_PATH"
echo "Perf数据路径: $PERF_DATA_PATH"
echo "输出路径: $OUTPUT_PATH"
echo "========================================="

# 1. 创建输出目录
mkdir -p "$OUTPUT_PATH"
if [ $? -ne 0 ]; then
    echo "错误: 无法创建输出目录 $OUTPUT_PATH"
    exit 1
fi

echo "输出目录已创建/已存在: $OUTPUT_PATH"

# 2. 查找并提取 run_xxx.xlsx 文件名中的 run_xxx
echo ""
echo "正在查找 Excel 表格文件..."
EXCEL_FILES=($(find "$TIME_DATA_PATH" -name "run_*.xlsx" -type f | head -1))

if [ ${#EXCEL_FILES[@]} -eq 0 ]; then
    echo "警告: 在 $TIME_DATA_PATH 中未找到任何 run_*.xlsx 文件"
    echo "请手动输入要处理的轮次文件夹名称 (如 run_001):"
    read -r RUN_FOLDER_NAME
    RUN_NAME="$RUN_FOLDER_NAME"
else
    # 只取第一个找到的文件
    EXCEL_FILE="${EXCEL_FILES[0]}"
    echo "找到 Excel 文件: $EXCEL_FILE"
    
    # 提取文件名（不带路径）
    EXCEL_BASENAME=$(basename "$EXCEL_FILE")
    echo "Excel 文件名: $EXCEL_BASENAME"
    
    # 提取 run_xxx 部分（去掉 .xlsx 扩展名）
    RUN_NAME="${EXCEL_BASENAME%.xlsx}"
    echo "提取的轮次名称: $RUN_NAME"
fi

echo ""

# 3. 检查 perf 数据路径中对应的轮次文件夹
RUN_PERF_PATH="$PERF_DATA_PATH/$RUN_NAME/multi_core/perf_data"
echo "正在检查 perf 数据路径: $RUN_PERF_PATH"

if [ ! -d "$RUN_PERF_PATH" ]; then
    echo "错误: 找不到 perf 数据目录 $RUN_PERF_PATH"
    echo "可用的轮次目录:"
    find "$PERF_DATA_PATH" -maxdepth 1 -type d -name "run_*" | sort
    exit 1
fi

# 4. 查找 perf data 文件
echo "正在查找 perf 数据文件..."
PERF_FILES=($(find "$RUN_PERF_PATH" -name "$PERF_FILE_PATTERN" -type f | sort))

if [ ${#PERF_FILES[@]} -eq 0 ]; then
    echo "警告: 在 $RUN_PERF_PATH 中未找到任何 $PERF_FILE_PATTERN 文件"
    echo "当前目录内容:"
    ls -la "$RUN_PERF_PATH"
    exit 1
fi

echo "找到 ${#PERF_FILES[@]} 个 perf 数据文件"
echo ""

# 5. 为每个 perf data 文件生成报告
PROCESSED_COUNT=0
ERROR_COUNT=0

for perf_file in "${PERF_FILES[@]}"; do
    # 提取文件名（不带路径）
    PERF_BASENAME=$(basename "$perf_file")
    # 提取基础名称（不带扩展名）
    PERF_BASE="${PERF_BASENAME%.data}"
    
    echo "处理文件: $PERF_BASENAME"
    echo "  完整路径: $perf_file"
    
    # 构建输出文件名
    SORTED_OUTPUT="$OUTPUT_PATH/${PERF_BASE}.txt"
    NOSORT_OUTPUT="$OUTPUT_PATH/${PERF_BASE}_nosort.txt"
    
    echo "  排序版输出: $(basename "$SORTED_OUTPUT")"
    echo "  非排序版输出: $(basename "$NOSORT_OUTPUT")"
    
    # 检查 perf 文件是否存在且可读
    if [ ! -r "$perf_file" ]; then
        echo "  错误: 文件不可读或不存在"
        ((ERROR_COUNT++))
        continue
    fi
    
    # 获取文件大小（用于进度显示）
    FILE_SIZE=$(stat -c%s "$perf_file" 2>/dev/null || echo "未知")
    echo "  文件大小: $FILE_SIZE 字节"
    
    # 记录开始时间
    START_TIME=$(date +%s)
    
    # 生成排序版 perf 报告
    SORTED_EXIT_CODE=0
    
    # 生成非排序版 perf 报告
    echo "  生成非排序版报告..."
    perf report -i "$perf_file" --stdio -g \
        --show-total-period > "$NOSORT_OUTPUT" 2>&1
    NOSORT_EXIT_CODE=$?
    
    # 记录结束时间
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    
    # 检查输出文件是否生成
    if [ $SORTED_EXIT_CODE -eq 0 ] && [ $NOSORT_EXIT_CODE -eq 0 ]; then
        if [ -s "$SORTED_OUTPUT" ] && [ -s "$NOSORT_OUTPUT" ]; then
            echo "  ✓ 成功生成报告 (耗时: ${DURATION}秒)"
            ((PROCESSED_COUNT++))
            
            # 获取生成的报告文件大小
            SORTED_SIZE=$(stat -c%s "$SORTED_OUTPUT" 2>/dev/null || echo "未知")
            NOSORT_SIZE=$(stat -c%s "$NOSORT_OUTPUT" 2>/dev/null || echo "未知")
            echo "    排序版大小: $SORTED_SIZE 字节"
            echo "    非排序版大小: $NOSORT_SIZE 字节"
        else
            echo "  ✗ 错误: 生成的报告文件为空"
            ((ERROR_COUNT++))
        fi
    else
        echo "  ✗ 错误: perf 命令执行失败"
        echo "    排序版退出码: $SORTED_EXIT_CODE"
        echo "    非排序版退出码: $NOSORT_EXIT_CODE"
        ((ERROR_COUNT++))
    fi
    
    echo ""
done

# 6. 生成处理摘要
echo "========================================="
echo "处理完成！"
echo ""
echo "处理摘要:"
echo "  - 轮次名称: $RUN_NAME"
echo "  - 查找的Excel文件: ${EXCEL_FILE:-手动输入}"
echo "  - Perf数据目录: $RUN_PERF_PATH"
echo "  - 输出目录: $OUTPUT_PATH"
echo "  - 找到的perf文件数: ${#PERF_FILES[@]}"
echo "  - 成功处理: $PROCESSED_COUNT"
echo "  - 处理失败: $ERROR_COUNT"
echo ""

mkdir -p "$BASE_OUTPUT_DIR/ai_xlsx" 

# 开始生成perf的xlsx文件
python "$SCRIPT_PATH/perffuncnew_dir_type.py" -i "$OUTPUT_PATH" -o "$BASE_OUTPUT_DIR/ai_xlsx"

# 把文件移到同一个位置方便下载
mkdir -p "$SCRIPT_DIR/alltest_result/ai"

\cp -f "$BASE_OUTPUT_DIR/ai_all_times.xlsx" "$SCRIPT_DIR/alltest_result/ai"

\cp -f "$BASE_OUTPUT_DIR/ai_case_times.xlsx" "$SCRIPT_DIR/alltest_result/ai"

\cp -rf "$BASE_OUTPUT_DIR/ai_xlsx" "$SCRIPT_DIR/alltest_result/ai"

\cp -f "$EXCEL_FILE" "$SCRIPT_DIR/alltest_result/ai"