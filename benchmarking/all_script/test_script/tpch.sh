#!/bin/bash

# ========== 参数检查 ==========
if [ $# -ne 3 ]; then
    echo "用法: $0 <BENCHMARK_ROOT> <TPCH_GEN_FOLDER> <SCRIPT_PATH>"
    exit 1
fi

BENCHMARK_ROOT="$1"
TPCH_GEN_FOLDER="$2"
SCRIPT_PATH="$3"
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")

# 验证目录是否存在
if [ ! -d "$BENCHMARK_ROOT" ]; then
    echo "错误：BENCHMARK_ROOT 目录不存在: $BENCHMARK_ROOT"
    exit 1
fi

if [ ! -d "$TPCH_GEN_FOLDER" ]; then
    echo "错误：TPCH_GEN_FOLDER 目录不存在: $TPCH_GEN_FOLDER"
    exit 1
fi

if [ ! -d "$SCRIPT_PATH" ]; then
    echo "错误：SCRIPT_PATH 目录不存在: $SCRIPT_PATH"
    exit 1
fi

# 切换到基准目录
cd "$BENCHMARK_ROOT" || exit 1

# ========== 用户配置区域 ==========
export DAFT_RUNNER=native

# 后续可使用 $TPCH_GEN_FOLDER 和 $SCRIPT_PATH 变量
# ... 其他脚本内容保持不变

# #!/bin/bash

# cd /home/Daft/benchmarking/tpch
# BENCHMARK_ROOT="/home/Daft/benchmarking/tpch"
# # ========== 用户配置区域 ==========
# export DAFT_RUNNER=native
# TPCH_GEN_FOLDER="/home/data/tpch-dbgen"


PERF_FREQ=249
SCALE_FACTOR=100.0

# 测试循环次数
RUN_TIMES=5

CPU_CONFIGS=("num_cpus_40")

# 查询列表 (1到22)
QUERIES=$(seq 1 22)

# 基础输出目录
BASE_OUTPUT_DIR="$BENCHMARK_ROOT/tpch_benchmark_runs_$(date +%Y%m%d_%H%M%S)"
BASE_OUTPUT_DIR_DATA="$BENCHMARK_ROOT/tpch_benchmark_runs_data_$(date +%Y%m%d_%H%M%S)"
# ================================

echo "开始 TPC-H 大规模性能测试..."
echo "总计划：运行 $RUN_TIMES 轮，每轮测试 ${#CPU_CONFIGS[@]} 种CPU配置，每种配置运行 ${#QUERIES[@]} 个查询。"
echo "所有结果将保存在: $BASE_OUTPUT_DIR/"
echo "注意：完整运行可能耗时较长。"
echo "========================================="
sleep 3

# 创建主日志目录
MAIN_LOG_DIR="$BENCHMARK_ROOT/tpch_test_logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$MAIN_LOG_DIR"
OVERALL_LOG="$MAIN_LOG_DIR/overall_test.log"
SUMMARY_LOG="$MAIN_LOG_DIR/test_summary.csv"

# 初始化总日志和摘要文件
echo "=== TPC-H 基准测试开始: $(date) ===" > "$OVERALL_LOG"
echo "总运行轮次: $RUN_TIMES" >> "$OVERALL_LOG"
echo "CPU配置数: ${#CPU_CONFIGS[@]}" >> "$OVERALL_LOG"
echo "查询数量: ${#QUERIES[@]}" >> "$OVERALL_LOG"
echo "基础输出目录: $BASE_OUTPUT_DIR" >> "$OVERALL_LOG"
echo "数据文件目录: $BASE_OUTPUT_DIR_DATA" >> "$OVERALL_LOG"
echo "==========================================" >> "$OVERALL_LOG"

echo "运行轮次,CPU配置,查询号,测试类型,开始时间,结束时间,运行时长(秒),退出码,结果文件,日志文件" > "$SUMMARY_LOG"

# 主运行循环
for ((run=1; run<=RUN_TIMES; run++)); do
    RUN_DIR=$(printf "%s/run_%03d" "$BASE_OUTPUT_DIR" "$run")
    RUN_DIR_DATA=$(printf "%s/run_%03d" "$BASE_OUTPUT_DIR_DATA" "$run")
    
    echo ""
    echo ">>>>>> 开始第 $run/$RUN_TIMES 轮运行 <<<<<<"
    echo "结果目录: $RUN_DIR"
    
    # 记录到总日志
    echo "=== 第 $run/$RUN_TIMES 轮运行开始: $(date) ===" >> "$OVERALL_LOG"
    echo "结果目录: $RUN_DIR" >> "$OVERALL_LOG"
    echo "数据目录: $RUN_DIR_DATA" >> "$OVERALL_LOG"
    
    # 针对每种CPU配置进行测试
    for config in "${CPU_CONFIGS[@]}"; do
        CONFIG_DIR="$RUN_DIR/$config"
        CONFIG_DIR_DATA="$RUN_DIR_DATA/$config"
        echo ""
        echo "  ==== 配置: $config ===="
        
        # 记录到总日志
        echo "  === 配置: $config 开始 ===" >> "$OVERALL_LOG"
        echo "  配置目录: $CONFIG_DIR" >> "$OVERALL_LOG"

        ARCH=$(uname -m)
        CPU_COUNT=$(nproc --all)
        echo "检测系统信息:"
        echo "  架构: $ARCH"
        echo "  CPU核心数: $CPU_COUNT"

        # 根据架构设置绑核配置（条目名称必须与 case 中的 config 值一致）
        if [ "$ARCH" = "x86_64" ]; then
            BIND_SINGLE="nnumactl --cpunodebind=2 --membind=2 taskset -c 48"
            BIND_MULTI="numactl --cpunodebind=2 --membind=2 taskset -c 48-71"
        elif [ "$ARCH" = "aarch64" ]; then
            BIND_SINGLE="numactl --cpunodebind=2 --membind=2 taskset -c 160"
            BIND_MULTI="numactl --cpunodebind=2 --membind=2 taskset -c 160-183"
        fi
        
        case $config in
            "num_cpus_1")
                CPU_ARG=""
                BIND_PREFIX="$BIND_SINGLE"
                ;;
            "num_cpus_40")
                CPU_ARG=""
                BIND_PREFIX="$BIND_MULTI"
                ;;
            "no_cpus_arg")
                CPU_ARG=""
                BIND_PREFIX=""
                ;;
        esac

        # 创建本配置的perf和noperf结果目录
        mkdir -p "$CONFIG_DIR/perf_results"
        mkdir -p "$CONFIG_DIR/noperf_results"
        mkdir -p "$CONFIG_DIR/xlsx"
        mkdir -p "$CONFIG_DIR/perf_logs"
        mkdir -p "$CONFIG_DIR/noperf_logs"
        mkdir -p "$CONFIG_DIR_DATA"

        # 1. 首先运行所有22个查询的 perf 版本
        echo "    运行 perf 版本查询..."
        echo "    == 开始运行 perf 版本查询 ==" >> "$OVERALL_LOG"
        
        for i in $QUERIES; do
            # 构建输出文件名
            PERF_DATA_FILE="$CONFIG_DIR_DATA/perf_q${i}.data"
            PERF_REPORT_FILE="$CONFIG_DIR/perf_results/perf_q${i}.txt"
            PERF_REPORT_NOSORT_FILE="$CONFIG_DIR/perf_results/perf_q${i}_nosort.txt"
            OUTPUT_CSV_FILE="$CONFIG_DIR/perf_results/tpch_results${i}.csv"
            PERF_LOG_FILE="$CONFIG_DIR/perf_logs/q${i}_perf_output.log"
            
            echo "      查询 Q${i}..."
            echo "      === 查询 Q${i} perf测试开始: $(date) ===" >> "$OVERALL_LOG"
            echo "      数据文件: $PERF_DATA_FILE" >> "$OVERALL_LOG"
            echo "      日志文件: $PERF_LOG_FILE" >> "$OVERALL_LOG"
            
            # 记录开始时间
            START_TIME=$(date +%s)
            START_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
            
            # 记录到查询日志
            echo "=== TPC-H 查询 Q${i} perf测试开始: $START_TIMESTAMP ===" > "$PERF_LOG_FILE"
            echo "运行轮次: $run/$RUN_TIMES" >> "$PERF_LOG_FILE"
            echo "CPU配置: $config" >> "$PERF_LOG_FILE"
            echo "绑核命令: $BIND_PREFIX" >> "$PERF_LOG_FILE"
            echo "--num_cpus 参数: $CPU_ARG" >> "$PERF_LOG_FILE"
            echo "命令: perf record -F $PERF_FREQ -g --call-graph dwarf -o $PERF_DATA_FILE $BIND_PREFIX python -X perf __main__.py --tpch_gen_folder $TPCH_GEN_FOLDER --questions $i $CPU_ARG --output_csv $OUTPUT_CSV_FILE" >> "$PERF_LOG_FILE"
            echo "==========================================" >> "$PERF_LOG_FILE"
            
            # 使用 perf 记录运行性能数据（注意：绑核命令包裹了python命令）
            perf record \
                -F $PERF_FREQ \
                -g \
                -e cpu-clock \
                --call-graph dwarf \
                -o "$PERF_DATA_FILE" \
                $BIND_PREFIX python -X perf __main__.py \
                    --tpch_gen_folder "$TPCH_GEN_FOLDER" \
                    --skip_warmup \
                    --scale_factor "$SCALE_FACTOR" \
                    --questions "$i" \
                    $CPU_ARG \
                    --output_csv "$OUTPUT_CSV_FILE" >> "$PERF_LOG_FILE" 2>&1
            
            PERF_EXIT_CODE=$?
            
            # 记录结束时间
            END_TIME=$(date +%s)
            END_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
            DURATION=$((END_TIME - START_TIME))
            
            # 记录到查询日志
            echo "==========================================" >> "$PERF_LOG_FILE"
            echo "=== TPC-H 查询 Q${i} perf测试结束: $END_TIMESTAMP ===" >> "$PERF_LOG_FILE"
            echo "运行时长: ${DURATION}秒" >> "$PERF_LOG_FILE"
            echo "退出码: $PERF_EXIT_CODE" >> "$PERF_LOG_FILE"
            
            # 记录到总日志
            echo "      === 查询 Q${i} perf测试结束: $END_TIMESTAMP ===" >> "$OVERALL_LOG"
            echo "      运行时长: ${DURATION}秒" >> "$OVERALL_LOG"
            echo "      退出码: $PERF_EXIT_CODE" >> "$OVERALL_LOG"
            
            # 记录到摘要文件
            echo "$run,$config,Q${i},perf版本,$START_TIMESTAMP,$END_TIMESTAMP,$DURATION,$PERF_EXIT_CODE,$OUTPUT_CSV_FILE,$PERF_LOG_FILE" >> "$SUMMARY_LOG"
            
            echo "      查询 Q${i} perf测试完成 (退出码: $PERF_EXIT_CODE, 时长: ${DURATION}秒)"
        done
        
        echo "    == perf 版本查询完成 ==" >> "$OVERALL_LOG"
        
        # 2. 然后运行所有22个查询的 noperf (无性能分析) 版本
        echo ""
        echo "    运行 noperf (无性能分析) 版本查询..."
        echo "    == 开始运行 noperf 版本查询 ==" >> "$OVERALL_LOG"
        
        for i in $QUERIES; do
            OUTPUT_CSV_FILE="$CONFIG_DIR/noperf_results/tpch_results${i}.csv"
            NOPERF_LOG_FILE="$CONFIG_DIR/noperf_logs/q${i}_noperf_output.log"
            
            echo "      查询 Q${i}..."
            echo "      === 查询 Q${i} noperf测试开始: $(date) ===" >> "$OVERALL_LOG"
            echo "      输出文件: $OUTPUT_CSV_FILE" >> "$OVERALL_LOG"
            echo "      日志文件: $NOPERF_LOG_FILE" >> "$OVERALL_LOG"
            
            # 记录开始时间
            START_TIME=$(date +%s)
            START_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
            
            # 记录到查询日志
            echo "=== TPC-H 查询 Q${i} noperf测试开始: $START_TIMESTAMP ===" > "$NOPERF_LOG_FILE"
            echo "运行轮次: $run/$RUN_TIMES" >> "$NOPERF_LOG_FILE"
            echo "CPU配置: $config" >> "$NOPERF_LOG_FILE"
            echo "绑核命令: $BIND_PREFIX" >> "$NOPERF_LOG_FILE"
            echo "--num_cpus 参数: $CPU_ARG" >> "$NOPERF_LOG_FILE"
            echo "命令: $BIND_PREFIX python __main__.py --tpch_gen_folder $TPCH_GEN_FOLDER --questions $i $CPU_ARG --output_csv $OUTPUT_CSV_FILE" >> "$NOPERF_LOG_FILE"
            echo "==========================================" >> "$NOPERF_LOG_FILE"

            $BIND_PREFIX perf stat -e task-clock,cpu-clock,context-switches,cpu-migrations,page-faults,minor-faults,major-faults python -X perf __main__.py \
                --tpch_gen_folder "$TPCH_GEN_FOLDER" \
                --questions "$i" \
                --skip_warmup \
                --scale_factor "$SCALE_FACTOR" \
                $CPU_ARG \
                --output_csv "$OUTPUT_CSV_FILE" >> "$NOPERF_LOG_FILE" 2>&1

            NOPERF_EXIT_CODE=$?
            
            # 记录结束时间
            END_TIME=$(date +%s)
            END_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")
            DURATION=$((END_TIME - START_TIME))
            
            # 记录到查询日志
            echo "==========================================" >> "$NOPERF_LOG_FILE"
            echo "=== TPC-H 查询 Q${i} noperf测试结束: $END_TIMESTAMP ===" >> "$NOPERF_LOG_FILE"
            echo "运行时长: ${DURATION}秒" >> "$NOPERF_LOG_FILE"
            echo "退出码: $NOPERF_EXIT_CODE" >> "$NOPERF_LOG_FILE"
            
            # 检查CSV文件是否生成
            if [ -f "$OUTPUT_CSV_FILE" ] && [ -s "$OUTPUT_CSV_FILE" ]; then
                echo "        CSV结果文件已生成: $OUTPUT_CSV_FILE"
                echo "CSV结果文件已生成: $OUTPUT_CSV_FILE" >> "$NOPERF_LOG_FILE"
                echo "CSV文件大小: $(stat -c%s "$OUTPUT_CSV_FILE") 字节" >> "$NOPERF_LOG_FILE"
            else
                echo "        警告: CSV结果文件未生成或为空"
                echo "CSV结果文件未生成或为空" >> "$NOPERF_LOG_FILE"
            fi
            
            # 记录到总日志
            echo "      === 查询 Q${i} noperf测试结束: $END_TIMESTAMP ===" >> "$OVERALL_LOG"
            echo "      运行时长: ${DURATION}秒" >> "$OVERALL_LOG"
            echo "      退出码: $NOPERF_EXIT_CODE" >> "$OVERALL_LOG"
            
            # 记录到摘要文件
            echo "$run,$config,Q${i},noperf版本,$START_TIMESTAMP,$END_TIMESTAMP,$DURATION,$NOPERF_EXIT_CODE,$OUTPUT_CSV_FILE,$NOPERF_LOG_FILE" >> "$SUMMARY_LOG"
            
            echo "      查询 Q${i} noperf测试完成 (退出码: $NOPERF_EXIT_CODE, 时长: ${DURATION}秒)"
        done
        
        echo "    == noperf 版本查询完成 ==" >> "$OVERALL_LOG"
        
        echo "    配置 [$config] 完成。"
        echo "  === 配置: $config 完成 ===" >> "$OVERALL_LOG"
    done
    
    echo ">>>>>> 第 $run/$RUN_TIMES 轮运行完成 <<<<<<"
    echo "=== 第 $run/$RUN_TIMES 轮运行完成: $(date) ===" >> "$OVERALL_LOG"
done

# 记录总结束时间
TOTAL_END_TIME=$(date +%s)
TOTAL_END_TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

echo ""
echo "========================================="
echo "所有测试运行完成！"
echo "结果保存在: $BASE_OUTPUT_DIR/"
echo "测试日志保存在: $MAIN_LOG_DIR/"
echo ""

# 以下开始得出对应的perf xlsx表格和耗时数据表格
python "$SCRIPT_PATH/log_collect.py" "$BASE_OUTPUT_DIR" -o "$BASE_OUTPUT_DIR/tpch_all_times.xlsx"

python "$SCRIPT_PATH/xlsx_process.py" -i "$BASE_OUTPUT_DIR/tpch_all_times.xlsx" -o "$BASE_OUTPUT_DIR" -s "num_cpus_40_perf"

python "$SCRIPT_PATH/xlsx_collect.py" "$BASE_OUTPUT_DIR" -o "$BASE_OUTPUT_DIR/tpch_case_times.xlsx"

# ========== 用户配置区域 ==========
# 测试耗时数据表格的路径
TIME_DATA_PATH="$BASE_OUTPUT_DIR"
# 所有轮次perf数据的路径
PERF_DATA_PATH="$BASE_OUTPUT_DIR_DATA"
# 输出txt文件的路径
OUTPUT_PATH="$BASE_OUTPUT_DIR/tpch_perf"

# 需要处理的perf数据文件前缀模式（可修改）
PERF_FILE_PATTERN="perf_q*.data"
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
RUN_PERF_PATH="$PERF_DATA_PATH/$RUN_NAME/num_cpus_40"
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

mkdir -p "$BASE_OUTPUT_DIR/tpch_xlsx" 

# 开始生成perf的xlsx文件
python "$SCRIPT_PATH/perffuncnew_dir_type.py" -i "$OUTPUT_PATH" -o "$BASE_OUTPUT_DIR/tpch_xlsx"

# 把文件移到同一个位置方便下载
mkdir -p "$SCRIPT_DIR/alltest_result/tpch"

\cp -f "$BASE_OUTPUT_DIR/tpch_all_times.xlsx" "$SCRIPT_DIR/alltest_result/tpch"

\cp -f "$BASE_OUTPUT_DIR/tpch_case_times.xlsx" "$SCRIPT_DIR/alltest_result/tpch"

\cp -rf "$BASE_OUTPUT_DIR/tpch_xlsx" "$SCRIPT_DIR/alltest_result/tpch"

\cp -f "$EXCEL_FILE" "$SCRIPT_DIR/alltest_result/tpch"