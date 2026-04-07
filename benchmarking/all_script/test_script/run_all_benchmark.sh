#!/bin/bash

# ==========================================
# 检查并接收数据路径参数
# ==========================================
DEFAULT_DATA_PATH="/home/data"   # 请根据实际情况修改默认路径

if [ $# -eq 0 ]; then
    DATA_PATH="$DEFAULT_DATA_PATH"
    echo "未指定数据路径，使用默认路径: $DATA_PATH"
elif [ $# -eq 1 ]; then
    DATA_PATH="$1"
else
    echo "用法: $0 [数据路径]"
    echo "如果不指定数据路径，将使用默认路径: $DEFAULT_DATA_PATH"
    DATA_PATH="$DEFAULT_DATA_PATH"
fi

if [ ! -d "$DATA_PATH" ]; then
    echo "错误：数据路径不存在: $DATA_PATH"
    DATA_PATH="$DEFAULT_DATA_PATH"
fi
echo "数据路径: $DATA_PATH"

grep -i huge /proc/meminfo
echo 65535 > /proc/sys/vm/nr_hugepages

cat /sys/kernel/mm/transparent_hugepage/enabled
sudo sh -c 'echo always > /sys/kernel/mm/transparent_hugepage/enabled'

cat /proc/sys/kernel/numa_balancing
sudo sh -c 'echo 0 > /proc/sys/kernel/numa_balancing'

lscpu
sudo sh -c 'echo on > /sys/devices/system/cpu/smt/control'

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1

# ==========================================
# 自动检测脚本所在目录（即 alltest 目录）
# ==========================================
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "脚本所在目录: $SCRIPT_DIR"

# BENCHMARK_ROOT：SCRIPT_DIR 向上两级
BENCHMARK_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
if [ ! -d "$BENCHMARK_ROOT" ]; then
    echo "错误：BENCHMARK_ROOT 目录不存在: $BENCHMARK_ROOT"
    exit 1
fi
echo "BENCHMARK_ROOT: $BENCHMARK_ROOT"

# SCRIPT_PATH：SCRIPT_DIR 下的 python_script 子目录
SCRIPT_PATH="$SCRIPT_DIR/python_script"
if [ ! -d "$SCRIPT_PATH" ]; then
    echo "错误：SCRIPT_PATH 目录不存在: $SCRIPT_PATH"
    exit 1
fi
echo "SCRIPT_PATH: $SCRIPT_PATH"

# tpch 测试（传递三个参数）
# TPCH_GEN_FOLDER：数据路径下的 tpch 子目录
TPCH_GEN_FOLDER="$DATA_PATH/tpch-dbgen"
if [ ! -d "$TPCH_GEN_FOLDER" ]; then
    echo "错误：TPCH_GEN_FOLDER 目录不存在: $TPCH_GEN_FOLDER"
    exit 1
fi
echo "TPCH_GEN_FOLDER: $TPCH_GEN_FOLDER"

bash +x "$SCRIPT_DIR/tpch.sh" "$BENCHMARK_ROOT/tpch" "$TPCH_GEN_FOLDER" "$SCRIPT_PATH"

# tpcds 测试
# TPCDS_GEN_FOLDER：数据路径下的 tpch 子目录
TPCDS_GEN_FOLDER="$DATA_PATH/tpcds-dbgen"
if [ ! -d "$TPCDS_GEN_FOLDER" ]; then
    echo "错误：TPCDS_GEN_FOLDER 目录不存在: $TPCDS_GEN_FOLDER"
    exit 1
fi
echo "TPCDS_GEN_FOLDER: $TPCDS_GEN_FOLDER"
bash +x "$SCRIPT_DIR/tpcds.sh" "$BENCHMARK_ROOT/tpcds" "$TPCDS_GEN_FOLDER" "$SCRIPT_PATH"

# ai 测试
bash +x "$SCRIPT_DIR/ai.sh" "$BENCHMARK_ROOT/ai" "$SCRIPT_PATH"

# parquet 测试
bash +x "$SCRIPT_DIR/parquet.sh" "$BENCHMARK_ROOT/parquet" "$SCRIPT_PATH"

# vllm 测试
bash +x "$SCRIPT_DIR/vllm.sh" "$BENCHMARK_ROOT/vllm" "$SCRIPT_PATH"


# 输出最终生成的文件路径（供下载脚本使用）
DATA_DIR="$SCRIPT_DIR"
DATA_FILE="$SCRIPT_DIR/alltest_result.zip"

echo "DATA_DIR=$DATA_DIR"
echo "DATA_FILE=$DATA_FILE"

cd $DATA_DIR
rm -f "$DATA_FILE"
zip -r "$DATA_FILE" "alltest_result/"
echo "DATA_FILE:${DATA_FILE}"