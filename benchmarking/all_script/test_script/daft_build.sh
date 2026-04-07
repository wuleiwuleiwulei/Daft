#!/bin/bash

# ========== 配置区域 ==========
# 请修改为你的 Daft 项目路径
# 0.7.1版本里面的daft-dashboard用的是bun，nvm版本和0.7.5不同，是20
# 0.7.5版本里面的daft-dashboard用的是nvm，nvm版本是22
# 现在用的是0.7.5就不要修改了
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# 向上回溯4级到 Daft 根目录（脚本位于 Daft/benchmarking/all_script/test_script/ 下）
DAFT_PATH="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

echo "检测到 Daft 路径: $DAFT_PATH"
# ===============================

# set -u  # 使用未定义变量时报错

# 检查是否提供了路径（已在配置区域修改，但可保留提示）
if [ ! -d "$DAFT_PATH" ]; then
    echo "错误: DAFT_PATH 所指向的目录不存在: $DAFT_PATH"
    echo "请在脚本开头的配置区域修改 DAFT_PATH 变量。"
    exit 1
fi

echo "开始准备编译 Daft，项目路径: $DAFT_PATH"
echo "========================================="

# 1. 检查 Rust 是否安装
echo "检查 Rust 环境..."
if ! command -v rustc &> /dev/null || ! command -v cargo &> /dev/null; then
    echo "错误: Rust 未安装。请先安装 Rust 。"
    exit 1
fi
echo "✓ Rust 已安装"

# bun安装方法，适用于0.7.1
# To get started, run: 
#   curl -fsSL https://bun.com/install | bash
#   source /root/.bash_profile 
#   bun --help 

# 2. 检查并安装 Node.js (通过 nvm)，适用于0.7.5
echo "检查 Node.js 环境..."
if ! command -v node &> /dev/null; then
    echo "Node.js 未安装，正在通过 nvm 安装..."
    # 安装 nvm
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
    # 加载 nvm
    # export NVM_DIR="$HOME/.nvm"
    # [ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"
    source ~/.bashrc
    if ! command -v nvm &> /dev/null; then
        echo "错误: nvm 安装失败或无法加载。"
        exit 1
    fi
    # 安装 Node.js 22
    nvm install 22
    nvm alias default 22
    if ! command -v node &> /dev/null; then
        echo "错误: Node.js 安装失败。"
        exit 1
    fi
    echo "✓ Node.js 安装成功"
else
    nvm alias default 22
    echo "✓ Node.js 已安装"
fi

# 检查 npm 是否可用
if ! command -v npm &> /dev/null; then
    echo "错误: npm 未找到，请检查 Node.js 安装。"
    exit 1
fi

# 3. 取消 npm SSL 检查（解决证书问题）
npm config set strict-ssl false
echo "✓ npm 已配置 strict-ssl=false"

# 4. 检查并安装 uv
echo "检查 uv 环境..."
if ! command -v uv &> /dev/null; then
    echo "uv 未安装，正在安装..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    # # 安装脚本会将 uv 添加到 ~/.cargo/bin，尝试刷新 PATH
    # export PATH="$HOME/.cargo/bin:$PATH"
    source ~/.bashrc
    if ! command -v uv &> /dev/null; then
        echo "错误: uv 安装失败，请手动安装后重试。"
        exit 1
    fi
    echo "✓ uv 安装成功"
else
    echo "✓ uv 已安装"
fi

# 5. 检查 CMake
echo "检查 CMake 环境..."
if ! command -v cmake &> /dev/null; then
    echo "错误: CMake 未安装。请先安装 CMake (如 sudo yum install cmake 或从官网下载)。"
    exit 1
fi
echo "✓ CMake 已安装"

# 6. 设置环境变量（解决字体下载网络问题）
export NEXT_TURBOPACK_EXPERIMENTAL_USE_SYSTEM_TLS_CERTS=1
echo "✓ 已设置 NEXT_TURBOPACK_EXPERIMENTAL_USE_SYSTEM_TLS_CERTS=1"

# 7. 进入项目目录
cd "$DAFT_PATH" || { echo "错误: 无法进入目录 $DAFT_PATH"; exit 1; }

# 8. 编译
echo "开始编译 Daft..."
echo "-----------------------------------------"

echo "执行 make clean..."
make clean

echo "执行 make build-release..."
make build-release

echo "执行 make build-whl..."
make build-whl

# 9. 输出 whl 文件位置（尝试从 make 输出中获取最后一行）
echo "-----------------------------------------"
echo "编译完成！生成的 wheel 文件位于："
# 查找最近生成的 .whl 文件
WHL_FILE=$(find target/wheels/ -name "*.whl" -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1 | cut -d' ' -f2-)
if [ -n "$WHL_FILE" ]; then
    echo "  $WHL_FILE"
    echo "你可以使用以下命令安装："
    echo "  pip install $WHL_FILE"
    pip install $WHL_FILE
else
    echo "未能自动定位 wheel 文件，请查看上述 make 输出中的路径。"
fi

echo "========================================="