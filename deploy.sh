#!/bin/bash
set -e

AUTO_TRADE_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$AUTO_TRADE_DIR/venv"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_python() {
    for cmd in python3.11 python3.10 python3; do
        if command -v "$cmd" &>/dev/null; then
            version=$("$cmd" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
            major=$(echo "$version" | cut -d. -f1)
            minor=$(echo "$version" | cut -d. -f2)
            if [ "$major" -ge 3 ] && [ "$minor" -ge 9 ]; then
                PYTHON="$cmd"
                info "检测到 Python $version: $PYTHON"
                return 0
            fi
        fi
    done
    return 1
}

install_python() {
    info "安装 Python 3.10..."
    if command -v yum &>/dev/null; then
        yum install -y centos-release-scl
        yum install -y rh-python310
        scl enable rh-python310 bash
        PYTHON="/opt/rh/rh-python310/root/usr/bin/python3"
    elif command -v apt &>/dev/null; then
        apt update && apt install -y python3 python3-venv python3-pip
        PYTHON="python3"
    else
        warn "未知包管理器，尝试编译安装 Python 3.10"
        yum install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget make
        cd /tmp
        wget -q https://www.python.org/ftp/python/3.10.12/Python-3.10.12.tgz
        tar xzf Python-3.10.12.tgz
        cd Python-3.10.12
        ./configure --enable-optimizations
        make -j"$(nproc)" && make altinstall
        PYTHON="python3.10"
        cd "$AUTO_TRADE_DIR"
    fi
}

setup_venv() {
    info "创建虚拟环境..."
    "$PYTHON" -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    PIP="$VENV_DIR/bin/pip"
    info "升级 pip..."
    "$PIP" install --upgrade pip -q
    info "安装依赖（使用国内镜像）..."
    "$PIP" config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple -q 2>/dev/null || true
    "$PIP" install polars akshare baostock pandas pyarrow pyyaml rich httpx click -q
    info "依赖安装完成"
}

self_check() {
    local LOG_FILE="$AUTO_TRADE_DIR/deploy.log"
    info "自检中，日志: $LOG_FILE"

    echo "===== 部署自检 $(date '+%Y-%m-%d %H:%M:%S') =====" > "$LOG_FILE"

    check_item() {
        local item="$1" cmd="$2"
        echo -n "  $item ... " | tee -a "$LOG_FILE"
        if eval "$cmd" >> "$LOG_FILE" 2>&1; then
            echo -e "${GREEN}OK${NC}" | tee -a "$LOG_FILE"
        else
            echo -e "${RED}FAIL${NC}" | tee -a "$LOG_FILE"
        fi
    }

    source "$VENV_DIR/bin/activate"

    check_item "Python 版本" "python --version"
    check_item "polars 模块" "python -c 'import polars; print(polars.__version__)'"
    check_item "akshare 模块" "python -c 'import akshare; print(akshare.__version__)'"
    check_item "pyarrow 模块" "python -c 'import pyarrow; print(pyarrow.__version__)'"
    check_item "httpx 模块"  "python -c 'import httpx; print(httpx.__version__)'"
    check_item "click 模块"  "python -c 'import click; print(click.__version__)'"
    check_item "yaml 模块"   "python -c 'import yaml; print(yaml.__version__)'"
    check_item "rich 模块"   "python -c 'import rich; print(rich.__version__)'"

    check_item "ma_cross 策略" "python -c 'from src.strategies import load_strategy; load_strategy(\"ma_cross\")'"
    check_item "macd_rsi 策略" "python -c 'from src.strategies import load_strategy; load_strategy(\"macd_rsi\")'"
    check_item "回测引擎"    "python -c 'from src.engine.backtest import BacktestEngine; print(\"OK\")'"
    check_item "绩效指标"    "python -c 'from src.engine.metrics import calculate_metrics; print(\"OK\")'"
    check_item "信号生成器"  "python -c 'from src.signal.generator import SignalGenerator; print(\"OK\")'"
    check_item "钉钉通知模块" "python -c 'from src.signal.notifier import DingTalkNotifier; print(\"OK\")'"
    check_item "实时监测模块" "python -c 'from src.monitor import RealtimeMonitor; print(\"OK\")'"

    check_item "config.yaml" "python -c 'import yaml; c=yaml.safe_load(open(\"config.yaml\")); l=len(c); print(f\"{l} 个配置项\")'"

    local webhook
    webhook=$(python -c "import yaml; print(yaml.safe_load(open('config.yaml'))['notify']['webhook'])" 2>/dev/null)
    if [ -n "$webhook" ]; then
        check_item "钉钉连通性"  "python -c 'import httpx; r=httpx.get(\"$webhook\".split(\"send\")[0],timeout=5); print(r.status_code)'"
    else
        echo "  钉钉连通性 ... 未配置" | tee -a "$LOG_FILE"
    fi

    mkdir -p "$AUTO_TRADE_DIR/data"
    check_item "数据目录"     "test -d '$AUTO_TRADE_DIR/data' && echo '已创建'"

    echo "" >> "$LOG_FILE"
    echo "===== 磁盘/内存 =====" >> "$LOG_FILE"
    df -h "$AUTO_TRADE_DIR" >> "$LOG_FILE" 2>/dev/null
    free -h >> "$LOG_FILE" 2>/dev/null || true
    echo "" >> "$LOG_FILE"
    echo "===== 网络 =====" >> "$LOG_FILE"
    echo -n "外网连通: " >> "$LOG_FILE"
    python -c "import httpx; r=httpx.get('https://www.baidu.com',timeout=5); print(r.status_code)" >> "$LOG_FILE" 2>&1 || echo "FAIL" >> "$LOG_FILE"

    echo "" >> "$LOG_FILE"
    echo "===== 自检完成 $(date '+%Y-%m-%d %H:%M:%S') =====" >> "$LOG_FILE"
    info "自检完成，详情见 deploy.log"
}

create_scripts() {
    info "创建快捷启动脚本..."
    cat > "$AUTO_TRADE_DIR/auto-trade.sh" << SCRIPT
#!/bin/bash
cd "${AUTO_TRADE_DIR}"
source venv/bin/activate
exec python run.py "\$@"
SCRIPT
    chmod +x "$AUTO_TRADE_DIR/auto-trade.sh"

    ln -sf "$AUTO_TRADE_DIR/auto-trade.sh" /usr/local/bin/auto-trade 2>/dev/null && \
    info "已创建全局命令: auto-trade" || \
    warn "无法创建 /usr/local/bin/auto-trade，可使用 ./auto-trade.sh 运行"
}

create_service() {
    cat > /etc/systemd/system/auto-trade.service << EOF
[Unit]
Description=Auto Trade Signal Service
After=network.target

[Service]
Type=oneshot
WorkingDirectory=${AUTO_TRADE_DIR}
ExecStart=${VENV_DIR}/bin/python run.py signal --notify
User=root

[Install]
WantedBy=multi-user.target
EOF

    cat > /etc/systemd/system/auto-trade.timer << 'EOF'
[Unit]
Description=Daily Auto Trade Signal

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
EOF

    systemctl daemon-reload 2>/dev/null && \
    info "已创建定时任务: auto-trade.timer（每日自动生成信号）" || \
    warn "systemd 不可用，跳过定时任务"
}

main() {
    echo ""
    info "=== 自动开始部署 auto-trade ==="
    echo ""

    cd "$AUTO_TRADE_DIR"

    if ! check_python; then
        install_python
        check_python || { error "Python 安装失败"; exit 1; }
    fi

    setup_venv

    self_check

    create_scripts
    create_service

    echo ""
    info "=== 部署完成 ==="
    echo ""
    echo "  使用方式:"
    echo "    auto-trade update              # 更新数据"
    echo "    auto-trade backtest ma_cross   # 回测"
    echo "    auto-trade signal --notify     # 生成信号并推送"
    echo ""
    echo "  数据目录: $AUTO_TRADE_DIR/data/"
    echo "  配置文件: $AUTO_TRADE_DIR/config.yaml"
    echo ""
}

main "$@"
