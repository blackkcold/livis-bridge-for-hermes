FROM python:3.11-slim

WORKDIR /app

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 源码与测试先 COPY（editable install 需要包存在）
COPY pyproject.toml .
COPY src/ src/
COPY tests/ tests/
COPY scripts/ scripts/

# Python 依赖
RUN pip install --no-cache-dir -e ".[dev]"

# 运行时数据目录
RUN mkdir -p /app/data

# 非 root 运行
RUN useradd -m -s /bin/bash bridge
RUN chown -R bridge:bridge /app
USER bridge

ENV PYTHONUNBUFFERED=1 \
    LIVIS_DATA_DIR=/app/data \
    HERMES_BIN=/app/scripts/fake_hermes.sh

WORKDIR /app

# 默认跑测试；启动 bridge 由 compose/命令覆盖
CMD ["python", "-m", "pytest", "tests/", "-v"]
