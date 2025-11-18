FROM python:3.9-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONPIP_NO_CACHE_DIR=off
ENV PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ENV TZ=Asia/Shanghai

# apt换国内源
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 安装基础工具
RUN apt update && apt install -y --no-install-recommends \
    curl \
    git \
    sudo \
    wget \
    && rm -rf /var/lib/apt/lists/*

# 创建 UID/GID 1000 的用户
RUN groupadd -g 1000 developer && \
    useradd -m -u 1000 -g 1000 -s /bin/bash developer && \
    echo 'developer ALL=(root) NOPASSWD:ALL' >> /etc/sudoers.d/developer

# 设置工作目录权限
WORKDIR /workspace
RUN chown developer:developer /workspace

# 切换用户
USER developer

# 安装 Python 包到用户目录
ENV PATH="/home/developer/.local/bin:${PATH}"
COPY --chown=developer:developer requirements.txt .
RUN pip install -U pip && pip install -r requirements.txt
