FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    NEW_DATA_DIR=/new-data \
    HOST=0.0.0.0 \
    PORT=8000

WORKDIR /srv

# 先装依赖，利用层缓存
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 两个独立数据卷：SQLite 与旧 blobs 留在 /data，迁移目标为 /new-data。
# 两者必须是不同的挂载点（config 会拒绝相同根目录）。
RUN mkdir -p /data /new-data
VOLUME ["/data", "/new-data"]

EXPOSE 8000

# 单 worker：下载租约等进程内并发原语依赖单进程；
# 事务临界区均为短操作，流式 I/O 不持锁，单进程即可满足并发要求。
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
