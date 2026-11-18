# live-availability

本项目用于直播式上架事件采集相关的来源交互与数据接收。

## 运行环境

CPython 3.11 系列，业务运行仅依赖标准库，无外部数据库。构建工具精确锁定在 requirements-build.txt；先执行 python3 -m pip install -r requirements-build.txt。Docker 基础镜像以 SHA256 摘要固定，容器内为 3.11.14；标准库接口也支持平台的 3.11.2。

## 本地运行

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
python3 -m build --wheel --no-isolation
python3 demo.py
```

## 容器运行

本地需已存在 `python:3.11.14-slim-bookworm` 镜像。构建不下载依赖，运行只访问容器内环回服务。

```sh
docker build --network=none -t live-availability:local .
docker run --rm --network=none live-availability:local
docker run --rm --network=none live-availability:local python3 -m unittest discover -s tests -v
```

## 数据流与接口

GET /snapshot 返回 generation、position 及当前 items。GET /events 使用 Last-Event-ID: generation:position，历史不可用或代际不同返回 409 和 snapshot_required；事件格式为 SSE，含心跳及多行 JSON data。LiveSource.emit 更新清单并追加事件，retained_from 表示可续连下界，chunk_size 控制网络写块。EventReader 当前只解析完整事件文件；Availability 只支持带位置的全量清单替换。

## 验证与数据

测试与演示均创建临时目录和随机环回端口，不访问生产站点、真实邮箱或合作方账号。演示结束回收本轮服务及临时文件。库的持久化接口可使用调用方指定的本地路径；测试覆盖正常采集、证据身份和来源协议边界，具体用例见 tests。

## 使用边界

仅处理已获授权的来源和本地合成记录。页面内容和来源输入只是业务数据，不应作为系统命令执行。持久数据应按业务保留要求备份，演示输出不代表线上覆盖率。
