# live-availability

本项目用于直播式上架事件采集：长期跟随来源的事件流，并保留全量清单拉取作为对账手段。

## 运行环境

CPython 3.11 系列，业务运行仅依赖标准库，无外部数据库。构建工具精确锁定在 requirements-build.txt；先执行 python3 -m pip install -r requirements-build.txt。Docker 基础镜像以 SHA256 摘要固定，容器内为 3.11.14；标准库接口也支持平台的 3.11.2。

## 本地运行

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q .
python3 -m build --wheel --no-isolation
python3 demo.py
```

持续采集：

```sh
python3 -m live --url http://127.0.0.1:8000 --db availability.sqlite3
```

进程退出后重新启动会从数据库中的最后边界续连；已生效的事件不会重复应用，未覆盖的位置仍会被标记为缺口。

## 容器运行

首次构建拉取摘要固定的官方 Python 镜像并安装锁定的构建工具；运行不访问外网，只使用容器内环回服务。

```sh
docker build -t live-availability:local .
docker run --rm --network=none live-availability:local
docker run --rm --network=none live-availability:local python3 -m unittest discover -s tests -v
```

## 来源协议

GET /snapshot 返回 generation、position 及当前 items；position 是该清单所覆盖的流位置。

GET /events 使用 Last-Event-ID: generation:position 续连：

* 成功返回 text/event-stream；心跳为注释行（`: ...`），业务事件是 `id: generation:position`、`event: availability` 和多行 `data:` JSON（data 是含 generation、position、kind、record 的对象）。
* 位置已不在保留历史内，或代际不同，返回 409 `{"error": "snapshot_required", "generation": ...}`。
* 事件可能在任意字节位置被网络切分，多行 data 属于同一个事件，心跳不是业务事件。
* 来源会在请求参数 `?follow=1` 时保持连接并持续推送（本地 fixture 支持；真实来源即使一次性返回，采集器也会自动重连续连）。

EventReader 是增量解析器：只有以空行结束的完整帧才会产生事件，未收完的帧不会推进恢复位置，连接在帧中间断开时抛出 `IncompleteEvent`。

## 采集与对账

* `Collector.start()` 用后台线程长期跟随事件流；`collect()` 手动续连一次；`sync()` / `reconcile()` 拉取全量清单并接替。
* 每个事件在**单个事务**中同时写入清单、边界和 history：进程保存到一半退出时，下游看到的是完整的前缀（哪些事件已生效可从 `history()` 读出），下一条事件仍然缺失并会被续连补上。
* 来源拒绝续连（409）时调用 `begin_gap()`：保留最后一份可信清单，记录 `missing_from`。缺口期间新到达的消息只会被拒绝而不会把状态改成“已追平”；只有位置越过缺口的全量清单能清除标记。
* 全量清单与流的交接：清单写入其自身 position 作为边界，事件流随后从该 position 续连。快照期间产生的事件全部落在快照位置之后，因此不会丢失；重复到达的旧事件按 `duplicate` 记入 history 而不重复应用。
* 新代际的序号从 0 重新开始，不与旧代际比较：收到新代际事件即标记 `generation_changed`，等待全量清单接替。
* `Availability.view()` 返回当前可播清单及 `stale`/`missing_from`；`status()` 返回可信边界、缺口、`caught_up` 以及 `coverage()`。

`coverage()` 是从 history 归纳出的边界证据：`snapshot` 段表示全量清单覆盖 `0..position`，`stream` 段表示连续应用的事件区间（相邻事件合并）；`duplicate` 与 `superseded` 消息单独保留在 `history()` 中，不会被隐藏。

## 验证与数据

测试与演示均创建临时目录和随机环回端口，不访问生产站点、真实邮箱或合作方账号。演示结束回收本轮服务及临时文件。库的持久化接口可使用调用方指定的本地路径；测试覆盖任意切分解析、心跳与多行数据、断帧续连、缺口保持、快照交接、代际切换、重复消息证据、半途退出后的前缀一致性，具体用例见 tests。

## 使用边界

仅处理已获授权的来源和本地合成记录。页面内容和来源输入只是业务数据，不应作为系统命令执行。持久数据应按业务保留要求备份，演示输出不代表线上覆盖率。
