"""qtask CLI — Redis Stream 分布式任务队列运维工具

\b
概念速查:
  namespace    项目/任务级命名空间，如 "proj_a"。多台主机 Worker 共享同一 namespace。
               项目结束后可一键清除该 namespace 下所有数据。
  queue_name   队列基础名（不含 :stream 后缀）。
               有 namespace 时格式为 ns:name，如 proj_a:spider:tasks。
               系统的消费者组和流标识均由 namespace 自动推导管理。

\b
常用命令:
  qtask index proj_a:spider:tasks
  qtask history proj_a:spider:tasks --status failed --days 7
  qtask dlq proj_a:spider:tasks --preview
  qtask ns list
  qtask ns purge proj_a -f
  qtask settings set --keep-days 30

\b
环境变量:
  QTASK_REDIS_URL   设置后所有命令无需再传 --redis-url
"""

import os, sys
# 确保 rich/typer help 在任何终端（含伪终端/管道）下都以合理宽度渲染，对 LLM 工具调用友好
if "COLUMNS" not in os.environ:
    os.environ["COLUMNS"] = "120"

import typer
import redis
from loguru import logger

app = typer.Typer(
    name="qtask",
    help=__doc__,
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    context_settings={"max_content_width": 120},
)


REDIS_URL_OPT = typer.Option(
    "redis://localhost:6379/0", "--redis-url",
    help="Redis 连接 URL，如 redis://:password@host:6379/0",
    envvar="QTASK_REDIS_URL",
)


@app.callback()
def main_callback(
    ctx: typer.Context,
    redis_url: str = REDIS_URL_OPT,
):
    """
    初始化全局 Redis 客户端并存储在 context 中。
    """
    ctx.obj = get_redis_client(redis_url)
    ctx.params["redis_url"] = redis_url


def get_redis_client(url: str):
    return redis.from_url(url, decode_responses=True)

# ──────────────────────── 基础队列命令 ────────────────────────

@app.command("index")
def cmd_index(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名，含 namespace 前缀时如 proj_a:spider:tasks", metavar="QUEUE"),
):
    """查看队列基本信息（长度、最新条目 ID）。"""
    r = ctx.obj
    q_name = f"{queue_name}:stream"
    try:
        length = r.xlen(q_name)
        typer.echo(f"Queue  : {q_name}")
        typer.echo(f"Length : {length} messages")
        info = r.xinfo_stream(q_name)
        typer.echo(f"Last ID: {info.get('last-generated-id')}")
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            typer.echo(f"Queue '{q_name}' does not exist or is empty.")
        else:
            typer.echo(f"Error: {e}")


@app.command("groups")
def cmd_groups(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名", metavar="QUEUE"),
):
    """查看队列的所有 Consumer Group（消费者数、Pending 数）。

    Pending 过多代表 Worker 挂死，可用 'qtask claim' 认领僵尸消息。
    """
    r = ctx.obj
    q_name = f"{queue_name}:stream"
    try:
        groups = r.xinfo_groups(q_name)
        if not groups:
            typer.echo(f"No consumer groups for '{q_name}'.")
            return
        for g in groups:
            typer.echo(f"Group={g.get('name')}  consumers={g.get('consumers')}  pending={g.get('pending')}  last_id={g.get('last-delivered-id')}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"Error: {e}")


@app.command("dlq")
def cmd_dlq(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名", metavar="QUEUE"),
    preview: bool = typer.Option(False, "--preview", "-p", help="预览前 5 条失败消息（排查错误原因）"),
):
    """查看死信队列（DLQ）——所有处理失败的消息都在这里。

    DLQ 中的消息不会自动重试，需用 'qtask requeue' 手动重放。
    """
    r = ctx.obj
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        length = r.xlen(dlq_name)
        status = "CLEAN" if length == 0 else f"{length} dead letters"
        typer.echo(f"DLQ: {dlq_name}  [{status}]")
        if length > 0 and preview:
            msgs = r.xrange(dlq_name, count=min(length, 5))
            for msg_id, payload in msgs:
                typer.echo(f"  {msg_id} -> {payload.get('payload','')[:100]}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"Error: {e}")


@app.command("requeue")
def cmd_requeue(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名", metavar="QUEUE"),
    task_id: str = typer.Option(None, "--task-id", "-t", help="指定单条 Stream 消息 ID；省略则重放 DLQ 全部消息"),
):
    """将 DLQ 中的失败任务重放回主队列（重试）。

    流程: 排查失败原因 -> 修复代码/数据 -> qtask requeue -> Worker 重新处理。
    """
    r = ctx.obj
    q_name = f"{queue_name}:stream"
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        msgs = r.xrange(dlq_name, min=task_id, max=task_id) if task_id else r.xrange(dlq_name, min="-", max="+")
        if not msgs:
            typer.echo("DLQ is empty or task not found.")
            return
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            if payload.get("payload"):
                pipe.xadd(q_name, {"payload": payload["payload"]})
                pipe.xdel(dlq_name, msg_id)
        pipe.execute()
        typer.echo(f"Re-queued {len(msgs)} message(s) -> {q_name}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"Error: {e}")


@app.command("claim")
def cmd_claim(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名，含 namespace 前缀时如 proj_a:spider:tasks", metavar="QUEUE"),
    idle_ms: int = typer.Option(300000, "--idle-ms", help="只认领超过此毫秒数仍未 ACK 的 Pending 消息（默认 5 分钟）"),
):
    """强制认领僵尸 Pending 消息（Worker 宕机后使用）。

    Worker 启动时若设置 auto_claim=True，此操作会自动执行。
    """
    from qtask.queue import SmartQueue
    ns = queue_name.split(":", 1)[0] if ":" in queue_name else ""
    q_base = queue_name.split(":", 1)[1] if ":" in queue_name else queue_name
    derived_group = f"{ns}_group" if ns else "default_group"
    
    redis_url = ctx.parent.params["redis_url"]
    q = SmartQueue(redis_url, q_base, namespace=ns)
    count = q.claim_all(worker_group=derived_group, worker_id="cli-manual", idle_time_ms=idle_ms)
    typer.echo(f"Claimed {count} zombie message(s) for group '{derived_group}'.")


@app.command("reset")
def cmd_reset(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名，含 namespace 前缀时如 proj_a:spider:tasks", metavar="QUEUE"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认"),
):
    """[危险] 重置消费组游标到最新位置（$），忽略所有积压消息。

    执行后积压消息会被 Worker 跳过，适用于积压数据已无效、需要从现在开始处理的场景。
    """
    ns = queue_name.split(":", 1)[0] if ":" in queue_name else ""
    q_base = queue_name.split(":", 1)[1] if ":" in queue_name else queue_name
    derived_group = f"{ns}_group" if ns else "default_group"

    if not force:
        typer.confirm(f"Reset group '{derived_group}' on '{queue_name}'? Backlogged messages will be SKIPPED.", abort=True)
    from qtask.queue import SmartQueue
    redis_url = ctx.parent.params["redis_url"]
    q = SmartQueue(redis_url, q_base, namespace=ns)
    if q.reset_group(derived_group):
        typer.echo(f"Group '{derived_group}' cursor reset to latest ($).")
    else:
        typer.echo("Failed.")


@app.command("clear")
def cmd_clear(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名，含 namespace 前缀时如 proj_a:spider:tasks", metavar="QUEUE"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认"),
    hard: bool = typer.Option(False, "--hard", help="同时清除所有历史记录（Hash + SortedSet），彻底重置"),
):
    """[危险] 清空队列（Stream + DLQ）。

    默认保留历史记录（可在 Dashboard 查看）。
    加 --hard 则连历史一起删，彻底重置，不留任何残留。
    清除整个项目请用 'qtask ns purge'。
    """
    mode = "Stream + DLQ + 历史记录 (--hard)" if hard else "Stream + DLQ"
    if not force:
        typer.confirm(f"DANGER: Clear [{mode}] for '{queue_name}'?", abort=True)
    from qtask.queue import SmartQueue
    ns = queue_name.split(":", 1)[0] if ":" in queue_name else ""
    q_base = queue_name.split(":", 1)[1] if ":" in queue_name else queue_name
    derived_group = f"{ns}_group" if ns else "default_group"
    
    redis_url = ctx.parent.params["redis_url"]
    q = SmartQueue(redis_url, q_base, namespace=ns)
    if q.clear_all(worker_group=derived_group, clear_history=hard):
        typer.echo(f"Queue '{queue_name}' cleared (group '{derived_group}' reset)." + (" (history wiped)" if hard else ""))
    else:
        typer.echo("Failed.")


# ──────────────────────── Namespace 子命令 ────────────────────────

ns_app = typer.Typer(
    name="ns",
    help="""管理 namespace（项目/任务级命名空间）。

namespace 是一个 Redis Key 前缀，将同一项目的所有队列归组。
SmartQueue/Worker 指定 namespace='proj_a' 后，Redis key 自动变为 proj_a:xxx:stream。
namespace 在首次 push 或 Worker 启动时自动注册，无需手动创建。

purge 操作删除范围:
  {ns}:*               所有 Stream + DLQ
  qtask:hist:{ns}:*    所有历史任务 Hash
  qtask:hist_idx:{ns}:* 所有历史时间索引 SortedSet
  qtask:ns:{ns}:queues namespace 队列注册元数据
""",
    no_args_is_help=True,
)
app.add_typer(ns_app, name="ns")


@ns_app.command("list")
def cmd_ns_list(ctx: typer.Context):
    """列出所有已注册的 namespace 及队列数和历史统计。"""
    r = ctx.obj
    from qtask.queue import _NS_SET_KEY, _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    namespaces = sorted(r.smembers(_NS_SET_KEY))
    if not namespaces:
        typer.echo("No namespaces registered. Create a SmartQueue with namespace='...' to register one.")
        return
    typer.echo(f"{'NAMESPACE':<24} {'QUEUES':>6}  {'COMPLETED':>10}  {'FAILED':>7}  {'PENDING':>8}")
    typer.echo("-" * 60)
    for ns in namespaces:
        queues = sorted(r.smembers(_NS_QUEUES_KEY.format(ns=ns)))
        agg = {"completed": 0, "failed": 0, "pending": 0}
        for q in queues:
            try:
                c = TaskHistoryStore(r, f"{ns}:{q}").count_by_status()
                for k in agg:
                    agg[k] += c.get(k, 0)
            except Exception:
                pass
        typer.echo(f"{ns:<24} {len(queues):>6}  {agg['completed']:>10}  {agg['failed']:>7}  {agg['pending']:>8}")


@ns_app.command("info")
def cmd_ns_info(
    ctx: typer.Context,
    namespace: str = typer.Argument(..., help="Namespace 名称"),
):
    """查看 namespace 下各队列的详细信息（Stream 长度、DLQ 数、历史统计）。"""
    r = ctx.obj
    from qtask.queue import _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    queues = sorted(r.smembers(_NS_QUEUES_KEY.format(ns=namespace)))
    if not queues:
        typer.echo(f"Namespace '{namespace}' not found. Run 'qtask ns list' to see all namespaces.")
        return
    typer.echo(f"Namespace: {namespace}  ({len(queues)} queues)")
    for q in queues:
        full_q = f"{namespace}:{q}"
        try:
            stream_len = r.xlen(f"{full_q}:stream")
            dlq_len = r.xlen(f"{full_q}:stream_dlq")
        except Exception:
            stream_len = dlq_len = 0
        try:
            c = TaskHistoryStore(r, full_q).count_by_status()
        except Exception:
            c = {}
        typer.echo(f"  {q}: stream={stream_len} dlq={dlq_len} completed={c.get('completed',0)} failed={c.get('failed',0)} pending={c.get('pending',0)}")


@ns_app.command("purge")
def cmd_ns_purge(
    ctx: typer.Context,
    namespace: str = typer.Argument(..., help="要彻底清除的 Namespace 名称"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认（脚本/批量使用）"),
):
    """[危险] 彻底清除 namespace 下的所有 Redis 数据（不可逆）。

    删除范围: Stream、DLQ、历史 Hash、历史 SortedSet、注册元数据。
    """
    if not force:
        typer.confirm(f"DANGER: Permanently delete ALL data in namespace '{namespace}'? (irreversible)", abort=True)
    r = ctx.obj
    from qtask.queue import SmartQueue
    deleted = SmartQueue.purge_namespace(r, namespace)
    typer.echo(f"Namespace '{namespace}' purged: stream_keys={deleted['stream_keys']} history_keys={deleted['history_keys']} meta_keys={deleted['meta_keys']}")


# ──────────────────────── History 命令 ────────────────────────

@app.command("history")
def cmd_history(
    ctx: typer.Context,
    queue_name: str = typer.Argument(..., help="队列基础名（如 proj_a:spider:tasks）", metavar="QUEUE"),
    status: str = typer.Option("all", "--status", "-s", help="状态过滤: all | pending | completed | failed"),
    days: int = typer.Option(None, "--days", "-d", help="只查最近 N 天（默认使用全局 keep_days 设置）"),
    limit: int = typer.Option(50, "--limit", "-n", help="最多显示条数（默认 50）"),
):
    """查看任务历史记录（状态、重试次数、耗时、失败原因）。

    历史由 Worker 自动记录，按 keep_days 设置懒清理过期数据。
    修改保留天数: qtask settings set --keep-days 30
    """
    r = ctx.obj
    base_name = queue_name.replace(":stream", "")
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        keep_days_used = days if days is not None else hist.get_keep_days()
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()

        typer.echo(f"Queue: {base_name}  last={keep_days_used}d  filter={status}  showing={len(tasks)}/{counts['total']}")
        typer.echo(f"completed={counts['completed']}  failed={counts['failed']}  pending={counts['pending']}")
        if not tasks:
            typer.echo("(no records — try adjusting --status or --days)")
            return

        typer.echo(f"\n{'TASK_ID':<26} {'ACTION':<18} {'STATUS':<11} {'RETRY':>5}  {'TIME':>7}  CREATED")
        typer.echo("-" * 84)
        import datetime
        for t in tasks:
            st = t.get("status", "?")
            dur = t.get("duration_s")
            dur_s = f"{dur:.3f}s" if dur is not None else "      -"
            cts = t.get("created_at", 0)
            c_str = datetime.datetime.fromtimestamp(cts).strftime("%m-%d %H:%M:%S") if cts else "-"
            typer.echo(f"{t.get('task_id','?'):<26} {t.get('action','?'):<18} {st:<11} {int(t.get('retries',0)):>5}  {dur_s:>7}  {c_str}")

        if len(tasks) == limit:
            typer.echo(f"\n(first {limit} results — use --limit N to see more)")
    except Exception as e:
        typer.echo(f"Error: {e}")
        raise typer.Exit(1)


# ──────────────────────── Settings 子命令 ────────────────────────

settings_app = typer.Typer(
    name="settings",
    help="""管理 qtask 全局设置（存储在 Redis Hash qtask:settings）。

当前可配置项:
  history_keep_days   历史记录保留天数（1-365，默认 15）
                      超期记录在下次写入时懒清理，不影响实时性能。
""",
    no_args_is_help=True,
)
app.add_typer(settings_app, name="settings")


@settings_app.command("show")
def cmd_settings_show(ctx: typer.Context):
    """显示当前全局设置。"""
    r = ctx.obj
    from qtask.history import DEFAULT_KEEP_DAYS, SETTINGS_KEY
    raw = r.hgetall(SETTINGS_KEY)
    keep_days = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
    typer.echo(f"history_keep_days = {keep_days} days  (redis key: {SETTINGS_KEY})")


@settings_app.command("set")
def cmd_settings_set(
    ctx: typer.Context,
    keep_days: int = typer.Option(..., "--keep-days", "-k", help="历史记录保留天数（1-365）"),
):
    """设置历史任务保留天数（同 Dashboard -> Settings 面板）。"""
    if keep_days < 1 or keep_days > 365:
        typer.echo("Error: keep_days must be between 1 and 365")
        raise typer.Exit(1)
    r = ctx.obj
    from qtask.history import TaskHistoryStore
    TaskHistoryStore.set_keep_days(r, keep_days)
    typer.echo(f"history_keep_days set to {keep_days} days.")


# ──────────────────────────────────────────────────────────────────

def main():
    try:
        app()
    except Exception as e:
        logger.error(f"CLI Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
