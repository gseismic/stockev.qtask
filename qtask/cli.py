"""
qtask CLI — Redis Stream 分布式任务队列运维工具

快速开始::

    # 查看某个队列的状态
    qtask index my_ns:spider:tasks

    # 查看任务历史（最近 7 天的失败任务）
    qtask history my_ns:spider:tasks --status failed --days 7

    # 列出所有 namespace
    qtask ns list

    # 彻底清除一个 namespace 的所有数据（项目结束时使用）
    qtask ns purge my_ns -f

    # 更改历史保留天数
    qtask settings set --keep-days 30

概念速查:
    namespace  : 项目/任务级命名空间（如 "proj_a"），多台主机的 Worker 可共享。
                 用于按项目分组管理所有队列，项目结束后一键清除。
    queue_name : 队列基础名（不含 :stream 后缀）。
                 有 namespace 时格式为 "{ns}:{queue_name}"（如 "proj_a:spider:tasks"）。
    worker_id  : 单个 Worker 进程的唯一标识，格式为 "{hostname}-{random}"。
                 用于区分同一 namespace/group 中的不同主机/进程。
    worker_group: Redis Consumer Group 名，同 group 内多个 Worker 共享消费。
"""

import typer
import redis
from loguru import logger
import sys
from typing import Optional

app = typer.Typer(
    name="qtask",
    help=__doc__,
    no_args_is_help=True,
    rich_markup_mode="markdown",
)


def get_redis_client(url: str):
    return redis.from_url(url, decode_responses=True)


# ──────────────────────────────────────────────────────────────────
# 基础队列命令
# ──────────────────────────────────────────────────────────────────

@app.command("index")
def cmd_index(
    queue_name: str = typer.Argument(
        ...,
        help="队列基础名称。若队列属于某 namespace，请带前缀，如 'proj_a:spider:tasks'。",
        metavar="QUEUE",
    ),
    redis_url: str = typer.Option(
        "redis://localhost:6379/0", "--redis-url",
        help="Redis 连接 URL。格式: redis://[:password@]host:port/db",
        envvar="QTASK_REDIS_URL",
    ),
):
    """
    **查看队列基本信息**（Stream 长度、最新条目 ID）。

    示例::

        qtask index spider:tasks
        qtask index proj_a:spider:tasks --redis-url redis://:pass@10.0.0.1:6379/0
    """
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        length = r.xlen(q_name)
        typer.echo(f"📊 Queue : {q_name}")
        typer.echo(f"   Length: {length} messages")
        info = r.xinfo_stream(q_name)
        typer.echo(f"   Last ID: {info.get('last-generated-id')}")
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            typer.echo(f"⚠️  Queue '{q_name}' does not exist or is empty.")
        else:
            typer.echo(f"❌ Error: {e}")


@app.command("groups")
def cmd_groups(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL",
                                   help="Redis 连接 URL"),
):
    """
    **查看队列的所有 Consumer Group 状态**（消费者数量、Pending 数）。

    Pending 表示已被取出但尚未 ACK 的消息数；Pending 过多通常意味着 Worker 挂死了。
    此时可用 `qtask claim` 命令强制认领这些"僵尸"消息。

    示例::

        qtask groups proj_a:spider:tasks
    """
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        groups = r.xinfo_groups(q_name)
        if not groups:
            typer.echo(f"ℹ️  No consumer groups for '{q_name}'.")
            return
        typer.echo(f"\n👥 Consumer Groups for {q_name}:\n")
        for g in groups:
            typer.echo(f"  Group   : {g.get('name')}")
            typer.echo(f"  Consumers: {g.get('consumers')}")
            typer.echo(f"  Pending  : {g.get('pending')}")
            typer.echo(f"  Last ID  : {g.get('last-delivered-id')}\n")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("dlq")
def cmd_dlq(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    preview: bool = typer.Option(False, "--preview", "-p",
                                  help="预览前 5 条失败消息的 payload（用于排查错误原因）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **查看死信队列（DLQ）**——存放所有处理失败的消息。

    任务处理抛异常或重试次数超限后，消息会被移入 DLQ，不再自动重试。
    可用 `qtask requeue` 将 DLQ 中的消息重新放回主队列重试。

    示例::

        qtask dlq proj_a:spider:tasks            # 查看 DLQ 长度
        qtask dlq proj_a:spider:tasks --preview  # 顺带预览前 5 条
    """
    r = get_redis_client(redis_url)
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        length = r.xlen(dlq_name)
        status = typer.style("CLEAN ✅", fg=typer.colors.GREEN) if length == 0 \
            else typer.style(f"⚠️  {length} dead letters", fg=typer.colors.RED, bold=True)
        typer.echo(f"💀 DLQ: {dlq_name}  →  {status}")
        if length > 0 and preview:
            msgs = r.xrange(dlq_name, count=min(length, 5))
            typer.echo(f"\n   [Preview top {min(length,5)} of {length}]")
            for msg_id, payload in msgs:
                data = payload.get("payload", "")[:120]
                typer.echo(f"   {msg_id} → {data}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("requeue")
def cmd_requeue(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    task_id: str = typer.Option(None, "--task-id", "-t",
                                 help="指定单条任务的 Stream 消息 ID 重试；省略则重放 DLQ 全部消息"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **将 DLQ 中的失败任务重放回主队列**（Requeue / 重试）。

    一般流程：排查失败原因 → 修复代码/数据 → `qtask requeue` 重新投递。

    示例::

        qtask requeue proj_a:spider:tasks                        # 重放全部失败任务
        qtask requeue proj_a:spider:tasks --task-id 17419401234  # 只重放指定 ID
    """
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        msgs = r.xrange(dlq_name, min=task_id, max=task_id) if task_id else r.xrange(dlq_name, min="-", max="+")
        if not msgs:
            typer.echo("✅ DLQ is empty or task not found.")
            return
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            if payload.get("payload"):
                pipe.xadd(q_name, {"payload": payload["payload"]})
                pipe.xdel(dlq_name, msg_id)
        pipe.execute()
        typer.echo(f"🎉 Re-queued {len(msgs)} message(s) back to {q_name}.")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("claim")
def cmd_claim(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    group: str = typer.Option("default_group", "--group", "-g",
                               help="Consumer Group 名称（见 `qtask groups` 输出）"),
    idle_ms: int = typer.Option(300000, "--idle-ms",
                                help="只认领超过此毫秒数仍未 ACK 的 Pending 消息（默认 5 分钟）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **强制认领"僵尸" Pending 消息**（Claim 操作）。

    当 Worker 进程意外宕机时，它正在处理的消息会一直处于 Pending 状态。
    使用此命令将超时的 Pending 消息重新分配给当前 Worker，继续处理。
    Worker 启动时若设置了 `auto_claim=True`，这一步骤也会自动完成。

    示例::

        qtask claim proj_a:spider:tasks --group proj_a_group --idle-ms 60000
    """
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    count = q.claim_all(idle_time_ms=idle_ms)
    typer.echo(f"🎉 Claimed {count} zombie message(s).")


@app.command("reset")
def cmd_reset(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    group: str = typer.Option("default_group", "--group", "-g",
                               help="要重置的 Consumer Group 名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过二次确认"),
):
    """
    **重置消费组游标到最新位置（$）**，忽略历史积压消息。

    ⚠️ 危险操作：执行后游标之前的所有未处理消息将被 Worker 跳过，不再消费。
    适用场景：积压消息已无意义（如过时数据），需要让 Worker 从"现在"开始处理新消息。

    示例::

        qtask reset proj_a:spider:tasks --group proj_a_group -f
    """
    if not force:
        typer.confirm(
            f"Reset group '{group}' on '{queue_name}'?\n"
            "⚠️  All backlogged messages before current position will be SKIPPED.",
            abort=True,
        )
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    if q.reset_group():
        typer.echo(f"✅ Group '{group}' cursor reset to latest ($).")
    else:
        typer.echo("❌ Failed.")


@app.command("clear")
def cmd_clear(
    queue_name: str = typer.Argument(..., help="队列基础名称（含 namespace 前缀）", metavar="QUEUE"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过二次确认"),
    hard: bool = typer.Option(
        False, "--hard",
        help="彻底重置：在清空 Stream + DLQ 的基础上，同时删除该队列所有历史记录（Hash + SortedSet）",
    ),
):
    """
    **清空队列**（Stream + DLQ 所有消息）。

    默认只清 Stream 和 DLQ，历史记录保留（可在 Dashboard 查看）。
    加上 `--hard` 则连历史记录一并删除，彻底重置——不留任何残留。

    若要清除整个项目的所有队列，建议使用 `qtask ns purge` 命令。

    示例::

        qtask clear proj_a:spider:tasks -f         # 清空 Stream+DLQ，保留历史
        qtask clear proj_a:spider:tasks -f --hard  # 彻底清除，连历史也删
    """
    mode_desc = "Stream + DLQ + 所有历史记录（彻底重置）" if hard else "Stream + DLQ（历史保留）"
    if not force:
        typer.confirm(f"DANGER: Clear [{mode_desc}] for queue '{queue_name}'?", abort=True)
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name)
    if q.clear_all(clear_history=hard):
        extra = " (+ history wiped)" if hard else ""
        typer.echo(f"💣 Queue '{queue_name}' cleared{extra}.")
    else:
        typer.echo("❌ Failed.")


# ──────────────────────────────────────────────────────────────────
# Namespace 子命令组
# ──────────────────────────────────────────────────────────────────

ns_app = typer.Typer(
    name="ns",
    help="""
**管理 namespace（项目/任务级命名空间）**

namespace 用于按项目对所有队列进行分组。同一项目下的所有 SmartQueue/Worker 使用同一
namespace 参数，Redis key 会自动带上 "{ns}:" 前缀。

项目结束时，执行 `qtask ns purge <ns>` 可一键清除该 namespace 下**所有**数据（Stream、
DLQ、历史记录 Hash、历史索引 SortedSet、注册元数据），不留任何残留。

**创建 namespace（代码侧）**::

    # 生产者
    queue = SmartQueue("redis://localhost:6379/0", "spider:tasks", namespace="proj_a")

    # 消费者（多台主机，各自生成唯一 worker_id）
    worker = Worker(listen_url="...", listen_q_name="spider:tasks", namespace="proj_a")

namespace 注册发生在首次 push/Worker 启动时，无需手动创建。
""",
    no_args_is_help=True,
)
app.add_typer(ns_app, name="ns")


@ns_app.command("list")
def cmd_ns_list(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **列出所有已注册的 namespace** 及其队列数和历史统计。

    示例::

        qtask ns list
        qtask ns list --redis-url redis://:pass@10.0.0.1:6379/0
    """
    r = get_redis_client(redis_url)
    from qtask.queue import _NS_SET_KEY, _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    namespaces = sorted(r.smembers(_NS_SET_KEY))
    if not namespaces:
        typer.echo("ℹ️  No namespaces registered yet.\n\nHint: create a SmartQueue with namespace='...' to register one.")
        return
    typer.echo(f"\n🗂️  Registered Namespaces ({len(namespaces)})\n")
    header = f"{'NAMESPACE':<24} {'QUEUES':>6}  {'COMPLETED':>10}  {'FAILED':>7}  {'PENDING':>8}"
    typer.echo(typer.style(header, fg=typer.colors.BRIGHT_BLACK))
    typer.echo("─" * 62)
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
        fc = typer.style(str(agg['failed']), fg=typer.colors.RED if agg['failed'] else typer.colors.WHITE)
        line = f"{typer.style(ns, bold=True):<24} {len(queues):>6}  {agg['completed']:>10}  {fc:>7}  {agg['pending']:>8}"
        typer.echo(line)
    typer.echo()


@ns_app.command("info")
def cmd_ns_info(
    namespace: str = typer.Argument(..., help="Namespace 名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **查看 namespace 下各队列的详细信息**（Stream 长度、DLQ 数、历史统计）。

    示例::

        qtask ns info proj_a
    """
    r = get_redis_client(redis_url)
    from qtask.queue import _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    queues = sorted(r.smembers(_NS_QUEUES_KEY.format(ns=namespace)))
    if not queues:
        typer.echo(f"⚠️  Namespace '{namespace}' not found or empty.\nRun `qtask ns list` to see all namespaces.")
        return
    typer.echo(f"\n📂 Namespace: {typer.style(namespace, bold=True, fg=typer.colors.CYAN)}")
    typer.echo(f"   Queues count: {len(queues)}\n")
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
        dlq_tag = typer.style(f"DLQ:{dlq_len}", fg=typer.colors.RED, bold=True) if dlq_len else "DLQ:0"
        typer.echo(f"  📌 {typer.style(q, bold=True)}")
        typer.echo(f"     Stream:{stream_len}  {dlq_tag}  "
                   f"completed={c.get('completed',0)}  failed={c.get('failed',0)}  pending={c.get('pending',0)}")
    typer.echo()


@ns_app.command("purge")
def cmd_ns_purge(
    namespace: str = typer.Argument(..., help="要彻底清除的 Namespace 名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过二次确认（脚本批量使用时加此参数）"),
):
    """
    **彻底清除 namespace 下的所有数据**（Stream、DLQ、历史 Hash、历史 SortedSet、注册元数据）。

    ⚠️ 此操作**不可逆**，适用于项目结束后的完整清理。

    清除范围::

        {ns}:*                     ← 所有 Stream 和 DLQ
        qtask:hist:{ns}:*          ← 所有历史任务 Hash
        qtask:hist_idx:{ns}:*      ← 所有历史时间索引 SortedSet
        qtask:ns:{ns}:queues       ← namespace 队列注册元数据
        qtask:namespaces           ← 从全局 namespace 集合中移除

    示例::

        qtask ns purge proj_a           # 二次确认后清除
        qtask ns purge proj_a --force   # 无需确认（CI/脚本使用）
    """
    if not force:
        typer.confirm(
            f"\n⚠️  DANGER: Permanently delete ALL data in namespace '{namespace}'?\n"
            "   This includes: Streams, DLQ, history records, index, registration metadata.\n"
            "   This operation is IRREVERSIBLE.\n\nConfirm?",
            abort=True,
        )
    r = get_redis_client(redis_url)
    from qtask.queue import SmartQueue
    deleted = SmartQueue.purge_namespace(r, namespace)
    typer.echo(
        f"💣 Namespace '{namespace}' purged.\n"
        f"   stream_keys={deleted['stream_keys']}  "
        f"history_keys={deleted['history_keys']}  "
        f"meta_keys={deleted['meta_keys']}"
    )


# ──────────────────────────────────────────────────────────────────
# History 子命令
# ──────────────────────────────────────────────────────────────────

@app.command("history")
def cmd_history(
    queue_name: str = typer.Argument(
        ...,
        help="队列基础名称（含 namespace 前缀，如 'proj_a:spider:tasks'）",
        metavar="QUEUE",
    ),
    status: str = typer.Option(
        "all", "--status", "-s",
        help="按状态过滤：all（全部）| pending（等待中）| completed（已完成）| failed（已失败）",
    ),
    days: int = typer.Option(None, "--days", "-d",
                              help="只查最近 N 天（默认使用全局 keep_days 设置）"),
    limit: int = typer.Option(50, "--limit", "-n", help="最多显示条数（默认 50）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **查看队列的任务历史记录**（状态、重试次数、耗时、失败原因）。

    Worker 在处理任务的每个阶段（push/ack/fail/retry）都会自动记录历史。
    历史数据保存在 Redis 中，按 keep_days 设置自动清理过期记录。

    示例::

        # 查看所有历史（用全局 keep_days 设置）
        qtask history proj_a:spider:tasks

        # 只看最近 7 天的失败任务
        qtask history proj_a:spider:tasks --status failed --days 7 --limit 100
    """
    r = get_redis_client(redis_url)
    base_name = queue_name.replace(":stream", "")
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        keep_days_used = days if days is not None else hist.get_keep_days()
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()

        typer.echo(f"\n📋 History: {typer.style(base_name, bold=True)}")
        typer.echo(f"   Range   : last {keep_days_used} day(s) | Filter: {status} | Showing: {len(tasks)} / {counts['total']} total")
        typer.echo(f"   ✅ completed={counts['completed']}  ❌ failed={counts['failed']}  ⏳ pending={counts['pending']}\n")

        if not tasks:
            typer.echo("   (no records found — try adjusting --status or --days)")
            return

        header = f"{'TASK ID':<26} {'ACTION':<18} {'STATUS':<12} {'RETRIES':>7}  {'TIME':>7}  {'CREATED'}"
        typer.echo(typer.style(header, fg=typer.colors.BRIGHT_BLACK))
        typer.echo("─" * 88)

        import datetime
        status_color = {"completed": typer.colors.GREEN, "failed": typer.colors.RED, "pending": typer.colors.YELLOW}
        for t in tasks:
            st = t.get("status", "?")
            color = status_color.get(st, typer.colors.WHITE)
            dur = t.get("duration_s")
            dur_s = f"{dur:.3f}s" if dur is not None else "      -"
            cts = t.get("created_at", 0)
            c_str = datetime.datetime.fromtimestamp(cts).strftime("%m-%d %H:%M:%S") if cts else "-"
            st_styled = typer.style(f"{st:<12}", fg=color)
            typer.echo(f"{t.get('task_id','?'):<26} {t.get('action','?'):<18} {st_styled} {int(t.get('retries',0)):>7}  {dur_s:>7}  {c_str}")

        if len(tasks) == limit:
            typer.echo(f"\n  (showing first {limit} — use --limit to see more)")
    except Exception as e:
        typer.echo(f"❌ Error: {e}")
        raise typer.Exit(1)


# ──────────────────────────────────────────────────────────────────
# Settings 子命令组
# ──────────────────────────────────────────────────────────────────

settings_app = typer.Typer(
    name="settings",
    help="""
**管理 qtask 全局设置**

设置存储在 Redis Hash `qtask:settings` 中，所有 Worker 和 Queue 共享。

可配置项::

    history_keep_days   历史记录保留天数（1-365，默认 15）
                        超期记录在下次写入时懒清理，不影响实时性能。
""",
    no_args_is_help=True,
)
app.add_typer(settings_app, name="settings")


@settings_app.command("show")
def cmd_settings_show(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **显示当前全局设置**。

    示例::

        qtask settings show
    """
    r = get_redis_client(redis_url)
    from qtask.history import DEFAULT_KEEP_DAYS, SETTINGS_KEY
    raw = r.hgetall(SETTINGS_KEY)
    keep_days = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
    typer.echo(f"\n⚙️  qtask Global Settings\n")
    typer.echo(f"   history_keep_days = {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} days")
    typer.echo(f"\n   (stored in Redis key: {SETTINGS_KEY})\n")


@settings_app.command("set")
def cmd_settings_set(
    keep_days: int = typer.Option(..., "--keep-days", "-k",
                                   help="历史记录保留天数（1-365）。超过此天数的历史在下次写入时被懒清理。"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", envvar="QTASK_REDIS_URL"),
):
    """
    **设置历史任务保留天数**。

    也可通过 Web Dashboard → Settings 面板修改，效果相同。

    示例::

        qtask settings set --keep-days 30   # 保留 30 天
        qtask settings set -k 7             # 只保留最近 7 天
    """
    if keep_days < 1 or keep_days > 365:
        typer.echo("❌ keep_days 必须在 1 到 365 之间")
        raise typer.Exit(1)
    r = get_redis_client(redis_url)
    from qtask.history import TaskHistoryStore
    TaskHistoryStore.set_keep_days(r, keep_days)
    typer.echo(f"✅ history_keep_days 已设置为 {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} 天。")


# ──────────────────────────────────────────────────────────────────

def main():
    try:
        app()
    except Exception as e:
        logger.error(f"CLI Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
