import typer
import redis
from loguru import logger
import sys
from typing import Optional

app = typer.Typer(help="qtask CLI: Inspect Redis Stream Queues.")


def get_redis_client(url: str):
    return redis.from_url(url, decode_responses=True)


@app.command("index")
def cmd_index(
    queue_name: str = typer.Argument(..., help="队列基础名称（如 spider:tasks 或 proj_a:spider:tasks）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """显示队列的总消息数和 Stream 信息"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        length = r.xlen(q_name)
        typer.echo(f"📊 Queue: {q_name}  Length: {length}")
        info = r.xinfo_stream(q_name)
        typer.echo(f"   Last Entry ID: {info.get('last-generated-id')}")
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            typer.echo(f"⚠️ Queue {q_name} does not exist or is empty.")
        else:
            typer.echo(f"❌ Error: {e}")


@app.command("groups")
def cmd_groups(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """显示消费组和 Pending 状态"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        groups = r.xinfo_groups(q_name)
        if not groups:
            typer.echo(f"ℹ️ No consumer groups found for {q_name}.")
            return
        typer.echo(f"👥 Consumer Groups for {q_name}:")
        for g in groups:
            typer.echo(f"  - [{g.get('name')}] consumers={g.get('consumers')} pending={g.get('pending')} last_id={g.get('last-delivered-id')}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("dlq")
def cmd_dlq(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    preview: bool = typer.Option(False, "--preview", help="预览前 5 条失败消息"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """显示死信队列（DLQ）状态"""
    r = get_redis_client(redis_url)
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        length = r.xlen(dlq_name)
        typer.echo(f"💀 DLQ: {dlq_name}  Length: {length}")
        if length > 0 and preview:
            msgs = r.xrange(dlq_name, count=min(length, 5))
            typer.echo("   [Preview top 5]")
            for msg_id, payload in msgs:
                data = payload.get("payload", "")[:80]
                typer.echo(f"      {msg_id} -> {data}")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("requeue")
def cmd_requeue(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    task_id: str = typer.Option(None, "--task-id", help="指定单条任务 ID，不指定则重放全部"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """将 DLQ 中的失败任务重放回主队列"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        if task_id:
            msgs = r.xrange(dlq_name, min=task_id, max=task_id)
        else:
            msgs = r.xrange(dlq_name, min="-", max="+")
        if not msgs:
            typer.echo("✅ DLQ is empty or task not found.")
            return
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            if payload.get("payload"):
                pipe.xadd(q_name, {"payload": payload["payload"]})
                pipe.xdel(dlq_name, msg_id)
        pipe.execute()
        typer.echo(f"🎉 Successfully re-queued {len(msgs)} messages!")
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")


@app.command("claim")
def cmd_claim(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    group: str = typer.Option("default_group", "--group", help="消费组名称"),
    idle_ms: int = typer.Option(300000, "--idle-ms", help="只认领超过此毫秒数的 Pending 消息"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """强制认领所有 Zombie Pending 消息"""
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    count = q.claim_all(idle_time_ms=idle_ms)
    typer.echo(f"🎉 Claimed {count} zombie messages!")


@app.command("reset")
def cmd_reset(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    group: str = typer.Option("default_group", "--group", help="消费组名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认"),
):
    """重置消费组游标至最新（放弃当前积压）"""
    if not force:
        typer.confirm(f"Reset group '{group}' on '{queue_name}'? Backlog will be ignored.", abort=True)
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    if q.reset_group():
        typer.echo(f"✅ Group {group} cursor reset to latest ($).")
    else:
        typer.echo("❌ Failed.")


@app.command("clear")
def cmd_clear(
    queue_name: str = typer.Argument(..., help="队列基础名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认"),
):
    """危险：清空队列及其 DLQ 的所有数据"""
    if not force:
        typer.confirm(f"DANGER: Permanently delete ALL data in queue '{queue_name}'?", abort=True)
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name)
    if q.clear_all():
        typer.echo(f"💣 Queue {queue_name} has been completely wiped.")
    else:
        typer.echo("❌ Failed.")


# ──────────────────────── Namespace 子命令 ────────────────────────

ns_app = typer.Typer(help="管理 qtask namespace（项目/任务级命名空间）")
app.add_typer(ns_app, name="ns")


@ns_app.command("list")
def cmd_ns_list(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """列出所有已注册的 namespace 及其队列统计"""
    r = get_redis_client(redis_url)
    from qtask.queue import SmartQueue, _NS_SET_KEY, _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    namespaces = sorted(r.smembers(_NS_SET_KEY))
    if not namespaces:
        typer.echo("ℹ️  No namespaces registered yet.")
        return
    typer.echo(f"\n🗂️  Registered Namespaces ({len(namespaces)})\n")
    header = f"{'NAMESPACE':<24} {'QUEUES':>7}  {'COMPLETED':>10}  {'FAILED':>7}  {'PENDING':>8}"
    typer.echo(typer.style(header, fg=typer.colors.BRIGHT_BLACK))
    typer.echo("─" * 65)
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
        row = f"{typer.style(ns, bold=True):<24} {len(queues):>7}  {agg['completed']:>10}  {fc:>7}  {agg['pending']:>8}"
        typer.echo(row)


@ns_app.command("info")
def cmd_ns_info(
    namespace: str = typer.Argument(..., help="Namespace 名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """详细显示某 namespace 下的所有队列和历史统计"""
    r = get_redis_client(redis_url)
    from qtask.queue import _NS_QUEUES_KEY
    from qtask.history import TaskHistoryStore
    queues = sorted(r.smembers(_NS_QUEUES_KEY.format(ns=namespace)))
    if not queues:
        typer.echo(f"⚠️  Namespace '{namespace}' not found or empty.")
        return
    typer.echo(f"\n📂 Namespace: {typer.style(namespace, bold=True, fg=typer.colors.CYAN)}")
    typer.echo(f"   Queues: {len(queues)}\n")
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
        typer.echo(f"  📌 {q}")
        typer.echo(f"     Stream: {stream_len}  DLQ: {dlq_len}  completed={c.get('completed',0)}  failed={c.get('failed',0)}  pending={c.get('pending',0)}")


@ns_app.command("purge")
def cmd_ns_purge(
    namespace: str = typer.Argument(..., help="要清除的 Namespace 名称"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
    force: bool = typer.Option(False, "--force", "-f", help="跳过确认"),
):
    """危险：删除 namespace 下所有数据（Stream / DLQ / 历史记录 / 注册元数据）"""
    if not force:
        typer.confirm(
            f"DANGER: Permanently delete ALL data in namespace '{namespace}'?\n"
            "This includes streams, DLQ, history and registration metadata.",
            abort=True,
        )
    r = get_redis_client(redis_url)
    from qtask.queue import SmartQueue
    deleted = SmartQueue.purge_namespace(r, namespace)
    typer.echo(f"💣 Namespace '{namespace}' purged: {deleted}")


# ──────────────────────── History 子命令 ────────────────────────

@app.command("history")
def cmd_history(
    queue_name: str = typer.Argument(..., help="队列基础名称（如 proj_a:spider:tasks）"),
    status: str = typer.Option("all", "--status", "-s", help="筛选状态: all | pending | completed | failed"),
    days: int = typer.Option(None, "--days", "-d", help="最近 N 天"),
    limit: int = typer.Option(50, "--limit", "-n", help="最多显示条数"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """查看任务历史（状态/重试次数/耗时）"""
    r = get_redis_client(redis_url)
    base_name = queue_name.replace(":stream", "")
    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        keep_days_used = days if days is not None else hist.get_keep_days()
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()
        typer.echo(f"\n📋 History: {typer.style(base_name, bold=True)}")
        typer.echo(f"   Range: last {keep_days_used}d | Filter: {status} | Showing: {len(tasks)}")
        typer.echo(f"   ✅ {counts['completed']}  ❌ {counts['failed']}  ⏳ {counts['pending']}  total={counts['total']}\n")
        if not tasks:
            typer.echo("   (no records found)")
            return
        header = f"{'TASK ID':<28} {'ACTION':<20} {'STATUS':<12} {'RETRIES':>7}  {'DURATION':>9}  {'CREATED'}"
        typer.echo(typer.style(header, fg=typer.colors.BRIGHT_BLACK))
        typer.echo("─" * 90)
        import datetime
        for t in tasks:
            st = t.get("status", "?")
            color = {"completed": typer.colors.GREEN, "failed": typer.colors.RED, "pending": typer.colors.YELLOW}.get(st, typer.colors.WHITE)
            dur = t.get("duration_s")
            dur_s = f"{dur:.2f}s" if dur is not None else "-"
            cts = t.get("created_at", 0)
            c_str = datetime.datetime.fromtimestamp(cts).strftime("%m-%d %H:%M:%S") if cts else "-"
            typer.echo(f"{t.get('task_id','?'):<28} {t.get('action','?'):<20} {typer.style(f'{st:<12}', fg=color)} {int(t.get('retries',0)):>7}  {dur_s:>9}  {c_str}")
    except Exception as e:
        typer.echo(f"❌ Error: {e}")
        raise typer.Exit(1)


# ──────────────────────── Settings 子命令 ────────────────────────

settings_app = typer.Typer(help="管理 qtask 全局设置")
app.add_typer(settings_app, name="settings")


@settings_app.command("show")
def cmd_settings_show(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """显示当前全局设置"""
    r = get_redis_client(redis_url)
    from qtask.history import DEFAULT_KEEP_DAYS, SETTINGS_KEY
    raw = r.hgetall(SETTINGS_KEY)
    keep_days = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
    typer.echo(f"\n⚙️  qtask Global Settings")
    typer.echo(f"   history_keep_days = {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} days")


@settings_app.command("set")
def cmd_settings_set(
    keep_days: int = typer.Option(..., "--keep-days", "-k", help="保留历史的天数（1-365）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """设置历史任务保留天数"""
    if keep_days < 1 or keep_days > 365:
        typer.echo("❌ keep_days 必须在 1 到 365 之间")
        raise typer.Exit(1)
    r = get_redis_client(redis_url)
    from qtask.history import TaskHistoryStore
    TaskHistoryStore.set_keep_days(r, keep_days)
    typer.echo(f"✅ history_keep_days 已设置为 {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} 天")


def main():
    try:
        app()
    except Exception as e:
        logger.error(f"CLI Error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
