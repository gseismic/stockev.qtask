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
    queue_name: str = typer.Argument(..., help="The base name of the queue (e.g., spider:tasks)"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Show total items and status of a queue"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        length = r.xlen(q_name)
        typer.echo(f"📊 Queue: {q_name}")
        typer.echo(f"   => Length: {length}")
        
        info = r.xinfo_stream(q_name)
        typer.echo(f"   => Last Entry ID: {info.get('last-generated-id')}")
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            typer.echo(f"⚠️ Queue {q_name} does not exist or is empty.")
        else:
            typer.echo(f"❌ Error: {e}")

@app.command("groups")
def cmd_groups(
    queue_name: str = typer.Argument(..., help="The base name of the queue (e.g., spider:tasks)"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Show consumer groups and pending states"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    try:
        groups = r.xinfo_groups(q_name)
        if not groups:
            typer.echo(f"ℹ️ No consumer groups found for {q_name}.")
            return
            
        typer.echo(f"👥 Consumer Groups for {q_name}:")
        for g in groups:
            name = g.get('name')
            consumers = g.get('consumers')
            pending = g.get('pending')
            last_id = g.get('last-delivered-id')
            typer.echo(f"  - Group: [{name}] | Consumers: {consumers} | Pending: {pending} | Last Delivered ID: {last_id}")
            
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
             typer.echo(f"⚠️ Queue {q_name} does not exist.")
        else:
             typer.echo(f"❌ Error: {e}")

@app.command("dlq")
def cmd_dlq(
    queue_name: str = typer.Argument(..., help="The base name of the underlying queue (e.g., spider:tasks)"),
    preview: bool = typer.Option(False, "--preview", help="Preview the last 5 failed messages"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Show Dead Letter Queue status and failed items"""
    r = get_redis_client(redis_url)
    dlq_name = f"{queue_name}:stream_dlq"
    try:
        length = r.xlen(dlq_name)
        typer.echo(f"💀 Dead Letter Queue: {dlq_name}")
        typer.echo(f"   => Length: {length}")
        
        if length > 0 and preview:
            msgs = r.xrange(dlq_name, count=min(length, 5))
            typer.echo("\n   [Preview top 5 failed messages]")
            for msg_id, payload in msgs:
                data = payload.get("payload", "")
                if len(data) > 80:
                    data = data[:80] + "..."
                orig_id = payload.get("original_id", "unknown")
                typer.echo(f"      ID: {msg_id} (Original ID: {orig_id}) -> Data: {data}")
                
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")

@app.command("requeue")
def cmd_requeue(
    queue_name: str = typer.Argument(..., help="The base name of the underlying queue (e.g., spider:tasks)"),
    task_id: str = typer.Option(None, "--task-id", help="Requeue a specific task ID instead of all tasks"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Re-queue messages from DLQ back into the main queue"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    dlq_name = f"{queue_name}:stream_dlq"
    
    try:
        if task_id:
            msgs = r.xrange(dlq_name, min=task_id, max=task_id)
            if not msgs:
                typer.echo(f"⚠️ Task ID {task_id} not found in DLQ.")
                return
            typer.echo(f"🔄 Re-queuing task {task_id} from {dlq_name} to {q_name}...")
        else:
            msgs = r.xrange(dlq_name, min="-", max="+")
            if not msgs:
                typer.echo(f"✅ DLQ {dlq_name} is empty. Nothing to requeue.")
                return
            typer.echo(f"🔄 Re-queuing all {len(msgs)} messages from {dlq_name} to {q_name}...")
        
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            orig_payload = payload.get("payload")
            if orig_payload:
                pipe.xadd(q_name, {"payload": orig_payload})
                pipe.xdel(dlq_name, msg_id)
        
        pipe.execute()
        typer.echo(f"🎉 Successfully re-queued {len(msgs)} messages!")
        
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")

@app.command("claim")
def cmd_claim(
    queue_name: str = typer.Argument(..., help="The base name of the underlying queue (e.g., spider:tasks)"),
    group: str = typer.Option("default_group", "--group", help="Consumer group name"),
    idle_ms: int = typer.Option(300000, "--idle-ms", help="Only claim messages idle for more than this ms (default 5 mins)"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Admin: Force claim all zombie pending messages"""
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    try:
        count = q.claim_all(idle_time_ms=idle_ms)
        typer.echo(f"🎉 Successfully claimed {count} zombie messages!")
    except Exception as e:
        typer.echo(f"❌ Error: {e}")

@app.command("reset")
def cmd_reset(
    queue_name: str = typer.Argument(..., help="The base name of the underlying queue (e.g., spider:tasks)"),
    group: str = typer.Option("default_group", "--group", help="Consumer group name"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL"),
    force: bool = typer.Option(False, "--force", "-f", help="Bypass confirmation prompt")
):
    """Admin: Reset consumer group cursor to the latest message ($)"""
    if not force:
        typer.confirm(f"Are you sure you want to reset group '{group}' on queue '{queue_name}'?\nThis will ignore all currently backlogged messages in the stream.", abort=True)
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name, worker_group=group)
    if q.reset_group():
        typer.echo(f"✅ Cursor for group {group} reset to latest ($).")
    else:
        typer.echo("❌ Failed to reset group.")

@app.command("clear")
def cmd_clear(
    queue_name: str = typer.Argument(..., help="The base name of the underlying queue (e.g., spider:tasks)"),
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL"),
    force: bool = typer.Option(False, "--force", "-f", help="Bypass confirmation prompt")
):
    """Admin: DANGER! Purge all data in the stream and its DLQ"""
    if not force:
        typer.confirm(f"DANGER ZONE: Are you permanently deleting ALL data in queue '{queue_name}' and its DLQ?", abort=True)
    from qtask.queue import SmartQueue
    q = SmartQueue(redis_url, queue_name)
    if q.clear_all():
        typer.echo(f"💣 Stream {queue_name} has been completely wiped and recreated.")
    else:
        typer.echo("❌ Failed to clear queue.")


# ──────────────────────── 新增：历史查询命令 ────────────────────────

@app.command("history")
def cmd_history(
    queue_name: str = typer.Argument(..., help="队列基础名称（如 spider:tasks）"),
    status: str = typer.Option("all", "--status", "-s", help="筛选状态: all | pending | completed | failed"),
    days: int = typer.Option(None, "--days", "-d", help="最近 N 天（不指定则使用全局 keep_days 设置）"),
    limit: int = typer.Option(50, "--limit", "-n", help="最多显示条数"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """查看任务历史（已完成/失败/待处理/重试次数）"""
    r = get_redis_client(redis_url)
    base_name = queue_name.replace(":stream", "")

    try:
        from qtask.history import TaskHistoryStore
        hist = TaskHistoryStore(r, base_name)
        keep_days_used = days if days is not None else hist.get_keep_days()
        tasks = hist.get_tasks(status=status, limit=limit, days=days)
        counts = hist.count_by_status()

        typer.echo(f"\n📋 History for queue: {typer.style(base_name, bold=True)}")
        typer.echo(f"   Range: last {keep_days_used} days | Filter: {status} | Showing: {len(tasks)}/{limit}")
        typer.echo(f"   ✅ completed={counts['completed']}  ❌ failed={counts['failed']}  ⏳ pending={counts['pending']}  total={counts['total']}\n")

        if not tasks:
            typer.echo("   (no records found)")
            return

        # 表头
        header = f"{'TASK ID':<28} {'ACTION':<20} {'STATUS':<12} {'RETRIES':>7}  {'DURATION':>9}  {'CREATED'}"
        typer.echo(typer.style(header, fg=typer.colors.BRIGHT_BLACK))
        typer.echo("─" * 90)

        import datetime
        for t in tasks:
            status_str = t.get("status", "?")
            color = {"completed": typer.colors.GREEN, "failed": typer.colors.RED, "pending": typer.colors.YELLOW}.get(status_str, typer.colors.WHITE)
            retries = int(t.get("retries", 0))
            duration = t.get("duration_s")
            dur_str = f"{duration:.2f}s" if duration is not None else "-"
            created_ts = t.get("created_at", 0)
            created_str = datetime.datetime.fromtimestamp(created_ts).strftime("%m-%d %H:%M:%S") if created_ts else "-"

            row = (
                f"{t.get('task_id', '?'):<28} "
                f"{t.get('action', '?'):<20} "
                f"{typer.style(f'{status_str:<12}', fg=color)} "
                f"{retries:>7}  "
                f"{dur_str:>9}  "
                f"{created_str}"
            )
            typer.echo(row)

        if tasks and tasks[0].get("fail_reason"):
            typer.echo(f"\n   💬 Last fail reason: {tasks[0]['fail_reason'][:120]}")

    except Exception as e:
        typer.echo(f"❌ Error: {e}")
        raise typer.Exit(1)


# ──────────────────────── 新增：全局设置命令 ────────────────────────

settings_app = typer.Typer(help="管理 qtask 全局设置")
app.add_typer(settings_app, name="settings")

@settings_app.command("show")
def cmd_settings_show(
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """显示当前全局设置"""
    r = get_redis_client(redis_url)
    try:
        from qtask.history import TaskHistoryStore, DEFAULT_KEEP_DAYS, SETTINGS_KEY
        raw = r.hgetall(SETTINGS_KEY)
        keep_days = int(raw.get("history_keep_days", DEFAULT_KEEP_DAYS))
        typer.echo(f"\n⚙️  qtask Global Settings")
        typer.echo(f"   history_keep_days = {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} days")
    except Exception as e:
        typer.echo(f"❌ Error: {e}")

@settings_app.command("set")
def cmd_settings_set(
    keep_days: int = typer.Option(..., "--keep-days", "-k", help="保留历史任务的天数（1-365）"),
    redis_url: str = typer.Option("redis://localhost:6379/0", "--redis-url", help="Redis 连接 URL"),
):
    """设置历史任务保留天数（K）"""
    if keep_days < 1 or keep_days > 365:
        typer.echo("❌ keep_days 必须在 1 到 365 之间")
        raise typer.Exit(1)
    r = get_redis_client(redis_url)
    try:
        from qtask.history import TaskHistoryStore
        TaskHistoryStore.set_keep_days(r, keep_days)
        typer.echo(f"✅ history_keep_days 已设置为 {typer.style(str(keep_days), bold=True, fg=typer.colors.CYAN)} 天")
    except Exception as e:
        typer.echo(f"❌ Error: {e}")


def main():
    try:
        app()
    except Exception as e:
        logger.error(f"CLI Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
