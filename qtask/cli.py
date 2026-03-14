import typer
import redis
from loguru import logger
import sys

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
    redis_url: str = typer.Option("redis://localhost:6379/0", help="Redis connection URL")
):
    """Re-queue all messages from DLQ back into the main queue"""
    r = get_redis_client(redis_url)
    q_name = f"{queue_name}:stream"
    dlq_name = f"{queue_name}:stream_dlq"
    
    try:
        msgs = r.xrange(dlq_name, min="-", max="+")
        if not msgs:
            typer.echo(f"✅ DLQ {dlq_name} is empty. Nothing to requeue.")
            return
            
        typer.echo(f"🔄 Re-queuing {len(msgs)} messages from {dlq_name} to {q_name}...")
        
        # 使用 Pipeline 批量提交，保证效率
        pipe = r.pipeline()
        for msg_id, payload in msgs:
            # 取出原始 payload
            orig_payload = payload.get("payload")
            if orig_payload:
                # 重新作为全新任务放回主队列
                pipe.xadd(q_name, {"payload": orig_payload})
                # 从死信队列删除
                pipe.xdel(dlq_name, msg_id)
        
        pipe.execute()
        typer.echo(f"🎉 Successfully re-queued {len(msgs)} messages!")
        
    except redis.exceptions.ResponseError as e:
        typer.echo(f"❌ Error: {e}")

def main():
    try:
        app()
    except Exception as e:
        logger.error(f"CLI Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
