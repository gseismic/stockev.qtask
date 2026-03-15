# qtask Agent Instructions

This file provides instructions for AI agents working with the qtask project.

## Quick Reference

### Running Tests
```bash
cd /home/lsl/macbook/pai/stockev.qtask
source venv/bin/activate
pytest tests/ -v
```

### Running Examples
```bash
# Terminal 1: Start storage server
cd server
export QTASK_ADMIN_USER=admin QTASK_ADMIN_PASS=admin123 QTASK_ADMIN_TOKEN=mytoken
python storage_server.py

# Terminal 2: Run generator
python examples/run_generator.py

# Terminal 3: Run worker
python examples/run_scraper.py
```

### CLI Commands
```bash
# Check queue status
qtask index myproj:spider:tasks

# Check history
qtask history myproj:spider:tasks --status failed

# Clean up
qtask ns purge myproj -f
```

## Code Structure

```
qtask/
  __init__.py     # Exports: SmartQueue, RemoteStorage, Worker
  queue.py        # SmartQueue class
  worker.py       # Worker class  
  storage.py      # RemoteStorage class
  history.py      # TaskHistoryStore class
  cli.py          # CLI commands

server/
  storage_server.py  # FastAPI server

examples/
  run_generator.py   # Producer example
  run_scraper.py     # Worker example (with result queue)
  run_store_result.py # Result consumer example
```

## Key Classes

### SmartQueue
- `push(payload)` - Add task to queue
- `pop_blocking(group, worker_id, timeout)` - Get task
- `ack(msg_context)` - Acknowledge task
- `fail(msg_context, reason)` - Move to DLQ

### Worker
- `@worker.on(action_name)` - Register handler
- `worker.run()` - Start consuming

### RemoteStorage
- `save(data)` / `save_bytes(data)` - Upload large data
- `load(key)` - Download large data
- `delete(key)` - Delete large data
