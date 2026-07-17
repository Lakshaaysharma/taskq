# taskqctl

A command-line operations tool for [taskq](../README.md). It reads and writes the
same Redis keyspace the C++ library uses, so you can inspect and control queues
without touching C++.

## Install

```bash
cd cli
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

The Redis connection is resolved from, in increasing priority:

1. Built-in default — `redis://127.0.0.1:6379/0`
2. `~/.config/taskq/config.json` — `{"redis_url": "redis://..."}`
3. `TASKQ_REDIS_URL` environment variable
4. `--url` flag

## Commands

| Command | Description |
| --- | --- |
| `queues` | List every queue with per-state counts and pause status. |
| `queue <name>` | Show task ids in a queue, grouped by state (`--state`, `--limit`). |
| `task <id>` | Dump the full record of one task. |
| `pause <name>` | Stop workers from dispatching a queue. |
| `resume <name>` | Resume a paused queue. |
| `requeue <name>` | Move `dead` (or `--state scheduled`) tasks back to pending. |
| `stats` | Aggregate totals across all queues. |

Every command accepts `--json` for scripting.

```bash
python3 taskqctl.py queues
python3 taskqctl.py queue default --state dead
python3 taskqctl.py task 519fa437-962a-45be-a641-ffb127e50ad5 --json
python3 taskqctl.py requeue default --state dead
```
