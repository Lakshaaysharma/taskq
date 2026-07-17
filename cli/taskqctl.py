#!/usr/bin/env python3
"""taskqctl — a command-line operations tool for the taskq task queue.

Talks directly to the same Redis keyspace the C++ library uses, so you can
inspect queues, drill into individual tasks, pause/resume processing, and
requeue dead-lettered work without writing any C++.

Configuration is resolved in this order (later wins):
  1. built-in defaults (redis://127.0.0.1:6379/0)
  2. ~/.config/taskq/config.json
  3. TASKQ_REDIS_URL environment variable
  4. --url command-line flag

Examples:
  taskqctl queues
  taskqctl queue default
  taskqctl task 519fa437-962a-45be-a641-ffb127e50ad5
  taskqctl pause default
  taskqctl requeue default --state dead
  taskqctl queues --json
"""
import json
import os
import sys
from pathlib import Path

try:
    import click
    import redis
    from rich.console import Console
    from rich.table import Table
except ImportError:
    sys.stderr.write(
        "Missing dependencies. Install them with:\n"
        "    pip install -r requirements.txt\n"
    )
    sys.exit(1)

console = Console()

# Mirror of the key layout defined in taskq.hpp.
PREFIX = "taskq"
STATES = ["pending", "scheduled", "active", "completed", "dead"]


def k_queues() -> str:
    return f"{PREFIX}:queues"


def k_pending(q: str) -> str:
    return f"{PREFIX}:queue:{q}:pending"


def k_active(q: str) -> str:
    return f"{PREFIX}:queue:{q}:active"


def k_scheduled(q: str) -> str:
    return f"{PREFIX}:queue:{q}:scheduled"


def k_completed(q: str) -> str:
    return f"{PREFIX}:queue:{q}:completed"


def k_dead(q: str) -> str:
    return f"{PREFIX}:queue:{q}:dead"


def k_paused(q: str) -> str:
    return f"{PREFIX}:queue:{q}:paused"


def k_task(task_id: str) -> str:
    return f"{PREFIX}:task:{task_id}"


def resolve_url(flag_url: str | None) -> str:
    """Layer the config sources described in the module docstring."""
    url = "redis://127.0.0.1:6379/0"
    cfg_path = Path.home() / ".config" / "taskq" / "config.json"
    if cfg_path.exists():
        try:
            data = json.loads(cfg_path.read_text())
            url = data.get("redis_url", url)
        except (json.JSONDecodeError, OSError):
            console.print(f"[yellow]warning:[/] could not read {cfg_path}")
    url = os.environ.get("TASKQ_REDIS_URL", url)
    if flag_url:
        url = flag_url
    return url


def queue_counts(r: redis.Redis, q: str) -> dict:
    """Return the size of every state bucket for a queue in one pipeline."""
    pipe = r.pipeline()
    pipe.llen(k_pending(q))
    pipe.zcard(k_scheduled(q))
    pipe.zcard(k_active(q))
    pipe.llen(k_completed(q))
    pipe.llen(k_dead(q))
    pipe.exists(k_paused(q))
    pending, scheduled, active, completed, dead, paused = pipe.execute()
    return {
        "pending": pending,
        "scheduled": scheduled,
        "active": active,
        "completed": completed,
        "dead": dead,
        "paused": bool(paused),
    }


def list_queues(r: redis.Redis) -> list[str]:
    return sorted(r.smembers(k_queues()))


STATE_COLORS = {
    "pending": "cyan",
    "scheduled": "blue",
    "active": "yellow",
    "completed": "green",
    "dead": "red",
}


def json_option(fn):
    """Add a `--json` flag to a command and store it on the context.

    Defined per-command (rather than only on the group) so it works in the
    natural position, e.g. `taskqctl stats --json`, not just before the
    subcommand name.
    """
    def callback(ctx, _param, value):
        if value:
            ctx.obj["json"] = True
        return value

    return click.option(
        "--json", "as_json", is_flag=True, expose_value=False, is_eager=True,
        callback=callback, help="Emit machine-readable JSON.",
    )(fn)


@click.group(help="Operations tool for the taskq task queue.")
@click.option("--url", default=None, help="Redis URL, e.g. redis://host:6379/0")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON.")
@click.pass_context
def cli(ctx, url, as_json):
    ctx.ensure_object(dict)
    resolved = resolve_url(url)
    try:
        ctx.obj["r"] = redis.from_url(resolved, decode_responses=True)
        ctx.obj["r"].ping()
    except redis.RedisError as e:
        console.print(f"[red]Cannot connect to Redis at {resolved}: {e}[/]")
        sys.exit(1)
    ctx.obj["json"] = as_json
    ctx.obj["url"] = resolved


@cli.command(help="List all queues with per-state counts.")
@json_option
@click.pass_context
def queues(ctx):
    r = ctx.obj["r"]
    names = list_queues(r)
    rows = [{"queue": q, **queue_counts(r, q)} for q in names]

    if ctx.obj["json"]:
        click.echo(json.dumps(rows, indent=2))
        return

    if not rows:
        console.print("[dim]No queues found.[/]")
        return

    table = Table(title="taskq queues", header_style="bold")
    table.add_column("Queue")
    for s in STATES:
        table.add_column(s.capitalize(), justify="right")
    table.add_column("Paused", justify="center")
    for row in rows:
        table.add_row(
            row["queue"],
            *[f"[{STATE_COLORS[s]}]{row[s]}[/]" if row[s] else "0" for s in STATES],
            "[red]yes[/]" if row["paused"] else "[green]no[/]",
        )
    console.print(table)


@cli.command(help="Show the tasks in one queue, grouped by state.")
@click.argument("queue")
@click.option("--state", type=click.Choice(STATES), default=None, help="Filter to one state.")
@click.option("--limit", default=20, help="Max task ids to show per state.")
@json_option
@click.pass_context
def queue(ctx, queue, state, limit):
    r = ctx.obj["r"]
    if queue not in list_queues(r):
        console.print(f"[yellow]Queue '{queue}' is not registered.[/]")
        return

    getters = {
        "pending": lambda: r.lrange(k_pending(queue), 0, limit - 1),
        "scheduled": lambda: r.zrange(k_scheduled(queue), 0, limit - 1),
        "active": lambda: r.zrange(k_active(queue), 0, limit - 1),
        "completed": lambda: r.lrange(k_completed(queue), 0, limit - 1),
        "dead": lambda: r.lrange(k_dead(queue), 0, limit - 1),
    }
    wanted = [state] if state else STATES

    result = {s: getters[s]() for s in wanted}
    if ctx.obj["json"]:
        click.echo(json.dumps(result, indent=2))
        return

    counts = queue_counts(r, queue)
    flag = " [red](paused)[/]" if counts["paused"] else ""
    console.print(f"[bold]Queue:[/] {queue}{flag}")
    for s in wanted:
        ids = result[s]
        console.print(f"\n[{STATE_COLORS[s]}]{s}[/] ({counts[s]})")
        for tid in ids:
            console.print(f"  {tid}")
        if not ids:
            console.print("  [dim](none)[/]")


@cli.command(help="Dump the full record of a single task.")
@click.argument("task_id")
@json_option
@click.pass_context
def task(ctx, task_id):
    r = ctx.obj["r"]
    data = r.hgetall(k_task(task_id))
    if not data:
        console.print(f"[yellow]No task with id {task_id}.[/]")
        sys.exit(1)
    if ctx.obj["json"]:
        click.echo(json.dumps(data, indent=2))
        return

    table = Table(show_header=False, box=None)
    table.add_column("field", style="bold cyan")
    table.add_column("value")
    for key in ["id", "type", "queue", "state", "retryCount", "maxRetries",
                "createdAt", "updatedAt", "payload", "result", "lastError"]:
        if key in data:
            table.add_row(key, data[key])
    console.print(table)


@cli.command(help="Pause a queue (workers stop dispatching from it).")
@click.argument("queue")
@click.pass_context
def pause(ctx, queue):
    ctx.obj["r"].set(k_paused(queue), "1")
    console.print(f"[green]Paused[/] queue '{queue}'.")


@cli.command(help="Resume a paused queue.")
@click.argument("queue")
@click.pass_context
def resume(ctx, queue):
    ctx.obj["r"].delete(k_paused(queue))
    console.print(f"[green]Resumed[/] queue '{queue}'.")


@cli.command(help="Move tasks from a state back onto the pending list.")
@click.argument("queue")
@click.option("--state", type=click.Choice(["dead", "scheduled"]), default="dead",
              help="Which bucket to requeue from.")
@click.confirmation_option(prompt="Requeue these tasks?")
@click.pass_context
def requeue(ctx, queue, state):
    r = ctx.obj["r"]
    src = k_dead(queue) if state == "dead" else k_scheduled(queue)
    ids = r.lrange(src, 0, -1) if state == "dead" else r.zrange(src, 0, -1)
    if not ids:
        console.print(f"[dim]Nothing in {state} for '{queue}'.[/]")
        return
    pipe = r.pipeline()
    for tid in ids:
        if state == "dead":
            pipe.lrem(src, 0, tid)
        else:
            pipe.zrem(src, tid)
        pipe.lpush(k_pending(queue), tid)
        pipe.hset(k_task(tid), mapping={"state": "pending", "retryCount": 0})
    pipe.execute()
    console.print(f"[green]Requeued {len(ids)} task(s)[/] from {state} to pending.")


@cli.command(help="Aggregate totals across all queues.")
@json_option
@click.pass_context
def stats(ctx):
    r = ctx.obj["r"]
    totals = {s: 0 for s in STATES}
    for q in list_queues(r):
        counts = queue_counts(r, q)
        for s in STATES:
            totals[s] += counts[s]
    if ctx.obj["json"]:
        click.echo(json.dumps(totals, indent=2))
        return
    table = Table(title="taskq totals", header_style="bold")
    for s in STATES:
        table.add_column(s.capitalize(), justify="right")
    table.add_row(*[f"[{STATE_COLORS[s]}]{totals[s]}[/]" for s in STATES])
    console.print(table)


if __name__ == "__main__":
    cli()
