# iowap-flow

**Flow Runner** for the IOWAP relay cluster - an orchestrator node with the
capability `flow.run`, implementing a *dumb Kanban*: it takes an origin task,
orchestrates planning via an `agent.ai` planner task, validates the plan
against live capability discovery, fans every plan step out as an ordinary
relay task, polls until the join completes, and aggregates results back to the
origin task.

Design goals:

- **Zero server changes** - builds entirely on existing relay APIs
  (task-simple submit, task notes, task status, discovery).
- **Fail-fast** - a permanently failed child fails the whole flow with a
  clear reason; the runner is deliberately non-creative.
- **Data injection** - payload fields can reference earlier task results via
  `${ref.result.path}` templates (T-002), resolved from the running aggregate
  at submit time; every referenced task must be in `depends_on`.
- **Long-run safe** - `long_run` capability profile + keepalive notes hold the
  origin stage lease for the entire flow duration.

Built on [`iowap-node`](https://github.com/iowap-org/iowap-node) (pinned
dependency) for the node daemon, relay client, and handler runner.

Status: in development (T-172). See `docs/` in the iowap-server repo for the
concept once published.