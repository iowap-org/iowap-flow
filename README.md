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
  at submit time; every referenced task must be in `depends_on`. The `path`
  segments are the field names of the capability's result - listed per
  capability as result paths in the capabilities snapshot (T-005c: paths are
  relative to the handler result envelope's inner `result` object).

  Example: a `chat.ai` task `t1` (hints `["answer"]`) followed by a task
  whose payload reads `{"summary": "${t1.result.answer}"}` - after flow
  unwraps the handler result envelope exactly once, the template resolves to
  `aggregate["t1"]["answer"]`, the chat answer itself.
- **Long-run safe** - `long_run` capability profile + keepalive notes hold the
  origin stage lease for the entire flow duration.
- **Repo-tracked agent.ai handler** - `handlers/agent_ai.py` serves the
  capability `agent.ai` (Hermes planner) and sends a T-154 longrun note right
  after claim, with keepalive notes during the LLM call (T-001c).

Built on [`iowap-node`](https://github.com/iowap-org/iowap-node) (pinned
dependency) for the node daemon, relay client, and handler runner.

Status: in development (T-172). See `docs/` in the iowap-server repo for the
concept once published.

## License

MIT — see [LICENSE](LICENSE).
