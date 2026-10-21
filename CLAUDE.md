# hive-server

The hub of the Hive. The Hive is a swarm of small language models running in people's browser tabs: anyone can open the page, watch the swarm work, ask it a question, or join it as a drone. This repo is the hub, a Rust WebSocket server that connects the drones, sends each question to them as tasks, and broadcasts a public log of what moves through the swarm. It never runs a model.

The page lives in the sibling repo, hive-client. You need both to see the Hive work.

## Run it

```
cargo run --release
```

The hub listens on `ws://0.0.0.0:8080/ws`. Settings, all optional, as environment variables:

- `HUB_HOST`, `HUB_PORT`: where to listen (default `0.0.0.0`, `8080`).
- `HUB_ALLOWED_ORIGINS`: the web pages allowed to connect, comma-separated (default: localhost pages only).

Then start the page from hive-client and open it.

## Check it

`scripts/ci.sh` runs the format check, clippy, the tests and the release build. It must pass before every commit.

## Principles

Read these before you change anything.

1. **Small on purpose.** What looks missing is left out by design: no database, no accounts, no search, no framework, no settings beyond the few listed here. A change does one thing; refactors, renames, reformatting and new dependencies are not part of it. A change comes with tests for the code it touches.
2. **No special cases.** One rule for every message, every peer and every cell. A fix that needs an `if` for one situation is the wrong fix.
3. **The hub stays dumb.** It routes messages, tracks peers, writes and reads files, and broadcasts log lines. It never runs a model and never decides what a drone could decide. If it smells like thinking, it belongs in a drone.
4. **Every job is a task for the swarm.** Answering, judging, classifying, archiving and telling the time are all tasks sent to drones.
5. **Storage is JSONL on disk.** One file per cell. No database, now or later.
6. **JSONL to store, JSON to send, compact YAML to put in prompts.**
7. **Retrieval, not search.** From the question's cell: lines whose tags appear in the question, then the most recent, at most 10. No embeddings, no index.
8. **The swarm's memory adds to the model's knowledge.** It never replaces it.
9. **The public sees the transport, never the words.** The feed shows ids, routing, cells and timings. What a user typed, and the answer they got, go only to that user. Text no user was involved in, like the hive's own time sentence, may be shown.
10. **The page is a log feed.** Watching it should feel like watching infrastructure. Plain HTML and JavaScript: no framework, no components, no chat bubbles.
11. **Drones pick their engine by what the device can do,** never by device name, and nothing is tuned per device.
12. **Simple words** in logs, messages, comments and docs.

## Code

- Rust with `tokio`, `tokio-tungstenite` and `serde`. Add no other kind of dependency.
- `src/hub.rs`: routing, tasks, timeouts. `src/protocol.rs`: the messages exchanged with the page. `src/log.rs`: every public log line. `src/validate.rs`: checks on what peers send.
