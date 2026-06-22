# Fly.io Distributed Systems Challenges

Solutions for the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/), using Maelstrom.

## Running tests

Run the echo challenge:

```bash
./maelstrom/maelstrom test \
  -w echo \
  --bin ./echo/main \
  --node-count 1 \
  --time-limit 10
```
