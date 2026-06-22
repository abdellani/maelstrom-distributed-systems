# Fly.io Distributed Systems Challenges

Solutions for the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/), using Maelstrom.

## Running tests
Build code 
```bash
cd src
go build main.go
cd ..
```

Run the echo challenge:

```bash
./maelstrom/maelstrom test \
  -w echo \
  --bin ./src/main \
  --node-count 1 \
  --time-limit 10
```

Run the unique IDs challenge:

```bash
./maelstrom/maelstrom test \
  -w unique-ids \
  --bin ./src/main \
  --node-count 3 \
  --time-limit 30 \
  --availability total \
  --nemesis partition
```