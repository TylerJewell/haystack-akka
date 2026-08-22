# haystack-akka

Decides which of a graph of components runs next, what values it receives, and when a
run stops — including a graph that loops back on itself.

A port of [deepset-ai/haystack](https://github.com/deepset-ai/haystack) onto **Akka**,
built with **Akka Specify**.

---

## Where it came from

deepset-ai/haystack is a Python framework for building AI pipelines: you wire named
components together into a graph, and its engine works out which one runs next, what it
receives, and how its output reaches the next one — including graphs that branch and
graphs that loop. It was ported to derive a specification format precise enough to
regenerate a system on a different stack — the port is the vehicle, the specification is
the deliverable.

The specifications the port was generated from are in
[TylerJewell/akka-specify-harness](https://github.com/TylerJewell/akka-specify-harness)
under `haystack-port/`.

---

## deepset-ai/haystack → this port

📉 1,066 Python lines → **589 Java lines**<br>
📁 3 files → **14 files**<br>
⚡ 111,136-359,383 → **3,046-12,696** nanoseconds per pipeline run, across 8 graph shapes<br>
🎯 8 of 8 → **8 of 8** graph runs giving the same answer<br>
🧪 not measured → **7 of 7** deliberate breakages caught by a check

Full method and the numbers that did *not* make this list:
[`bench/REPORT.md`](https://github.com/TylerJewell/akka-specify-harness/blob/main/haystack-port/bench/REPORT.md).

---

## What it took to build

⏱️ **1.1 hours** from the first command to the published repository, **1.1** of them active<br>
💬 **584** exchanges with the model<br>
✍️ **414,599** tokens written by the model, **157,969,588** counting everything sent and re-sent<br>
🙋 **0** questions to a human<br>
🧪 **15** tests

```bash
python toolkit/tokens.py --port haystack    # turns, tokens, elapsed and active time
```

The record of every question, and where the time went, is in
[`port-log/`](https://github.com/TylerJewell/akka-specify-harness/tree/main/port-log).

---

## What it does

From the specification:

- **A component's output reaches every wired receiver, whether or not it actually sent
  that receiver anything.** A receiver whose sender skipped it that round simply never
  runs — no error, no wait. Which branch of a graph fires is entirely up to what each
  component itself returns, never a separate decision the engine makes.
- **A socket that merges several senders either waits for one of them or waits for all
  of them, and the two are declared, not guessed.** One kind runs the moment any single
  sender delivers, keeping only the latest value if several arrive before it is
  scheduled; the other collects one value per sender, in the order they were wired, and
  is content to run with a partial set if one of its senders is permanently unreachable.
- **A loop closes by a component simply choosing not to send back around it anymore.**
  There is no separate "exit the loop" concept — a loop stops for exactly the same
  reason any branch stops: nobody sent anything down it this time.
- **The only thing that stops a loop with no exit is a flat cap on how many times any
  one component may run.** It applies the same way inside a loop or out of one, and
  going past it fails the whole run loudly rather than spinning forever.
- **When two components are both ready to go and neither depends on the other, the one
  whose name sorts first goes first.** Components that sit inside the same loop are
  treated as equally ready and are ordered by name too, so which one of a graph's several
  waiting parts runs next never comes down to chance.

---

## Design decisions

**A run's pending values live in one place, not scattered across components.** Every
socket's arrived-and-not-yet-used values sit in a single map the run carries with it, so
deciding what any component sees next never means asking the component itself.

**Kinds of socket are written down, not guessed from a type.** A socket says up front
whether it takes one sender, waits for all of several, or runs on the first of several —
a plain fact about the socket rather than something worked out by inspecting what type
of value it carries.

**A graph-shaped run is described as data, not as code that builds it.** Which
components exist, how they're wired, and what starting values they get all arrive as one
JSON document over HTTP, so trying a new graph shape never means writing and compiling a
new program.

**A small, fixed set of component behaviours, not an open door for arbitrary code.** A
caller picks from a short list of building blocks — pass a value through, route it one of
two ways, merge several into one — rather than supplying code of their own, because
accepting somebody else's code to run is a different kind of promise than this port
makes.

**A cap that fails loudly rather than a loop that quietly never ends.** A component that
would run past its allowed number of turns stops the whole run with an error naming
every component's turn count at that moment, rather than letting a mistaken graph spin
forever.

---

## Running it — the short path

You do not need Java, Maven, or the Akka CLI installed. Akka Specify installs them for you.

**1. Install Akka Specify** in Claude Code:

```
/plugin marketplace add akka/ai-marketplace
/plugin install akka@akka-ai-marketplace
```

Restart Claude Code when it asks.

**2. Give it this prompt:**

> Clone https://github.com/TylerJewell/haystack-akka into a new directory and open it.
> Then run /akka:setup to install everything this project needs, and /akka:build to
> compile it, run the tests, and start it locally.

**3. Open** http://localhost:9049.

---

## Running it — the developer path

### Requirements

- Java 21 or newer
- Maven 3.9 or newer
- An Akka download token — run `akka code token` once

### Start the service

```bash
mvn compile
akka local run
```

The service starts on **port 9049**.

### Try it

```bash
# a router with two branches: only the taken one ever runs
curl -s localhost:9049/pipelines/run -H 'content-type: application/json' -d '{
  "components": [
    {"name": "router", "type": "parity_router"},
    {"name": "evenSink", "type": "passthrough"},
    {"name": "oddSink", "type": "passthrough"}
  ],
  "connections": ["router.even -> evenSink.value", "router.odd -> oddSink.value"],
  "externalInputs": {"router": {"value": 4}}
}'

# a loop that closes itself after five turns
curl -s localhost:9049/pipelines/run -H 'content-type: application/json' -d '{
  "components": [
    {"name": "seed", "type": "constant", "config": {"value": 0}},
    {"name": "counter", "type": "loop_until", "config": {"limit": 5}},
    {"name": "passthrough", "type": "passthrough"}
  ],
  "connections": [
    "seed.value -> counter.value",
    "counter.loopBack -> passthrough.value",
    "passthrough.value -> counter.value"
  ]
}'
```

---

## Configuration

| Variable | Default | Notes |
|---|---|---|
| none | — | The service reads no environment variables. The port it listens on is set in `src/main/resources/application.conf`. |

This port calls no model provider, so it needs no key for one.

---

## Where it differs from deepset-ai/haystack

Everything not listed here behaves the same way on purpose, including the parts that
look like mistakes.

- **What a component may be.** haystack accepts any Python object with a `run` method as
  a component. This port accepts only a fixed, short list of built-in behaviours,
  because taking arbitrary code from a caller over HTTP is a different promise than this
  port makes; the graph-wiring and scheduling rules this port rebuilds are unaffected by
  which components sit in the graph.
- **Whether connected sockets are checked for matching types.** haystack refuses to wire
  two sockets together at all if their declared types do not match. This port performs
  no such check — every socket carries whatever value arrives — because the slice this
  port rebuilds is which component runs next and with what, not a type system components
  are written against.
- **How a socket's kind is decided.** haystack works out whether a socket takes one
  sender, waits for all of several, or runs on the first of several by inspecting a
  Python type annotation the moment a second connection is made. This port is told the
  kind directly when a component is registered. The three kinds and the rules for each
  are unchanged; only how the choice is recorded differs.
- **What the error report says when a component runs too many times.** haystack's error
  names only the one component that went over. This port's names every component's turn
  count at the moment of failure, because the caller debugging a mistaken graph over HTTP
  has no debugger to attach the way haystack's own caller does.
- **What a finished run reports.** haystack, by default, leaves out any component's
  output that another component already consumed. This port always reports every
  component's full output, because the point of calling this port from outside is to see
  what happened, and a caller with no access to haystack's Python return value has no
  other way to find out.
- **Which of two components that don't depend on each other, and sit in different loops
  or chains, goes first.** haystack leaves this to a detail of the graph library it is
  built on and makes no promise about it. This port always goes by name. Where the two
  components sit in the very same loop, both sides agree: whichever name sorts first
  goes first.
- **Rendering a graph, saving one to a file, and reading one back from a file.** haystack
  can draw a pipeline as a picture and save and load one as text. This port does none of
  these — a run is described as data over one HTTP call and nothing is kept afterwards.
- **Running several components at once.** haystack can run independent components
  concurrently. This port runs one at a time, always. **Not checked** against the
  original for a graph wide enough for the difference to show up in a timing.

---

## Licence

deepset-ai/haystack is under the Apache License 2.0, © 2021 deepset GmbH. This port
reimplements the behaviour without copied source; see `ACKNOWLEDGEMENTS.md`.
