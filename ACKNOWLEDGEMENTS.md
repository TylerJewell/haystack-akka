# Acknowledgements

This project is a port of **[deepset-ai/haystack](https://github.com/deepset-ai/haystack)**.

## Licence and copyright

- deepset-ai/haystack is licensed under the **Apache License 2.0**. Copyright 2021 deepset
  GmbH (`LICENSE-haystack:190`; individual source files carry `SPDX-FileCopyrightText:
  2022-present deepset GmbH <info@deepset.ai>`).
- **Nothing was copied verbatim.** Every Java file under `haystack-akka/src` was written
  fresh against behaviour read out of, and run against, the installed `haystack-ai` Python
  package (version 3.0.0); no source text, comments, or test fixtures were transcribed.
  Where a comment or the spec cites a source file and line range, that is citation, not
  copying.
- **Behaviour is derived throughout**, plainly: the priority-based scheduler, the
  branching/fan-out and loop/fan-in rules (normal, lazy variadic and greedy variadic
  sockets), the strongly-connected-component tie-break order, and the per-component visit
  cap are a direct port of the decision procedure in `core/pipeline/base.py`,
  `core/pipeline/pipeline.py` and `core/pipeline/component_checks.py`. This is the nature
  of a port and is not something to obscure.
- Because no Apache-2.0 text was copied into this repository, nothing here is bound by
  deepset-ai/haystack's licence terms — the "copied material carries its licence with it"
  rule does not trigger, since nothing was copied. `LICENSE-haystack` carries the original
  licence text for reference and attribution only.

## Also used

- [Akka](https://akka.io) — the SDK and runtime this port is built on.
