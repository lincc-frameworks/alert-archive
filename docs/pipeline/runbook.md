# Runbook

What to do when the pipeline needs a hand.

:::{admonition} Stub
:class: note

To do:

- Checking whether new alerts have arrived (raw archive → `landing/` →
  catalog)
- A job was preempted or killed: checking nothing is running, removing the
  stale lock, resubmitting
- The format-change flag is set: what changed, updating the schema, clearing
  the flag
- A run failed: reading the log, cleaning up, rerunning
- Running from a clone: committed code only, and the editable install check
:::
