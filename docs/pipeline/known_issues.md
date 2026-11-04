# Known issues

Problems we know about and haven't fixed yet, with links to their issues.

:::{admonition} Stub
:class: note

To do:

- Partitions grow without splitting, so the catalog needs a periodic
  re-import
- Updating the sky maps sometimes takes far longer than usual
  ([#22](https://github.com/lincc-frameworks/alert-archive/issues/22))
- The converter misses alerts archived after it has converted a night
- Reconverted chunks get new names, so they could be ingested twice
- Jobs on `rubin:default` can be preempted
:::
