# Developer guide

Setting up to work on this repository.

:::{admonition} Stub
:class: note

To do:

- Which environment to use on S3DF (the LSST env) and installing the package
  editable: `pip install -e .'[dev]'`
- Running the tests, and `pre-commit install`
- Building these docs locally (`make -C docs html`)
- Branches and PRs
:::

## Notebooks in the docs

Most pages with code are Jupyter notebooks, rendered as they are. ReadTheDocs
can't reach the catalog, so the notebooks are run on USDF and committed **with
their outputs**. `nbsphinx_execute = "never"` in `docs/conf.py` keeps the build
from running them.

:::{important}
If you're used to other LINCC Frameworks projects: there's no
`docs/pre_executed/` folder here. Every notebook under `docs/` is treated as
pre-executed, so put a notebook next to the pages it belongs with (for example
`docs/catalog/`). The pre-commit hook that clears notebook outputs skips all of
`docs/`, and still clears notebooks anywhere else in the repo.
:::

To add one:

1. Put the notebook in the folder for its section, and add it to that section's
   `toctree` in `docs/index.md` without the `.ipynb` extension.
2. Run it top to bottom on USDF, then commit it with its outputs.
3. Keep it under 500 KB, the `check-added-large-files` limit. Save figures at a
   modest size if a notebook has many of them.
