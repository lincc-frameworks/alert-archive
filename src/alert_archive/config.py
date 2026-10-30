"""Configuration for the incremental pipelines.

Dataclasses for the Dask client, the filesystem layout of the v2 archive and
the hats-import settings, with defaults for running on S3DF.

Adapted from ``ppdb_hats/config.py`` in ppdb_hats
(https://github.com/astronomy-commons/ppdb_hats, commit 241b4f2).
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class DaskConfig:
    """Configuration for the Dask distributed client.

    Attributes
    ----------
    n_workers : int
        Number of worker processes.
    memory_limit : str
        Memory limit per worker (e.g. ``"16GiB"``).
    threads_per_worker : int
        Threads per worker process.
    """

    n_workers: int = 8
    memory_limit: str = "16GiB"
    threads_per_worker: int = 1


@dataclass
class PathConfig:
    """Filesystem layout of the v2 alert archive.

    Everything lives under one root::

        rubin_alert_archive_v2/
        ├── landing/   parquet chunks waiting to be ingested
        ├── state/     ingested log, format-change flag, lock
        └── working/   HATS catalog the daily runs write to

    Attributes
    ----------
    root : pathlib.Path
        Root directory of the v2 archive.
    tmp_dir : pathlib.Path or None
        Where each run makes its temporary directory, which is also Dask's
        local directory for worker spill. Defaults to ``$LSCRATCH`` (node-local
        scratch on S3DF batch nodes) when it's set, else the system default
        (usually ``/tmp``, which can be small).
    dump_dir : pathlib.Path
        Neven Caplar's parquet dump of the alert archive, outside the root.
        The baseline is built from its first chunks, and the rest are
        replayed through the daily pipeline.
    """

    root: Path = Path("/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2")
    tmp_dir: Path | None = field(default_factory=lambda: os.environ.get("LSCRATCH") or None)
    dump_dir: Path = Path("/sdf/data/rubin/user/ncaplar/alert-parquet")

    @property
    def landing_dir(self) -> Path:
        """Directory the daily run reads new parquet chunks from."""
        return self.root / "landing"

    @property
    def state_dir(self) -> Path:
        """Directory holding the pipeline state, outside the catalog."""
        return self.root / "state"

    @property
    def working_dir(self) -> Path:
        """The working HATS catalog, which grows nightly."""
        return self.root / "working"

    def __post_init__(self):
        """Ensure paths are Path objects"""
        if isinstance(self.root, str):
            self.root = Path(self.root)
        if isinstance(self.tmp_dir, str):
            self.tmp_dir = Path(self.tmp_dir)
        if isinstance(self.dump_dir, str):
            self.dump_dir = Path(self.dump_dir)


@dataclass
class ImportConfig:
    """Configuration for hats-import runs.

    Attributes
    ----------
    ra_column : str
        Column name for right ascension.
    dec_column : str
        Column name for declination.
    byte_pixel_threshold : int
        Threshold (bytes) for splitting pixels.
    resume : bool
        Whether to resume an interrupted import.
    simple_progress_bar : bool
        Use a simplified progress display for non-interactive runs.
    """

    ra_column: str = "ra"
    dec_column: str = "dec"
    byte_pixel_threshold: int = 1 << 30  # 1 GiB
    resume: bool = False
    simple_progress_bar: bool = True


@dataclass
class PipelineConfig:
    """Top-level pipeline configuration.

    Attributes
    ----------
    dask : DaskConfig
        Dask configuration.
    paths : PathConfig
        Filesystem path configuration.
    import_config : ImportConfig
        Import-specific configuration.
    """

    dask: DaskConfig
    paths: PathConfig
    import_config: ImportConfig


def get_default_config(**kwargs) -> PipelineConfig:
    """Return a :class:`PipelineConfig` filled with sensible defaults.

    Parameters
    ----------
    **kwargs
        Overrides for :class:`PipelineConfig` fields, e.g.
        ``paths=PathConfig(root=tmp_dir)``.

    Returns
    -------
    PipelineConfig
        Default configuration instance with Dask, path and import settings.
    """
    defaults = {"dask": DaskConfig(), "paths": PathConfig(), "import_config": ImportConfig()}
    return PipelineConfig(**(defaults | kwargs))
