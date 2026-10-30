"""Base pipeline orchestration for the incremental alert archive.

This module provides an abstract ``Pipeline`` base class that handles
common orchestration tasks such as creating a Dask client, managing a
temporary directory, and invoking the pipeline-specific ``run`` method.

Adapted from ``ppdb_hats/pipeline.py`` in ppdb_hats
(https://github.com/astronomy-commons/ppdb_hats, commit 241b4f2).
"""

import logging
import tempfile
from abc import ABC, abstractmethod
from pathlib import Path

from dask.distributed import Client

from .config import PipelineConfig, get_default_config

logger = logging.getLogger(__name__)


class Pipeline(ABC):
    """Abstract base class for the alert archive pipelines.

    The :class:`Pipeline` class centralizes orchestration concerns so
    concrete pipelines implement only their domain logic in ``run``.

    Attributes
    ----------
    config : PipelineConfig
        Top-level pipeline configuration instance used by the pipeline.
    """

    def __init__(self, config: PipelineConfig | None = None):
        """Initialize pipeline with configuration.

        Parameters
        ----------
        config : PipelineConfig, optional
            Pipeline configuration. If ``None``, :func:`get_default_config`
            is used to provide sane defaults.
        """
        self.config = config or get_default_config()

    def execute(self, *args, **kwargs):
        """Execute the pipeline with a Dask client and temporary directory.

        The method creates a temporary directory (under
        ``config.paths.tmp_dir``) and a Dask client and then delegates to
        :meth:`run`.

        Parameters
        ----------
        *args, **kwargs
            Passed through to :meth:`run` implemented by subclasses.

        Returns
        -------
        object
            Whatever :meth:`run` returns.
        """
        self._configure_logging()
        logger.info("Starting pipeline...")
        with tempfile.TemporaryDirectory(dir=self.config.paths.tmp_dir) as tmp_dir:
            return self._run_with_client(Path(tmp_dir), *args, **kwargs)

    def _configure_logging(self):
        """Configure logging for the pipeline.

        This method sets up logging with a specific format and level. It
        is called at the beginning of the ``execute`` method to ensure
        that all log messages from the pipeline are properly formatted.
        """
        package_logger = logging.getLogger(__package__)
        if not package_logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s | %(module)s:%(funcName)s | %(message)s",
                datefmt="%H:%M:%S",
            )
            handler.setFormatter(formatter)
            package_logger.addHandler(handler)
        package_logger.setLevel(logging.INFO)

    def _run_with_client(self, tmp_dir: Path, *args, **kwargs):
        """Create a Dask client and execute the pipeline.

        Parameters
        ----------
        tmp_dir : pathlib.Path
            Temporary directory used as the Dask local directory.
        *args, **kwargs
            Passed through to :meth:`run`.
        """
        dask_cfg = self.config.dask
        with Client(
            n_workers=dask_cfg.n_workers,
            memory_limit=dask_cfg.memory_limit,
            threads_per_worker=dask_cfg.threads_per_worker,
            local_directory=str(tmp_dir),
        ) as client:
            logger.info("Tmp dir: %s", tmp_dir)
            logger.info("Dask client: %s", client)
            return self.run(client, tmp_dir, *args, **kwargs)

    @abstractmethod
    def run(self, client: Client, tmp_dir: Path, *args, **kwargs):
        """Execute pipeline-specific logic.

        Subclasses must implement this method.

        Parameters
        ----------
        client : dask.distributed.Client
            The Dask client to use for distributed operations.
        tmp_dir : pathlib.Path
            Temporary directory for intermediate files.
        *args, **kwargs
            Extra parameters specific to concrete pipelines.
        """
        raise NotImplementedError("Subclasses must implement the run method.")
