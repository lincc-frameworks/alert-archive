import logging

from alert_archive.config import DaskConfig, PathConfig, get_default_config
from alert_archive.pipeline import Pipeline
from dask.distributed import Client


class RecordingPipeline(Pipeline):
    """Records what ``run`` was called with."""

    def run(self, client, tmp_dir, *args, **kwargs):
        """Return the arguments, plus whether the temp dir existed during the run."""
        return client, tmp_dir, tmp_dir.is_dir(), args, kwargs


def test_execute_runs_with_client_and_tmp_dir():
    """``execute`` passes a live client, an existing temp dir and its own arguments to ``run``."""
    config = get_default_config(dask=DaskConfig(n_workers=1, memory_limit="1GiB"))
    result = RecordingPipeline(config).execute(42, key="value")
    client, tmp_dir, tmp_dir_existed_during_run, args, kwargs = result

    assert isinstance(client, Client)
    assert tmp_dir_existed_during_run
    assert not tmp_dir.exists()
    assert args == (42,)
    assert kwargs == {"key": "value"}
    assert logging.getLogger("alert_archive").level == logging.INFO


def test_execute_makes_tmp_dir_under_configured_dir(tmp_path):
    """The run's temp dir is made inside ``paths.tmp_dir``."""
    config = get_default_config(
        dask=DaskConfig(n_workers=1, memory_limit="1GiB"), paths=PathConfig(tmp_dir=tmp_path)
    )
    _, tmp_dir, *_ = RecordingPipeline(config).execute()
    assert tmp_dir.parent == tmp_path


def test_default_config():
    """A pipeline without a config gets the defaults."""
    assert RecordingPipeline().config == get_default_config()
