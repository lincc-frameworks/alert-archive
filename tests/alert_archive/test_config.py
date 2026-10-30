from pathlib import Path

from alert_archive.config import DaskConfig, PathConfig, get_default_config


def test_paths_under_root(tmp_path):
    """Landing, state and working directories all live under the root."""
    config = PathConfig(root=str(tmp_path))
    assert config.root == tmp_path
    assert config.landing_dir == tmp_path / "landing"
    assert config.state_dir == tmp_path / "state"
    assert config.working_dir == tmp_path / "working"


def test_default_root():
    """The default root is the shared v2 directory on S3DF."""
    assert PathConfig().root == Path("/sdf/data/rubin/shared/lsdb_commissioning/rubin_alert_archive_v2")


def test_tmp_dir_defaults_to_lscratch(tmp_path, monkeypatch):
    """The temp dir defaults to $LSCRATCH when it's set, else to the system default."""
    monkeypatch.setenv("LSCRATCH", str(tmp_path))
    assert PathConfig().tmp_dir == tmp_path
    monkeypatch.delenv("LSCRATCH")
    assert PathConfig().tmp_dir is None


def test_tmp_dir_override(tmp_path, monkeypatch):
    """An explicit temp dir wins over $LSCRATCH, and strings become paths."""
    monkeypatch.setenv("LSCRATCH", "/nonexistent")
    assert PathConfig(tmp_dir=str(tmp_path)).tmp_dir == tmp_path


def test_get_default_config_overrides(tmp_path):
    """Overrides replace one section and keep the defaults for the rest."""
    config = get_default_config(paths=PathConfig(root=tmp_path))
    assert config.paths.root == tmp_path
    assert config.dask == DaskConfig()
    assert config.import_config.byte_pixel_threshold == 1 << 30
