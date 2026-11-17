import pytest
from alert_archive.collection import (
    adopt_working,
    collection_versions,
    collections_dir,
    current_catalog_dir,
    remove_versions,
    set_current_version,
)
from alert_archive.config import PathConfig
from alert_archive.state import LockHeldError, PipelineState
from hats.catalog.dataset.collection_properties import CollectionProperties


def write_version(collection_dir, version):
    """Write a version's own collection.properties, as hats-import does."""
    name = f"rubin_alert_archive_{version}"
    properties = CollectionProperties(
        name=version,
        hats_primary_table_url=name,
        all_margins=[f"{name}_10arcs"],
        default_margin=f"{name}_10arcs",
        all_indexes={"diaSourceId": f"{name}_diaSourceId"},
    )
    (collection_dir / version).mkdir(parents=True)
    properties.to_properties_file(collection_dir / version)


def test_collection_versions(tmp_path):
    """Only the directories named by a date are versions."""
    for name in ["20261101", "20261102", "notes", "old_20261101", "20261107T090000Z"]:
        (tmp_path / name).mkdir()
    (tmp_path / "20261103").touch()  # a file, not a directory

    assert collection_versions(tmp_path) == {"20261101", "20261102"}


def test_set_current_version(tmp_path):
    """The top-level collection.properties identifies the current version."""
    collection = tmp_path / "collections"
    write_version(collection, "20261102")
    write_version(collection, "20261109")

    set_current_version(collection, "20261109")

    properties = CollectionProperties.read_from_dir(collection)
    name = "20261109/rubin_alert_archive_20261109"
    assert properties.name == "rubin_alert_archive"
    assert properties.hats_primary_table_url == name
    assert properties.all_margins == [f"{name}_10arcs"]
    assert properties.default_margin == f"{name}_10arcs"
    assert properties.all_indexes == {"diaSourceId": f"{name}_diaSourceId"}


def test_current_catalog_dir(tmp_path):
    """The current catalog is the main catalog of the version collection.properties points at."""
    paths = PathConfig(root=tmp_path)
    write_version(collections_dir(paths), "20261107")
    with pytest.raises(FileNotFoundError, match="no current version"):
        current_catalog_dir(paths)

    set_current_version(collections_dir(paths), "20261107")

    expected = tmp_path / "collections" / "20261107" / "rubin_alert_archive_20261107"
    assert current_catalog_dir(paths) == expected


@pytest.fixture
def paths(tmp_path):
    """Paths for a root with a state dir and a stand-in working/ catalog."""
    paths = PathConfig(root=tmp_path)
    paths.state_dir.mkdir()
    (paths.working_dir / "dataset").mkdir(parents=True)
    (paths.working_dir / "hats.properties").write_text("catalog_name=rubin_alert_archive\n")
    return paths


def test_adopt_working(paths):
    """working/ is moved, as is, into the collection's first version, which becomes current."""
    catalog_dir = adopt_working(paths, "20261101")

    assert catalog_dir == collections_dir(paths) / "20261101" / "rubin_alert_archive_20261101"
    assert not paths.working_dir.exists()
    assert (catalog_dir / "hats.properties").read_text() == "catalog_name=rubin_alert_archive\n"
    assert current_catalog_dir(paths) == catalog_dir


def test_adopt_working_only_once(paths):
    """A collection that already has a current version is never replaced."""
    adopt_working(paths, "20261101")
    (paths.working_dir / "dataset").mkdir(parents=True)

    with pytest.raises(FileExistsError, match="already has a current version"):
        adopt_working(paths, "20261102")

    assert paths.working_dir.exists()
    assert collection_versions(collections_dir(paths)) == {"20261101"}


def test_adopt_working_waits_for_the_lock(paths):
    """Nothing is moved while another job holds the lock."""
    with PipelineState(paths.state_dir).lock("daily"):
        with pytest.raises(LockHeldError):
            adopt_working(paths, "20261101")

    assert paths.working_dir.exists()
    assert not collections_dir(paths).exists()


def test_remove_versions(tmp_path):
    """The given versions are deleted with their contents."""
    for version in ["20261101", "20261102"]:
        (tmp_path / version / "catalog").mkdir(parents=True)
    (tmp_path / "collection.properties").touch()

    # 20261105 doesn't exist, which is fine
    remove_versions(tmp_path, {"20261101", "20261105"})

    assert not (tmp_path / "20261101").exists()
    assert (tmp_path / "20261102").exists()
    assert (tmp_path / "collection.properties").exists()
