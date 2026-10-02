import alert_archive


def test_version():
    """Check to see that we can get the package version"""
    assert alert_archive.__version__ is not None
