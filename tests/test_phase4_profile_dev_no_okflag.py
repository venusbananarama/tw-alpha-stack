from alpha_core.phase4.profile import should_write_ok


def test_profile_dev_never_writes_ok() -> None:
    assert should_write_ok("dev", "PASS") is False
    assert should_write_ok("dev", "WARN") is False
    assert should_write_ok("prod", "PASS") is True
