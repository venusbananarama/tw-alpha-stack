from alpha_core.phase4.ledger import release_lock


def test_release_lock_none_noop() -> None:
    release_lock(None)
