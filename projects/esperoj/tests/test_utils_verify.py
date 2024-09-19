def test_verify_successed(esperoj):
    files = esperoj.databases["primary"].query("files")
    assert all(esperoj.utils.verify(esperoj, files)) is True


def test_verify_failed(esperoj, tmp_file):
    files = esperoj.databases["primary"].query("files")
    esperoj.storages["s3_storage"].upload(str(tmp_file), "music.flac")
    assert all(esperoj.utils.verify(esperoj, files)) is False


def test_verify_failed_when_error(esperoj):
    files = esperoj.databases["primary"].query("files")
    esperoj.storages["s3_storage"].delete(["music.flac"])
    assert all(esperoj.utils.verify(esperoj, files)) is False


def test_verify_unverified_file(esperoj, tmp_file):
    db = esperoj.databases["primary"]
    files = db.query("files")
    db.batch_update("files", [{"id": files[0].id, "verified": False}])
    assert all(esperoj.utils.verify(esperoj, files)) is True
