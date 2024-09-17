def test_verify(esperoj):
    files = esperoj.databases["primary"].query("files")
    assert esperoj.utils.verify(esperoj, files) == [True]
