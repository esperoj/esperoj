"""Tests for Esperoj class."""

import logging

import pytest

from esperoj.database.memory import MemoryRecord
from esperoj.esperoj import Esperoj
from esperoj.exceptions import RecordNotFoundError


@pytest.mark.parametrize(
    ("db_env_value", "expected_db_class", "logger"),
    [
        ("Airtable", "Airtable", None),
        ("", "MemoryDatabase", logging.getLogger("TestLogger")),
    ],
)
def test_esperoj_default_initialization(mocker, db_env_value, expected_db_class, logger):
    """Test default initialization of Esperoj with different database environment values and logger."""
    mocker.patch.dict(
        "os.environ",
        {
            "ESPEROJ_DATABASE": db_env_value,
            "STORJ_ACCESS_KEY_ID": "test_access_key",
            "STORJ_SECRET_ACCESS_KEY": "test_secret_key",
            "STORJ_ENDPOINT_URL": "test_endpoint_url",
        },
    )

    mock_airtable = mocker.MagicMock(name="Airtable")
    mock_memory_database = mocker.MagicMock(name="MemoryDatabase")
    mock_s3storage = mocker.MagicMock(name="S3Storage")
    mocker.patch("esperoj.database.airtable.Airtable", return_value=mock_airtable)
    mocker.patch("esperoj.database.memory.MemoryDatabase", return_value=mock_memory_database)
    mocker.patch("esperoj.storage.s3.S3Storage", return_value=mock_s3storage)

    esperoj = Esperoj(db=None, storage=None, logger=logger)
    db_classes = {"Airtable": mock_airtable, "MemoryDatabase": mock_memory_database}
    assert esperoj.db == db_classes[expected_db_class]
    assert esperoj.storage == mock_s3storage
    assert isinstance(esperoj.logger, logging.Logger)


def test_archive_success(mocker, esperoj, memory_table, s3_storage):
    """Test successful archiving of a record."""
    record = memory_table.create({"Name": "test_file.txt"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    archive_url = esperoj.archive(record.record_id)
    assert archive_url == "http://archive.org/test_file.txt"
    assert record.fields["Internet Archive"] == archive_url


def test_archive_record_not_found(esperoj, memory_table):
    """Test archiving with a non-existent record ID."""
    with pytest.raises(RecordNotFoundError):
        esperoj.archive("nonexistent_record_id")


def test_archive_url_failure(mocker, esperoj, memory_table, s3_storage):
    """Test archiving failure due to a RuntimeError."""
    record = memory_table.create({"Name": "test_file.txt"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", side_effect=RuntimeError("Archiving failed"))
    with pytest.raises(RuntimeError) as excinfo:
        esperoj.archive(record.record_id)
    assert "Archiving failed" in str(excinfo.value)


def test_ingest_file_not_found(esperoj, tmp_path):
    """Test ingesting a non-existent file."""
    non_existent_file = tmp_path / "nonexistent.txt"
    with pytest.raises(FileNotFoundError):
        esperoj.ingest(non_existent_file)


def test_ingest_file_exists_in_database(esperoj, memory_table, tmp_file):
    """Test ingesting a file that already exists in the database."""
    memory_table.create({"Name": tmp_file.name, "Size": 123, "SHA256": "dummyhash"})
    with pytest.raises(FileExistsError):
        esperoj.ingest(tmp_file)


def test_ingest_file_exists_in_storage(esperoj, s3_storage, tmp_file):
    """Test ingesting a file that already exists in the storage."""
    s3_storage.upload_file(str(tmp_file), tmp_file.name)
    with pytest.raises(FileExistsError):
        esperoj.ingest(tmp_file)


def test_ingest_success(esperoj, memory_table, s3_storage, tmp_file):
    """Test successful ingestion of a file."""
    record = esperoj.ingest(tmp_file)
    assert isinstance(record, MemoryRecord)
    assert record.fields["Name"] == tmp_file.name
    assert record.fields["Size"] == tmp_file.stat().st_size
    assert "SHA256" in record.fields
    assert s3_storage.file_exists(tmp_file.name)


@pytest.mark.parametrize(
    ("file_ext", "title_key", "artist_key"),
    [
        (".flac", "Vorbis:Title", "Vorbis:Artist"),
        (".mp3", "ID3:Title", "ID3:Artist"),
        (".m4a", "QuickTime:Title", "QuickTime:Artist"),
    ],
)
def test_ingest_audio_file_success(
    mocker, esperoj, memory_table, s3_storage, tmp_path, file_ext, title_key, artist_key
):
    """Test successful ingestion of audio files (.flac, .mp3, .m4a)."""
    temp_file_path = tmp_path / f"test{file_ext}"
    temp_file_path.touch()
    mock_exiftool_helper = mocker.patch("esperoj.esperoj.ExifToolHelper")
    mock_exiftool_helper_instance = mock_exiftool_helper.return_value.__enter__.return_value
    mock_exiftool_helper_instance.get_metadata.return_value = [
        {
            "File:FileName": f"test{file_ext}",
            "File:FileSize": 0,
            "File:MIMEType": f"audio/{file_ext[1:]}",
            title_key: "Test Title",
            artist_key: "Test Artist",
        }
    ]
    record = esperoj.ingest(temp_file_path)
    assert isinstance(record, MemoryRecord)
    assert record.fields["Name"] == f"test{file_ext}"
    assert record.fields["Size"] == 0
    assert "SHA256" in record.fields
    music_record = next(esperoj.db.table("Musics").get_all({"Files": [record.record_id]}))
    assert music_record.fields["Name"] == "Test Title"
    assert music_record.fields["Artist"] == "Test Artist"
    assert s3_storage.file_exists(f"test{file_ext}")


def test_verify_integrity_success(mocker, esperoj, memory_table, s3_storage):
    """Test successful integrity verification of a record."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    mocker.patch("esperoj.esperoj.calculate_hash_from_url", return_value="validhash")
    assert esperoj.verify(record.record_id) is True


def test_verify_integrity_failure(mocker, esperoj, memory_table, s3_storage):
    """Test integrity verification failure due to an invalid hash."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    mocker.patch(
        "esperoj.esperoj.calculate_hash_from_url", side_effect=["invalidhash", "validhash"]
    )
    assert esperoj.verify(record.record_id) is False


def test_verify_record_not_found(esperoj, memory_table):
    """Test integrity verification with a non-existent record ID."""
    with pytest.raises(RecordNotFoundError):
        esperoj.verify("nonexistent_record_id")


def test_verify_storage_error(mocker, esperoj, memory_table, s3_storage):
    """Test integrity verification failure due to a storage error."""
    record = memory_table.create({"Name": "test_file.txt", "SHA256": "validhash"})
    mocker.patch.object(s3_storage, "get_link", side_effect=FileNotFoundError)
    mocker.patch("esperoj.esperoj.archive", return_value="http://archive.org/test_file.txt")
    with pytest.raises(FileNotFoundError):
        esperoj.verify(record.record_id)


def test_verify_with_internet_archive_field(mocker, esperoj, memory_table, s3_storage):
    """Test integrity verification when the 'Internet Archive' field is set."""
    record = memory_table.create(
        {
            "Name": "test_file.txt",
            "SHA256": "validhash",
            "Internet Archive": "http://archive.org/test_file.txt",
        }
    )
    mocker.patch.object(s3_storage, "get_link", return_value="http://example.com/test_file.txt")
    mocker.patch("esperoj.esperoj.calculate_hash_from_url", return_value="validhash")
    assert esperoj.verify(record.record_id)
