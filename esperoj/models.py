from django.db import models
from simple_history.models import HistoricalRecords
import uuid

# -----------------------
# Registry / relationship
# -----------------------
class Item(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    title = models.CharField(max_length=255)
    authors = models.JSONField(default=list)  # stores list of artist names or objects
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    def __str__(self):
        return f"{self.title} - {self.authors[0]}"


# ----------
# Files
# ----------
class File(models.Model):
    name = models.CharField(max_length=512)  # uploaded filename
    path = models.CharField(max_length=1024, blank=True)  # canonical ingest path (optional)
    size = models.BigIntegerField(null=True, blank=True)
    mime_type = models.CharField(max_length=255, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    item = models.ForeignKey(Item, on_delete=models.CASCADE)

    sha1 = models.CharField(max_length=256, blank=True)
    sha256 = models.CharField(max_length=256, blank=True)

    history = HistoricalRecords()

    def __str__(self):
        return self.name

# -------------------------
# Storage locations & copies
# -------------------------
class StorageLocation(models.Model):
    """
    Represents a storage backend or mount. Keep backend hint + flexible JSON config.
    """
    name = models.CharField(max_length=255)  # "AWS S3 - bucket", "Archive-1"
    backend = models.CharField(max_length=50, blank=True)  # e.g. "s3", "local", "ftp"
    base_path = models.CharField(max_length=1024, blank=True)  # root / bucket
    encryption = models.BooleanField(default=False)
    config = models.JSONField(default=dict, blank=True)  # creds, region, endpoint, etc.
    active = models.BooleanField(default=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.name


class FileStorage(models.Model):
    """
    One row per (file, storage location) copy.
    Stores per-location checksum, stored_path, key id, extra config.
    """
    file = models.ForeignKey(File, on_delete=models.CASCADE)
    storage_location = models.ForeignKey(StorageLocation, on_delete=models.CASCADE)
    stored_path = models.CharField(max_length=1024)   # path inside that storage location
    is_primary = models.BooleanField(default=False)  # optional: which copy is canonical
    extra = models.JSONField(default=dict, blank=True)  # any per-copy metadata
    created_at = models.DateTimeField(auto_now_add=True)
    sha1 = models.CharField(max_length=256, blank=True)
    sha256 = models.CharField(max_length=256, blank=True)

    history = HistoricalRecords()

    class Meta:
        unique_together = ("file", "storage_location")
        indexes = [
            models.Index(fields=["file", "storage_location"]),
            models.Index(fields=["storage_location"]),
            models.Index(fields=["is_primary"]),
        ]

class Song(Item):
    history = HistoricalRecords()