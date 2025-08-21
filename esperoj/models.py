import datetime
from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import (
    MinValueValidator,
    MaxValueValidator,
    URLValidator,
)
from django.db import models
from django.db.models import Q, Index, UniqueConstraint, CheckConstraint
from django.contrib.postgres.indexes import GinIndex
from simple_history.models import HistoricalRecords

# --- Abstract Base Models ---

class BaseModel(models.Model):
    """An abstract base model providing self-updating created_at and updated_at fields."""
    created_at = models.DateTimeField(auto_now_add=True, editable=False)
    updated_at = models.DateTimeField(auto_now=True, editable=False)
    history = HistoricalRecords(inherit=True)

    class Meta:
        abstract = True

# --- Localized and Normalized Data Models ---

class LocalizedTitle(BaseModel):
    """A normalized, language-specific title for an Item."""
    item = models.ForeignKey(
        "Item", on_delete=models.CASCADE, related_name="titles"
    )
    language = models.CharField(
        max_length=10,
        default=settings.LANGUAGE_CODE,
        null=False,
        blank=False,
        help_text="Language code (e.g., 'en', 'es', 'fr')",
    )
    title = models.CharField(max_length=255, null=False, blank=False)

    class Meta:
        ordering = ["language", "title"]
        constraints = [
            UniqueConstraint(fields=["item", "language"], name="unique_title_per_language_for_item")
        ]
        indexes = [Index(fields=["item", "language"])]

    def __str__(self):
        return f'"{self.title}" ({self.language})'


class LocalizedName(BaseModel):
    """A normalized, language-specific name for various models."""
    # This model uses a generic relation via ContentType framework to be reusable.
    from django.contrib.contenttypes.fields import GenericForeignKey
    from django.contrib.contenttypes.models import ContentType

    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey("content_type", "object_id")

    language = models.CharField(
        max_length=10,
        default=settings.LANGUAGE_CODE,
        null=False,
        blank=False,
        help_text="Language code (e.g., 'en', 'es', 'fr')",
    )
    name = models.CharField(max_length=512, null=False, blank=False)

    class Meta:
        ordering = ["language", "name"]
        constraints = [
            UniqueConstraint(fields=["content_type", "object_id", "language"], name="unique_name_per_language_for_object")
        ]
        indexes = [
            Index(fields=["content_type", "object_id", "language"], name="localized_name_gfk_lang_idx"),
        ]

    def __str__(self):
        return f'"{self.name}" ({self.language})'


class WebLink(BaseModel):
    """A normalized URL associated with an Item."""
    item = models.ForeignKey(
        "Item", on_delete=models.CASCADE, related_name="weblinks"
    )
    url = models.URLField(max_length=2048, null=False, blank=False, validators=[URLValidator()])
    description = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        ordering = ["-created_at"]
        constraints = [
            UniqueConstraint(fields=["item", "url"], name="unique_url_per_item")
        ]

    def __str__(self):
        return self.url

# --- Core Entity Models ---

class Creator(BaseModel):
    identifier = models.SlugField(max_length=255, unique=True, null=False, blank=False)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        indexes = [Index(fields=["identifier"])]

    def __str__(self):
        # Attempt to return the English name or the first available name
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        if primary_name:
            return primary_name.name
        return self.names.first().name if self.names.exists() else self.identifier


class Subject(BaseModel):
    identifier = models.SlugField(max_length=255, unique=True, null=False, blank=False)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        indexes = [Index(fields=["identifier"])]

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        if primary_name:
            return primary_name.name
        return self.names.first().name if self.names.exists() else self.identifier


class Collection(BaseModel):
    identifier = models.SlugField(max_length=255, unique=True, null=False, blank=False)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        indexes = [Index(fields=["identifier"])]

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        if primary_name:
            return primary_name.name
        return self.names.first().name if self.names.exists() else self.identifier


class File(BaseModel):
    path = models.CharField(max_length=1024, null=False, blank=False)
    size = models.PositiveBigIntegerField(validators=[MinValueValidator(0)], null=False, blank=False)
    mime_type = models.CharField(max_length=255, blank=True, null=True, default=None)
    sha1 = models.CharField(max_length=40, blank=True, null=True, default=None)
    sha256 = models.CharField(max_length=64, blank=True, null=True, default=None)

    class Meta:
        ordering = ["path"]
        indexes = [
            Index(fields=["path"]),
            Index(fields=["sha1"]),
            Index(fields=["sha256"]),
        ]
        constraints = [
            UniqueConstraint(fields=["sha256"], condition=Q(sha256__isnull=False), name="unique_sha256"),
            UniqueConstraint(fields=["sha1"], condition=Q(sha1__isnull=False), name="unique_sha1"),
        ]

    def get_primary_storage(self):
        return self.storages.filter(is_primary=True).first()

    def get_latest_storage(self):
        return self.storages.order_by("-updated_at").first()

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        if primary_name:
            return primary_name.name
        return self.names.first().name if self.names.exists() else self.path


class ItemQuerySet(models.QuerySet):
    def latest(self):
        return self.order_by("-date", "-updated_at")


class Item(BaseModel):
    identifier = models.SlugField(max_length=255, unique=True, null=False, blank=False)
    collections = models.ManyToManyField(Collection, related_name="items", blank=True)
    creators = models.ManyToManyField(Creator, related_name="items", blank=True)
    subjects = models.ManyToManyField(Subject, related_name="items", blank=True)
    languages = models.JSONField(
        default=list, blank=True, help_text="A cached list of language codes from associated titles."
    )
    year = models.PositiveSmallIntegerField(null=True, blank=True, default=None)
    month = models.PositiveSmallIntegerField(
        null=True, blank=True, default=None, validators=[MinValueValidator(1), MaxValueValidator(12)]
    )
    day = models.PositiveSmallIntegerField(
        null=True, blank=True, default=None, validators=[MinValueValidator(1), MaxValueValidator(31)]
    )
    files = models.ManyToManyField(File, related_name="items", blank=True)
    date = models.DateField(null=True, blank=True, editable=False)

    objects = ItemQuerySet.as_manager()

    class Meta:
        ordering = ["-date"]
        indexes = [
            Index(fields=["-date"]),
            Index(fields=["identifier"]),
            Index(fields=["updated_at"]),
            GinIndex(fields=["languages"]),
        ]
        constraints = [
            CheckConstraint(check=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),
            CheckConstraint(check=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),
        ]

    def clean(self):
        # Date validation logic
        if self.month and not self.year:
            raise ValidationError("Month cannot be set without a year.")
        if self.day and not self.month:
            raise ValidationError("Day cannot be set without a month.")
        if self.day and self.year and self.month:
            try:
                datetime.date(self.year, self.month, self.day)
            except ValueError:
                raise ValidationError(f"Invalid day '{self.day}' for the given month and year.")
        # Check if item has at least one title on creation
        if not self.pk and not self.titles.exists():
             # This check is tricky in clean() as relations are not saved yet.
             # Best enforced at the form/serializer level.
             pass

    def _update_date_field(self):
        """Constructs the 'date' field from year, month, and day."""
        if self.year:
            month = self.month if self.month else 1
            day = self.day if self.day else 1
            self.date = datetime.date(self.year, month, day)
        else:
            self.date = None

    def save(self, *args, **kwargs):
        self._update_date_field()
        # The 'languages' field could be automatically populated from related titles
        # if desired, e.g., in a post-save signal.
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        primary_title = self.titles.filter(language=settings.LANGUAGE_CODE).first()
        if primary_title:
            return primary_title.title
        return self.titles.first().title if self.titles.exists() else self.identifier


class FileStorage(BaseModel):
    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="storages")
    storage_name = models.CharField(max_length=100, null=False, blank=False)
    path = models.CharField(max_length=1024, null=False, blank=False)
    is_primary = models.BooleanField(default=False, null=False, blank=False)
    sha1 = models.CharField(max_length=40, blank=True, null=True, default=None)
    sha256 = models.CharField(max_length=64, blank=True, null=True, default=None)
    extra = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-is_primary", "-updated_at"]
        indexes = [
            Index(fields=["file", "storage_name"]),
            Index(fields=["storage_name"]),
            Index(fields=["updated_at"]),
        ]
        constraints = [
            UniqueConstraint(fields=["file", "path"], name="unique_file_path_in_storage"),
            UniqueConstraint(fields=["file"], condition=Q(is_primary=True), name="unique_primary_storage_per_file"),
        ]

    def __str__(self):
        return f"{self.file} @ {self.storage_name}"

# --- Proxy Models ---

class Song(Item):
    class Meta:
        proxy = True
        verbose_name = "Song"
        verbose_name_plural = "Songs"


class Book(Item):
    class Meta:
        proxy = True
        verbose_name = "Book"
        verbose_name_plural = "Books"