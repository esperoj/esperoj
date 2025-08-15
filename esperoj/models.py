from django.db import models
from django.db.models import Q, Index, UniqueConstraint, CheckConstraint
from django.core.validators import MinValueValidator, MaxValueValidator
from simple_history.models import HistoricalRecords
import datetime

class Creator(models.Model):
    name = models.CharField(max_length=100, db_index=True)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    class Meta:
        ordering = ["name"]
        indexes = [Index(fields=["name"])]

    def __str__(self):
        return self.name

class Subject(models.Model):
    name = models.CharField(max_length=100, db_index=True)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    class Meta:
        ordering = ["name"]
        indexes = [Index(fields=["name"])]

    def __str__(self):
        return self.name

class Collection(models.Model):
    name = models.CharField(max_length=100, db_index=True)
    description = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    class Meta:
        ordering = ["name"]
        indexes = [Index(fields=["name"])]

    def __str__(self):
        return self.name

class File(models.Model):
    name = models.CharField(max_length=512, db_index=True)
    path = models.CharField(max_length=1024)
    size = models.PositiveBigIntegerField(validators=[MinValueValidator(0)])
    mime_type = models.CharField(max_length=255, blank=True, db_index=True)
    sha1 = models.CharField(max_length=40, blank=True, db_index=True)
    sha256 = models.CharField(max_length=64, blank=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    class Meta:
        ordering = ["name", "-created_at"]
        indexes = [
            Index(fields=["name"]),
            Index(fields=["sha1"]),
            Index(fields=["sha256"]),
            Index(fields=["updated_at"]),
        ]

    def get_primary_storage(self):
        return self.storages.filter(is_primary=True).first()

    def get_latest_storage(self):
        return self.storages.order_by("-updated_at").first()

    def __str__(self):
        return self.name

class ItemQuerySet(models.QuerySet):
    def latest(self):
        return self.order_by("-date", "-updated_at")

class Item(models.Model):
    title = models.CharField(max_length=255, db_index=True)
    collections = models.ManyToManyField(Collection, related_name="items", blank=True)
    creators = models.ManyToManyField(Creator, related_name="items", blank=True)
    subjects = models.ManyToManyField(Subject, related_name="items", blank=True)
    languages = models.JSONField(default=list, blank=True)
    year = models.PositiveSmallIntegerField(null=True, blank=True)
    month = models.PositiveSmallIntegerField(null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(12)])
    day = models.PositiveSmallIntegerField(null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(31)])
    files = models.ManyToManyField(File, related_name="items", blank=True)
    date = models.DateField(null=True, blank=True, editable=False, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    objects = ItemQuerySet.as_manager()

    class Meta:
        ordering = ["-date", "title"]
        indexes = [
            Index(fields=["date"]),
            Index(fields=["title"]),
            Index(fields=["updated_at"]),
        ]
        constraints = [
            CheckConstraint(check=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),
            CheckConstraint(check=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),
            CheckConstraint(check=Q(month__isnull=True) | (Q(month__gte=1) & Q(month__lte=12)), name="month_range"),
            CheckConstraint(check=Q(day__isnull=True) | (Q(day__gte=1) & Q(day__lte=31)), name="day_range"),
        ]

    def clean(self):
        if self.month and not self.year:
            raise ValidationError("Month cannot exist without year.")
        if self.day and not self.month:
            raise ValidationError("Day cannot exist without month.")
        if self.month and (self.month < 1 or self.month > 12):
            raise ValidationError("Month must be between 1 and 12.")
        if self.day:
            try:
                datetime.date(self.year, self.month, self.day)
            except Exception:
                raise ValidationError("Invalid day for the given month/year.")

    def save(self, *args, **kwargs):
        if self.year:
            m = self.month if self.month else 1
            d = self.day if self.day else 1
            self.date = datetime.date(self.year, m, d)
        else:
            self.date = None
        self.full_clean()
        super().save(*args, **kwargs)

    def __str__(self):
        return self.title

class FileStorage(models.Model):
    file = models.ForeignKey(File, on_delete=models.CASCADE, related_name="storages")
    storage_name = models.CharField(max_length=100)
    path = models.CharField(max_length=1024)
    is_primary = models.BooleanField(default=False)
    sha1 = models.CharField(max_length=40, blank=True)
    sha256 = models.CharField(max_length=64, blank=True)
    extra = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    history = HistoricalRecords()

    class Meta:
        ordering = ["-is_primary", "-updated_at"]
        indexes = [
            Index(fields=["file", "path"]),
            Index(fields=["storage_name"]),
            Index(fields=["updated_at"]),
        ]
        constraints = [
            UniqueConstraint(fields=["file", "path"], name="unique_file_path"),
            UniqueConstraint(fields=["file"], condition=Q(is_primary=True), name="unique_primary_per_file"),
        ]

    def __str__(self):
        return f"{self.file.name} @ {self.storage_name}"
    
class Song(Item):
    history = HistoricalRecords()

    class Meta:
        proxy = False
        verbose_name = "Song"
        verbose_name_plural = "Songs"

class Book(Item):
    history = HistoricalRecords()

    class Meta:
        proxy = False
        verbose_name = "Book"
        verbose_name_plural = "Books"
