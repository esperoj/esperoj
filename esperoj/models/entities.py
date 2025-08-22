from django.conf import settings
from django.db import models
from django.db.models import UniqueConstraint, Index

from .base import BaseModel


class BaseName(BaseModel):
    """An abstract base model for language-specific names."""

    language = models.CharField(
        max_length=10,
        default=settings.LANGUAGE_CODE,
        help_text="Language code (e.g., 'en', 'es', 'fr')",
    )
    name = models.CharField(max_length=512)

    class Meta:
        abstract = True
        ordering = ["language", "name"]
        app_label = "esperoj"

    def __str__(self):
        return f'"{self.name}" ({self.language})'


class Creator(BaseModel):
    """A generic creator entity."""

    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Creator"
        verbose_name_plural = "Creators"
        indexes = [Index(fields=["identifier"])]
        db_table = "creator"

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name if primary_name else (self.names.first().name if self.names.exists() else self.identifier)
        )


class CreatorName(BaseName):
    creator = models.ForeignKey(Creator, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "Creator Name"
        verbose_name_plural = "Creator Names"
        constraints = [UniqueConstraint(fields=["creator", "language"], name="unique_name_per_lang_for_creator")]
        db_table = "creator_name"


class Artist(BaseModel):
    """An artist, typically associated with music or visual arts."""

    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Artist"
        verbose_name_plural = "Artists"
        indexes = [Index(fields=["identifier"])]
        db_table = "artist"

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name if primary_name else (self.names.first().name if self.names.exists() else self.identifier)
        )


class ArtistName(BaseName):
    artist = models.ForeignKey(Artist, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "Artist Name"
        verbose_name_plural = "Artist Names"
        constraints = [UniqueConstraint(fields=["artist", "language"], name="unique_name_per_lang_for_artist")]
        db_table = "artist_name"


class Author(BaseModel):
    """An author, typically associated with books or written works."""

    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Author"
        verbose_name_plural = "Authors"
        indexes = [Index(fields=["identifier"])]
        db_table = "author"

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name if primary_name else (self.names.first().name if self.names.exists() else self.identifier)
        )


class AuthorName(BaseName):
    author = models.ForeignKey(Author, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "Author Name"
        verbose_name_plural = "Author Names"
        constraints = [UniqueConstraint(fields=["author", "language"], name="unique_name_per_lang_for_author")]
        db_table = "author_name"


class Subject(BaseModel):
    """A subject, topic, or keyword used for categorization."""

    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Subject"
        verbose_name_plural = "Subjects"
        indexes = [Index(fields=["identifier"])]
        db_table = "subject"

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name if primary_name else (self.names.first().name if self.names.exists() else self.identifier)
        )


class SubjectName(BaseName):
    subject = models.ForeignKey(Subject, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "Subject Name"
        verbose_name_plural = "Subject Names"
        constraints = [UniqueConstraint(fields=["subject", "language"], name="unique_name_per_lang_for_subject")]
        db_table = "subject_name"


class Collection(BaseModel):
    """A collection that groups multiple Items."""

    identifier = models.SlugField(max_length=255, unique=True)
    description = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["identifier"]
        verbose_name = "Collection"
        verbose_name_plural = "Collections"
        indexes = [Index(fields=["identifier"])]
        db_table = "collection"

    def __str__(self):
        primary_name = self.names.filter(language=settings.LANGUAGE_CODE).first()
        return (
            primary_name.name if primary_name else (self.names.first().name if self.names.exists() else self.identifier)
        )


class CollectionName(BaseName):
    collection = models.ForeignKey(Collection, on_delete=models.CASCADE, related_name="names")

    class Meta(BaseName.Meta):
        verbose_name = "Collection Name"
        verbose_name_plural = "Collection Names"
        constraints = [
            UniqueConstraint(
                fields=["collection", "language"],
                name="unique_name_per_lang_for_collection",
            )
        ]
        db_table = "collection_name"
