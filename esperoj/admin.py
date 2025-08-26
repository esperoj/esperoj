from django.contrib import admin
from simple_history.admin import SimpleHistoryAdmin

from .models import (
    Book,
    Collection,
    Contribution,
    File,
    FileBlock,
    FileReplica,
    MusicalWork,
    Person,
    Recording,
    Subject,
)


@admin.register(Person)
class PersonAdmin(SimpleHistoryAdmin):
    """Admin configuration for the Person model."""

    search_fields = ("authorized_name", "sort_name")
    list_display = (
        "authorized_name",
        "sort_name",
        "birth_date",
        "death_date",
        "updated_at",
    )
    ordering = ("sort_name",)
    prepopulated_fields = {"identifier": ("authorized_name",)}
    fieldsets = (
        (None, {"fields": ("authorized_name", "sort_name", "identifier")}),
        (
            "Biographical",
            {"fields": ("birth_date", "death_date", "biographical_note", "wikipedia_link")},
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
    readonly_fields = ("created_at", "updated_at")


@admin.register(Subject)
class SubjectAdmin(SimpleHistoryAdmin):
    """Admin configuration for the Subject model."""

    search_fields = ("name",)
    list_display = ("name", "created_at", "updated_at")
    ordering = ("name",)
    prepopulated_fields = {"identifier": ("name",)}


@admin.register(Collection)
class CollectionAdmin(SimpleHistoryAdmin):
    """Admin configuration for the Collection model."""

    search_fields = ("name",)
    list_display = ("name", "created_at", "updated_at")
    ordering = ("name",)
    prepopulated_fields = {"identifier": ("name",)}


class FileReplicaInline(admin.TabularInline):
    """Inline for FileReplicas within the File admin."""

    model = FileReplica
    fk_name = "file"
    extra = 0
    fields = ("replica_type", "storage_name", "updated_at")
    readonly_fields = ("updated_at",)
    show_change_link = True


class FileBlockInline(admin.TabularInline):
    """Inline for FileBlocks within the FileReplica admin."""

    model = FileBlock
    fk_name = "replica"
    extra = 0
    fields = ("block_order", "file_path", "size", "mime_type", "sha256", "updated_at")
    readonly_fields = ("updated_at",)
    show_change_link = True


@admin.register(File)
class FileAdmin(SimpleHistoryAdmin):
    """Admin configuration for the File model."""

    inlines = (FileReplicaInline,)
    list_display = (
        "name",
        "size",
        "mime_type",
        "sha256",
        "updated_at",
    )
    search_fields = ("name", "md5", "sha1", "sha256")
    list_filter = ("mime_type",)
    readonly_fields = ("created_at", "updated_at")
    ordering = ("name", "-updated_at")
    fieldsets = (
        (None, {"fields": ("name", "size", "mime_type")}),
        ("Hashes", {"fields": ("md5", "sha1", "sha256")}),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )


@admin.register(FileReplica)
class FileReplicaAdmin(SimpleHistoryAdmin):
    """Admin configuration for the FileReplica model."""

    inlines = (FileBlockInline,)
    list_display = ("file", "replica_type", "storage_name", "updated_at")
    search_fields = ("file__name", "storage_name")
    list_filter = ("replica_type", "storage_name")
    autocomplete_fields = ("file",)


@admin.register(FileBlock)
class FileBlockAdmin(SimpleHistoryAdmin):
    """Admin configuration for the FileBlock model."""

    list_display = ("replica", "block_order", "file_path", "size", "updated_at")
    search_fields = ("replica__file__name", "file_path")
    readonly_fields = ("created_at", "updated_at")
    autocomplete_fields = ("replica",)


class ContributionInline(admin.TabularInline):
    """Inline for Contributions within Item-related admins."""

    model = Contribution
    extra = 1
    autocomplete_fields = ("person",)


class ItemAdmin(SimpleHistoryAdmin):
    """Base admin configuration for models inheriting from Item."""

    inlines = (ContributionInline,)
    list_display = ("title", "identifier", "item_type", "date", "created_at", "updated_at")
    list_filter = ("item_type", "date")
    search_fields = ("title", "identifier", "description", "people__authorized_name")
    readonly_fields = ("created_at", "updated_at", "date")
    ordering = ("-date", "identifier")
    fieldsets = (
        (None, {"fields": ("title", "identifier", "description")}),
        (
            "Date Information",
            {"fields": (("year", "month", "day"), "date")},
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )


@admin.register(MusicalWork)
class MusicalWorkAdmin(ItemAdmin):
    """Admin configuration for the MusicalWork model."""

    pass


@admin.register(Recording)
class RecordingAdmin(ItemAdmin):
    """Admin configuration for the Recording model."""

    fieldsets = (
        (None, {"fields": ("title", "identifier", "description")}),  # Added 'title'
        ("Recording Details", {"fields": ("work",)}),
        (
            "Date Information",
            {"fields": (("year", "month", "day"), "date")},
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
    autocomplete_fields = ("work",)


@admin.register(Book)
class BookAdmin(ItemAdmin):
    """Admin configuration for the Book model."""

    fieldsets = (
        (None, {"fields": ("title", "identifier", "description")}),  # Added 'title'
        ("Book Details", {"fields": ("subtitle", "isbn_10", "isbn_13")}),
        (
            "Date Information",
            {"fields": (("year", "month", "day"), "date")},
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
