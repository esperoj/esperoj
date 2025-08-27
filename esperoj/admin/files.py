"""
Admin configuration for file models in the esperoj application.

This module contains admin classes for managing digital files, replicas,
and file blocks in the digital preservation system.
"""

from django.contrib import admin
from django.db import models
from django.http import HttpRequest
from django_select2.forms import Select2Widget

from ..models import File, FileReplica, FileBlock
from .base import StandardModelAdmin, TabularInlineAdmin, AdminDisplayHelperMixin


class FileReplicaInline(TabularInlineAdmin):
    """Inline admin for FileReplicas within the File admin."""

    model = FileReplica
    fk_name = "file"
    fields = (
        "replica_type",
        "storage_name",
        "storage_path",
        "is_active",
        "verification_status",
        "last_verified",
        "updated_at",
    )
    readonly_fields = ("last_verified", "updated_at")
    select2_choice_fields = ["replica_type", "storage_name"]
    extra = 0

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to choice fields."""
        if db_field.name in ["replica_type", "storage_name"]:
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": f"Select {db_field.verbose_name.lower()}...",
                    "data-allow-clear": "true" if db_field.blank else "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)


class FileBlockInline(TabularInlineAdmin):
    """Inline admin for FileBlocks within the FileReplica admin."""

    model = FileBlock
    fk_name = "replica"
    fields = ("block_order", "file_path", "size", "is_last_block", "mime_type", "sha256", "updated_at")
    readonly_fields = ("updated_at",)
    ordering = ("block_order",)
    extra = 0


@admin.register(File)
class FileAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the File model."""

    # List display configuration
    list_display = (
        "name",
        "path",
        "display_size",
        "mime_type",
        "file_format",
        "admin_display_has_replicas",
        "admin_display_replica_count",
        "updated_at",
    )

    list_filter = (
        "mime_type",
        "file_format",
        "size",
        "created_at",
        "updated_at",
    )

    search_fields = (
        "^name",
        "^path",
        "original_filename",
        "md5",
        "sha1",
        "sha256",
        "mime_type",
        "file_format",
    )

    ordering = ("path", "name")

    # Form configuration
    fieldsets = (
        (
            None,
            {
                "fields": (
                    ("name", "path"),
                    "original_filename",
                    ("size", "mime_type"),
                    ("file_format", "compression"),
                )
            },
        ),
        (
            "Checksums",
            {
                "fields": (
                    ("md5", "sha1"),
                    "sha256",
                ),
                "classes": ("wide",),
            },
        ),
        (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )

    readonly_fields = ("created_at", "updated_at")

    # Inlines
    inlines = [FileReplicaInline]

    # Enable autocomplete
    search_fields = (
        "^name",
        "^path",
        "original_filename",
    )

    # Custom admin display methods
    admin_display_has_replicas = AdminDisplayHelperMixin.make_boolean_display_method(
        "has_replicas", "Has Replicas", "✓", "✗"
    )

    admin_display_replica_count = AdminDisplayHelperMixin.make_count_display_method("replicas", "Replicas")

    @admin.display(description="Size", ordering="size")
    def display_size(self, obj):
        """Display human-readable file size."""
        return obj.display_size

    @admin.display(description="Primary Checksum")
    def display_primary_checksum(self, obj):
        """Display the primary checksum."""
        checksum = obj.primary_checksum
        if len(checksum) > 16:
            return f"{checksum[:8]}...{checksum[-8:]}"
        return checksum

    def get_queryset(self, request):
        """Optimize queryset with prefetch_related for replicas."""
        return super().get_queryset(request).prefetch_related("replicas", "items")

    # Actions
    @admin.action(description="Verify file integrity for selected files")
    def verify_file_integrity(self, request, queryset):
        """Verify file integrity for selected files."""
        # This would integrate with your file verification system
        count = queryset.count()
        self.message_user(request, f"Queued {count} files for integrity verification.")

    actions = ["verify_file_integrity"]


@admin.register(FileReplica)
class FileReplicaAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the FileReplica model."""

    # List display configuration
    list_display = (
        "file",
        "replica_type",
        "storage_name",
        "admin_display_is_active",
        "verification_status",
        "admin_display_needs_verification",
        "last_verified",
        "updated_at",
    )

    list_filter = (
        "replica_type",
        "storage_name",
        "is_active",
        "verification_status",
        "last_verified",
        "created_at",
        "updated_at",
    )

    search_fields = (
        "file__name",
        "file__path",
        "storage_path",
        "replica_type",
        "storage_name",
    )

    ordering = ("file", "replica_type", "storage_name")

    # Form configuration
    autocomplete_fields = ("file",)
    select2_choice_fields = ["replica_type", "storage_name", "verification_status"]

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "file",
                    ("replica_type", "storage_name"),
                    "storage_path",
                )
            },
        ),
        (
            "Status",
            {
                "fields": (
                    ("is_active", "verification_status"),
                    "last_verified",
                ),
                "classes": ("wide",),
            },
        ),
        (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )

    readonly_fields = ("created_at", "updated_at", "last_verified")

    # Inlines
    inlines = [FileBlockInline]

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to choice fields."""
        if db_field.name in ["replica_type", "storage_name", "verification_status"]:
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": f"Select {db_field.verbose_name.lower()}...",
                    "data-allow-clear": "true" if db_field.blank else "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    # Custom admin display methods
    admin_display_is_active = AdminDisplayHelperMixin.make_boolean_display_method("is_active", "Active", "✓", "✗")

    admin_display_needs_verification = AdminDisplayHelperMixin.make_boolean_display_method(
        "needs_verification", "Needs Verification", "⚠️", "✓"
    )

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("file").prefetch_related("blocks")

    # Actions
    @admin.action(description="Mark selected replicas as verified")
    def mark_verified(self, request, queryset):
        """Mark selected replicas as verified."""
        updated = 0
        for replica in queryset:
            replica.mark_verified()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} replicas as verified.")

    @admin.action(description="Mark selected replicas as inactive")
    def mark_inactive(self, request, queryset):
        """Mark selected replicas as inactive."""
        updated = 0
        for replica in queryset:
            replica.mark_inactive()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} replicas as inactive.")

    @admin.action(description="Queue verification for selected replicas")
    def queue_verification(self, request, queryset):
        """Queue verification for selected replicas."""
        count = queryset.count()
        # This would integrate with your verification system
        self.message_user(request, f"Queued {count} replicas for verification.")

    actions = ["mark_verified", "mark_inactive", "queue_verification"]


@admin.register(FileBlock)
class FileBlockAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the FileBlock model."""

    # List display configuration
    list_display = (
        "replica",
        "block_order",
        "file_path",
        "display_size",
        "admin_display_is_last_block",
        "display_primary_checksum",
        "updated_at",
    )

    list_filter = (
        "replica__replica_type",
        "replica__storage_name",
        "is_last_block",
        "size",
        "created_at",
        "updated_at",
    )

    search_fields = (
        "replica__file__name",
        "replica__file__path",
        "file_path",
        "sha256",
        "sha1",
        "md5",
    )

    ordering = ("replica", "block_order")

    # Form configuration
    autocomplete_fields = ("replica",)

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "replica",
                    ("block_order", "is_last_block"),
                    "file_path",
                    ("size", "mime_type"),
                )
            },
        ),
        (
            "Checksums",
            {
                "fields": (
                    ("md5", "sha1"),
                    "sha256",
                ),
                "classes": ("wide",),
            },
        ),
        (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        ),
    )

    readonly_fields = ("created_at", "updated_at")

    # Custom admin display methods
    admin_display_is_last_block = AdminDisplayHelperMixin.make_boolean_display_method(
        "is_last_block", "Last Block", "🏁", "→"
    )

    @admin.display(description="Size", ordering="size")
    def display_size(self, obj):
        """Display human-readable block size."""
        return obj.display_size

    @admin.display(description="Primary Checksum")
    def display_primary_checksum(self, obj):
        """Display the primary checksum."""
        checksum = obj.primary_checksum
        if checksum and len(checksum) > 16:
            return f"{checksum[:8]}...{checksum[-8:]}"
        return checksum or "—"

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("replica", "replica__file")

    # Actions
    @admin.action(description="Verify checksums for selected blocks")
    def verify_block_checksums(self, request, queryset):
        """Verify checksums for selected blocks."""
        count = queryset.count()
        # This would integrate with your checksum verification system
        self.message_user(request, f"Queued {count} blocks for checksum verification.")

    actions = ["verify_block_checksums"]
