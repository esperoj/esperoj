"""
Admin configuration for core entities in the esperoj application.

This module contains admin classes for the fundamental entities:
Person, Subject, and Collection.
"""

from django.contrib import admin
from django.db import models

from ..models import Person, Subject, Collection
from .base import StandardModelAdmin, TabularInlineAdmin, AdminDisplayHelperMixin


class PersonExternalReferenceInline(TabularInlineAdmin):
    """Inline admin for PersonExternalReference."""

    from ..models import PersonExternalReference

    model = PersonExternalReference
    fk_name = "person"
    fields = ("type", "url", "label", "is_active", "notes")
    readonly_fields = ("created_at", "updated_at")

    def get_extra(self, request, obj=None, **kwargs):
        """Return number of extra forms."""
        return 1 if obj else 0


@admin.register(Person)
class PersonAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the Person model."""

    # List display configuration
    list_display = (
        "authorized_name",
        "sort_name",
        "display_birth_year",
        "display_death_year",
        "admin_display_is_living",
        "admin_display_role_count",
        "updated_at",
    )

    list_filter = (
        "birth_date",
        "death_date",
        "created_at",
        "updated_at",
    )

    search_fields = (
        "^authorized_name",
        "^sort_name",
        "biographical_note",
        "identifier",
    )

    ordering = ("sort_name", "authorized_name")

    # Form configuration
    prepopulated_fields = {"identifier": ("authorized_name",)}

    fieldsets = (
        (
            None,
            {
                "fields": (
                    ("authorized_name", "sort_name"),
                    "identifier",
                )
            },
        ),
        (
            "Biographical Information",
            {
                "fields": (
                    ("birth_date", "death_date"),
                    "biographical_note",
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
    inlines = [PersonExternalReferenceInline]

    # Enable autocomplete
    search_fields = (
        "^authorized_name",
        "^sort_name",
        "identifier",
    )

    # Custom admin display methods
    admin_display_is_living = AdminDisplayHelperMixin.make_boolean_display_method("is_living", "Living", "✓", "✗")

    admin_display_role_count = AdminDisplayHelperMixin.make_count_display_method("roles", "Roles")

    @admin.display(description="Birth Year", ordering="birth_date")
    def display_birth_year(self, obj):
        """Display birth year or dash if unknown."""
        return obj.birth_date.year if obj.birth_date else "—"

    @admin.display(description="Death Year", ordering="death_date")
    def display_death_year(self, obj):
        """Display death year or dash if still living."""
        return obj.death_date.year if obj.death_date else "—"

    def get_queryset(self, request):
        """Optimize queryset with prefetch_related for roles."""
        return super().get_queryset(request).prefetch_related("roles__item", "external_references")

    def save_model(self, request, obj, form, change):
        """Custom save logic for Person model."""
        # Ensure sort_name is generated if not provided
        if not obj.sort_name and obj.authorized_name:
            name_parts = obj.authorized_name.split()
            if len(name_parts) > 1:
                last_part = name_parts[-1]
                first_parts = " ".join(name_parts[:-1])
                obj.sort_name = f"{last_part}, {first_parts}"
            else:
                obj.sort_name = obj.authorized_name

        super().save_model(request, obj, form, change)


@admin.register(Subject)
class SubjectAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the Subject model."""

    # List display configuration
    list_display = (
        "name",
        "identifier",
        "admin_display_item_count",
        "created_at",
        "updated_at",
    )

    list_filter = (
        "created_at",
        "updated_at",
    )

    search_fields = (
        "^name",
        "identifier",
        "description",
    )

    ordering = ("name",)

    # Form configuration
    prepopulated_fields = {"identifier": ("name",)}

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "name",
                    "identifier",
                    "description",
                )
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

    # Enable autocomplete
    search_fields = (
        "^name",
        "identifier",
    )

    # Custom admin display methods
    admin_display_item_count = AdminDisplayHelperMixin.make_count_display_method("items", "Items")

    def get_queryset(self, request):
        """Optimize queryset with prefetch_related for items."""
        return super().get_queryset(request).prefetch_related("items")


@admin.register(Collection)
class CollectionAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the Collection model."""

    # List display configuration
    list_display = (
        "name",
        "identifier",
        "admin_display_item_count",
        "display_latest_item_date",
        "created_at",
        "updated_at",
    )

    list_filter = (
        "created_at",
        "updated_at",
    )

    search_fields = (
        "^name",
        "identifier",
        "description",
    )

    ordering = ("name",)

    # Form configuration
    prepopulated_fields = {"identifier": ("name",)}

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "name",
                    "identifier",
                    "description",
                )
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

    # Enable autocomplete
    search_fields = (
        "^name",
        "identifier",
    )

    # Custom admin display methods
    admin_display_item_count = AdminDisplayHelperMixin.make_count_display_method("items", "Items")

    @admin.display(description="Latest Item", ordering="items__date")
    def display_latest_item_date(self, obj):
        """Display the date of the most recent item in the collection."""
        latest_date = obj.latest_item_date
        return latest_date.strftime("%Y-%m-%d") if latest_date else "—"

    def get_queryset(self, request):
        """Optimize queryset with prefetch_related for items."""
        return super().get_queryset(request).prefetch_related("items").annotate(item_count=models.Count("items"))
