"""
Admin configuration for relationship models in the esperoj application.

This module contains admin classes for models that define relationships
between entities, including roles and external references.
"""

from django.contrib import admin
from django_select2.forms import Select2Widget

from ..models import Role, PersonExternalReference, ItemExternalReference
from .base import StandardModelAdmin, TabularInlineAdmin, AdminDisplayHelperMixin


class RoleInline(TabularInlineAdmin):
    """Inline admin for managing Person roles for an Item."""

    model = Role
    fk_name = "item"
    fields = ("person", "name", "order", "notes", "created_at", "updated_at")
    readonly_fields = ("created_at", "updated_at")
    autocomplete_fields = ("person",)
    select2_choice_fields = ["name"]
    extra = 1

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to the 'name' field."""
        if db_field.name == "name":
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": "Select role...",
                    "data-allow-clear": "true" if db_field.blank else "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("person", "item")


@admin.register(Role)
class RoleAdmin(StandardModelAdmin, AdminDisplayHelperMixin):
    """Admin configuration for the Role model."""

    # List display configuration
    list_display = (
        "person",
        "item",
        "name",
        "order",
        "admin_display_is_primary",
        "created_at",
        "updated_at",
    )

    list_filter = (
        "name",
        "order",
        "created_at",
        "updated_at",
    )

    search_fields = (
        "person__authorized_name",
        "person__sort_name",
        "item__title",
        "item__identifier",
        "name",
    )

    ordering = ("item", "name", "order", "person")

    # Form configuration
    autocomplete_fields = ("person", "item")
    select2_choice_fields = ["name"]

    fieldsets = (
        (
            None,
            {
                "fields": (
                    ("person", "item"),
                    ("name", "order"),
                    "notes",
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

    # Custom admin display methods
    admin_display_is_primary = AdminDisplayHelperMixin.make_boolean_display_method("is_primary", "Primary", "★", "☆")

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to choice fields."""
        if db_field.name == "name":
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": "Select role type...",
                    "data-allow-clear": "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("person", "item")

    def save_model(self, request, obj, form, change):
        """Custom save logic for Role model."""
        # Ensure order is at least 1
        if obj.order is None or obj.order < 1:
            obj.order = 1

        super().save_model(request, obj, form, change)


@admin.register(PersonExternalReference)
class PersonExternalReferenceAdmin(StandardModelAdmin):
    """Admin configuration for PersonExternalReference model."""

    # List display configuration
    list_display = (
        "person",
        "type",
        "label",
        "domain",
        "admin_display_is_active",
        "verified_at",  # Changed from 'last_verified' to 'verified_at'
        "updated_at",
    )

    list_filter = (
        "type",
        "is_active",
        # Removed 'verification_status'
        "created_at",
        "updated_at",
        # Removed 'last_verified'
    )

    search_fields = (
        "person__authorized_name",
        "person__sort_name",
        "url",
        "label",
        "type",
    )

    ordering = ("person", "type", "label")

    # Form configuration
    autocomplete_fields = ("person",)
    select2_choice_fields = ["type"]

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "person",
                    ("type", "label"),
                    "url",
                    "notes",
                )
            },
        ),
        (
            "Status",
            {
                "fields": (
                    ("is_active",),  # Removed 'verification_status'
                    "verified_at",
                ),
                "classes": ("collapse",),
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

    readonly_fields = ("created_at", "updated_at", "verified_at")

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to choice fields."""
        if db_field.name == "type":
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": "Select reference type...",
                    "data-allow-clear": "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    @admin.display(description="Active", boolean=True)
    def admin_display_is_active(self, obj):
        """Display active status with icon."""
        return obj.is_active

    @admin.display(description="Domain")
    def domain(self, obj):
        """Display the domain from the URL."""
        return obj.domain

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("person")

    def save_model(self, request, obj, form, change):
        """Custom save logic for PersonExternalReference model."""
        # Generate label from type if not provided
        if not obj.label and obj.type:
            obj.label = obj.get_type_display()

        super().save_model(request, obj, form, change)

    # Actions
    @admin.action(description="Mark selected references as verified")
    def mark_verified(self, request, queryset):
        """Mark selected references as verified."""
        updated = 0
        for ref in queryset:
            ref.mark_verified()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} references as verified.")

    @admin.action(description="Mark selected references as inactive")
    def mark_inactive(self, request, queryset):
        """Mark selected references as inactive."""
        updated = 0
        for ref in queryset:
            ref.mark_inactive()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} references as inactive.")

    actions = ["mark_verified", "mark_inactive"]


@admin.register(ItemExternalReference)
class ItemExternalReferenceAdmin(StandardModelAdmin):
    """Admin configuration for ItemExternalReference model."""

    # List display configuration
    list_display = (
        "item",
        "type",
        "label",
        "domain",
        "admin_display_is_active",
        "verified_at",  # Changed from 'last_verified' to 'verified_at'
        "updated_at",
    )

    list_filter = (
        "type",
        "is_active",
        # Removed 'verification_status'
        "item__item_type",
        "created_at",
        "updated_at",
        # Removed 'last_verified'
    )

    search_fields = (
        "item__title",
        "item__identifier",
        "url",
        "label",
        "type",
    )

    ordering = ("item", "type", "label")

    # Form configuration
    autocomplete_fields = ("item",)
    select2_choice_fields = ["type"]

    fieldsets = (
        (
            None,
            {
                "fields": (
                    "item",
                    ("type", "label"),
                    "url",
                    "notes",
                )
            },
        ),
        (
            "Status",
            {
                "fields": (
                    ("is_active",),  # Removed 'verification_status'
                    "verified_at",
                ),
                "classes": ("collapse",),
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

    readonly_fields = ("created_at", "updated_at", "verified_at")

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """Apply Select2Widget to choice fields."""
        if db_field.name == "type":
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": "Select reference type...",
                    "data-allow-clear": "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    @admin.display(description="Active", boolean=True)
    def admin_display_is_active(self, obj):
        """Display active status with icon."""
        return obj.is_active

    @admin.display(description="Domain")
    def domain(self, obj):
        """Display the domain from the URL."""
        return obj.domain

    def get_queryset(self, request):
        """Optimize queryset with select_related."""
        return super().get_queryset(request).select_related("item")

    def save_model(self, request, obj, form, change):
        """Custom save logic for ItemExternalReference model."""
        # Generate label from type if not provided
        if not obj.label and obj.type:
            obj.label = obj.get_type_display()

        super().save_model(request, obj, form, change)

    # Actions
    @admin.action(description="Mark selected references as verified")
    def mark_verified(self, request, queryset):
        """Mark selected references as verified."""
        updated = 0
        for ref in queryset:
            ref.mark_verified()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} references as verified.")

    @admin.action(description="Mark selected references as inactive")
    def mark_inactive(self, request, queryset):
        """Mark selected references as inactive."""
        updated = 0
        for ref in queryset:
            ref.mark_inactive()
            updated += 1

        self.message_user(request, f"Successfully marked {updated} references as inactive.")

    actions = ["mark_verified", "mark_inactive"]
