"""
Base admin utilities and mixins for the esperoj application.

This module contains common utilities, mixins, and helper functions used
across multiple admin classes to promote code reuse and consistency.
"""

from django import forms
from django.contrib import admin
from django.db import models
from django.http import HttpRequest
from django_select2.forms import Select2Widget, Select2MultipleWidget
from simple_history.admin import SimpleHistoryAdmin


class TimestampFieldsetMixin:
    """
    Mixin to add a standard timestamp fieldset to admin forms.

    Adds a collapsed fieldset containing created_at and updated_at fields.
    """

    @property
    def timestamp_fieldset(self) -> tuple:
        """Returns the standard timestamp fieldset."""
        return (
            "Timestamps",
            {
                "fields": ("created_at", "updated_at"),
                "classes": ("collapse",),
            },
        )


class LanguagesSelect2MultipleFormField(forms.MultipleChoiceField):
    """
    A custom form field for JSONField that stores a list of language codes.

    Uses Select2MultipleWidget to allow for a user-friendly multi-select
    interface with the ability to add new language codes.
    """

    widget = Select2MultipleWidget(
        attrs={
            "data-tags": "true",
            "data-placeholder": "Select or type language codes",
            "data-minimum-input-length": "0",
        }
    )

    def __init__(self, *args, **kwargs):
        initial = kwargs.get("initial", [])

        # Get common language codes for choices
        common_languages = [
            ("en", "English"),
            ("es", "Spanish"),
            ("fr", "French"),
            ("de", "German"),
            ("it", "Italian"),
            ("pt", "Portuguese"),
            ("ja", "Japanese"),
            ("zh", "Chinese"),
            ("ko", "Korean"),
            ("ru", "Russian"),
            ("ar", "Arabic"),
        ]

        # Combine with any initial values
        all_choices = list(common_languages)
        for lang in initial:
            if lang and (lang, lang) not in all_choices:
                all_choices.append((lang, lang))

        kwargs["choices"] = all_choices
        super().__init__(*args, **kwargs)

    def clean(self, value):
        """Clean the input value to ensure it's a list of strings."""
        if not value:
            return []
        return [str(item).strip().lower() for item in value if item]


class LanguagesFormFieldMixin:
    """
    Mixin to handle JSONField language lists with Select2 interface.

    This mixin provides a reusable way to handle language selection
    fields that are stored as JSON arrays.
    """

    def formfield_for_dbfield(self, db_field: models.Field, request: HttpRequest, **kwargs):
        """
        Apply custom language field for 'languages' JSONField,
        then delegate to the superclass for other fields.

        This method has been updated to safely call the superclass's
        formfield_for_dbfield, providing a fallback to the database
        field's default formfield if no such method is found in the MRO.
        This resolves static analysis warnings about `super()` potentially
        resolving to `object`, which does not have a `formfield_for_dbfield`.
        """
        if db_field.name == "languages" and isinstance(db_field, models.JSONField):
            return LanguagesSelect2MultipleFormField(required=not db_field.blank, **kwargs)

        # Safely attempt to call the superclass's formfield_for_dbfield.
        # This makes the mixin more robust against incorrect usage or strict linters.
        super_formfield_for_dbfield = getattr(super(), "formfield_for_dbfield", None)
        if super_formfield_for_dbfield:
            return super_formfield_for_dbfield(db_field, request, **kwargs)
        else:
            # Fallback to the database field's default formfield.
            # This path should ideally only be taken if LanguagesFormFieldMixin
            # is used with a base class that does not provide formfield_for_dbfield,
            # which would typically indicate an incorrect MRO setup for an admin class.
            return db_field.formfield(**kwargs)


class AdminDisplayHelperMixin:
    """
    Mixin providing helper methods for creating admin display methods.

    This mixin provides utilities for creating consistent display methods
    for related objects and computed fields.
    """

    @staticmethod
    def make_admin_display_method(property_name: str, short_description: str):
        """
        Creates an admin display method for a given property.

        Args:
            property_name: The name of the property to display
            short_description: The column header text

        Returns:
            A method suitable for use in admin list_display
        """

        def _admin_display(self, obj):
            return getattr(obj, property_name, "")

        _admin_display.short_description = short_description
        return _admin_display

    @staticmethod
    def make_boolean_display_method(
        property_name: str, short_description: str, true_icon: str = "✓", false_icon: str = "✗"
    ):
        """
        Creates an admin display method for boolean properties with icons.

        Args:
            property_name: The name of the boolean property
            short_description: The column header text
            true_icon: Icon to show for True values
            false_icon: Icon to show for False values

        Returns:
            A method suitable for use in admin list_display
        """

        def _admin_display(self, obj):
            value = getattr(obj, property_name, False)
            return true_icon if value else false_icon

        _admin_display.short_description = short_description
        _admin_display.boolean = True
        return _admin_display

    @staticmethod
    def make_count_display_method(related_name: str, short_description: str):
        """
        Creates an admin display method for counting related objects.

        Args:
            related_name: The name of the related field to count
            short_description: The column header text

        Returns:
            A method suitable for use in admin list_display
        """

        def _admin_display(self, obj):
            return getattr(obj, related_name).count()

        _admin_display.short_description = short_description
        return _admin_display


class StandardModelAdmin(SimpleHistoryAdmin):
    """
    Standard model admin class with history tracking.

    This class should be used as the base for most admin classes
    in the application, providing a consistent set of features
    including history tracking, enhanced search, and Select2 widgets.
    """

    # Default configuration
    list_per_page = 50
    readonly_fields = ("created_at", "updated_at")
    show_full_result_count = False  # Performance optimization for large datasets

    def __init__(self, model, admin_site):
        super().__init__(model, admin_site)

        # Automatically set date_hierarchy if model has common date fields
        if not getattr(self, "date_hierarchy", None):
            for field_name in ["created_at", "date", "updated_at"]:
                if hasattr(model, field_name):
                    self.date_hierarchy = field_name
                    break

    def get_readonly_fields(self, request: HttpRequest, obj=None) -> tuple:
        """Returns readonly fields, adding ID for existing objects."""
        readonly = list(super().get_readonly_fields(request, obj))
        if obj and hasattr(obj, "id") and "id" not in readonly:
            readonly.insert(0, "id")
        return tuple(readonly)

    def get_search_fields(self, request: HttpRequest) -> list[str]:
        """Returns optimized search fields."""
        search_fields = list(super().get_search_fields(request))

        # Optimize search patterns for better performance
        optimized_fields = []
        for field in search_fields:
            if not field.startswith(("^", "=", "@")):
                # Add ^ for startswith search for better performance on key fields
                if any(key_field in field for key_field in ["title", "name", "authorized_name", "identifier"]):
                    optimized_fields.append(f"^{field}")
                else:
                    optimized_fields.append(field)
            else:
                optimized_fields.append(field)

        return optimized_fields

    def formfield_for_choice_field(self, db_field: models.Field, request: HttpRequest, **kwargs):
        """Apply Select2Widget to choice fields where appropriate."""
        select2_choice_fields = getattr(self, "select2_choice_fields", [])
        if db_field.name in select2_choice_fields:
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": f"Select {db_field.verbose_name.lower()}...",
                    "data-allow-clear": "true" if db_field.blank else "false",
                }
            )
        return super().formfield_for_choice_field(db_field, request, **kwargs)

    def formfield_for_foreignkey(self, db_field: models.ForeignKey, request: HttpRequest, **kwargs):
        """Apply Select2Widget to foreign key fields where appropriate."""
        select2_fk_fields = getattr(self, "select2_fk_fields", [])
        if db_field.name in select2_fk_fields:
            kwargs["widget"] = Select2Widget(
                attrs={
                    "data-placeholder": f"Select {db_field.verbose_name.lower()}...",
                    "data-allow-clear": "true" if db_field.blank else "false",
                }
            )
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def formfield_for_manytomany(self, db_field: models.ManyToManyField, request: HttpRequest, **kwargs):
        """Apply Select2MultipleWidget to many-to-many fields where appropriate."""
        select2_m2m_fields = getattr(self, "select2_m2m_fields", [])
        if db_field.name in select2_m2m_fields:
            kwargs["widget"] = Select2MultipleWidget(
                attrs={
                    "data-placeholder": f"Select {db_field.verbose_name.lower()}...",
                    "data-allow-clear": "true",
                }
            )
        return super().formfield_for_manytomany(db_field, request, **kwargs)


class TabularInlineAdmin(admin.TabularInline):
    """
    Enhanced tabular inline admin with common configurations.

    Provides sensible defaults and common functionality for
    inline admin forms.
    """

    extra = 0
    show_change_link = True

    def get_readonly_fields(self, request: HttpRequest, obj=None) -> tuple:
        """Add timestamp fields to readonly if they exist."""
        readonly = list(super().get_readonly_fields(request, obj))

        if hasattr(self.model, "created_at") and "created_at" not in readonly:
            readonly.append("created_at")
        if hasattr(self.model, "updated_at") and "updated_at" not in readonly:
            readonly.append("updated_at")

        return tuple(readonly)


class StackedInlineAdmin(admin.StackedInline):
    """
    Enhanced stacked inline admin with common configurations.

    Provides sensible defaults and common functionality for
    stacked inline admin forms.
    """

    extra = 0
    show_change_link = True

    def get_readonly_fields(self, request: HttpRequest, obj=None) -> tuple:
        """Add timestamp fields to readonly if they exist."""
        readonly = list(super().get_readonly_fields(request, obj))

        if hasattr(self.model, "created_at") and "created_at" not in readonly:
            readonly.append("created_at")
        if hasattr(self.model, "updated_at") and "updated_at" not in readonly:
            readonly.append("updated_at")

        return tuple(readonly)
