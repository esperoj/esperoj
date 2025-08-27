"""
Admin configuration for item models in the esperoj application.

This module contains admin classes for catalogued items including the base
Item model and specific item types like Song and Book.
"""

import typing

from django.contrib import admin
from django.db import models
from django.http import HttpRequest
from django_select2.forms import Select2Widget, Select2MultipleWidget
from django import forms  # Import django.forms for custom form fields

from ..models import Item, Song, Book
from .base import StandardModelAdmin, LanguagesFormFieldMixin, AdminDisplayHelperMixin
from .relationships import RoleInline

_Choices = list[tuple[str, str]]


# Custom Form for ItemAdmin to handle the languages field with Select2MultipleWidget
class ItemAdminForm(forms.ModelForm):
    # This field will be dynamically populated with choices by the ModelAdmin's get_form method.
    base_fields: typing.Dict[str, forms.Field]
    languages = forms.MultipleChoiceField(
        required=False,  # Make it not required, matching JSONField common usage
        choices=[],  # Will be overridden in Admin's get_form
        widget=Select2MultipleWidget(
            attrs={
                "data-placeholder": "Select languages...",
                "data-allow-clear": "true",  # Allow clearing all selections
                # For server-side autocomplete with many choices, you might add:
                # "data-ajax--url": "/admin/your_app/item/language_autocomplete/",
                # However, for typical language lists, client-side filtering on a pre-defined list is common
                # when choices are provided directly.
            }
        ),
        help_text="Select multiple languages associated with this item.",
    )

    class Meta:
        model = Item
        fields = "__all__"  # Include all model fields

    # Override clean_languages to ensure it returns a list, which MultipleChoiceField already does.
    # This is mainly to ensure the data is in the expected format for JSONField.
    def clean_languages(self):
        data = self.cleaned_data["languages"]
        # The Select2MultipleWidget for MultipleChoiceField will return a list.
        # Ensure that if no selection, it's an empty list, not None.
        return data if data is not None else []


class ItemExternalReferenceInline(admin.TabularInline):
    """Inline admin for ItemExternalReference."""

    from ..models import ItemExternalReference

    model = ItemExternalReference
    fk_name = "item"
    fields = ("type", "url", "label", "is_active", "notes")
    readonly_fields = ("created_at", "updated_at")
    extra = 0

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


@admin.register(Item)
class ItemAdmin(StandardModelAdmin, LanguagesFormFieldMixin, AdminDisplayHelperMixin):
    """Base admin configuration for the Item model."""

    # Assign the custom form
    form = ItemAdminForm

    # List display configuration
    list_display = (
        "title",
        "identifier",
        "item_type",
        "display_date",
        "admin_display_languages",
        "admin_display_creators",
        "admin_display_contributors",
        "admin_display_file_count",
        "created_at",
        "updated_at",
    )

    list_filter = (
        "item_type",
        "year",
        "created_at",
        "updated_at",
        "collections",
        "subjects",
    )

    search_fields = (
        "^title",
        "^identifier",
        "subtitle",
        "description",
        "notes",
        "people__authorized_name",
        "people__sort_name",
    )

    ordering = ("-date", "-created_at", "identifier")

    # Form configuration
    prepopulated_fields = {"identifier": ("title",)}
    select2_m2m_fields = ["subjects", "collections", "files"]
    autocomplete_fields = ("subjects", "collections", "files")

    # Base fieldsets that subclasses can extend
    base_fieldsets = [
        (
            None,
            {
                "fields": (
                    ("title", "subtitle"),
                    "identifier",
                    "description",
                    "languages",  # This field is now handled by the custom form
                )
            },
        ),
        (
            "Date Information",
            {
                "fields": (
                    ("year", "month", "day"),
                    "date",
                ),
                "classes": ("wide",),
            },
        ),
        (
            "Relationships",
            {
                "fields": (
                    "subjects",
                    "collections",
                    "files",
                ),
                "classes": ("wide",),
            },
        ),
        (
            "Notes",
            {
                "fields": ("notes",),
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
    ]

    fieldsets = base_fieldsets

    readonly_fields = ("created_at", "updated_at", "date")

    # Inlines
    inlines = [RoleInline, ItemExternalReferenceInline]

    # Filter horizontal for better M2M widget
    filter_horizontal = ("subjects", "collections", "files")

    # Custom admin display methods
    admin_display_creators = AdminDisplayHelperMixin.make_admin_display_method("display_creators", "Creators")
    admin_display_contributors = AdminDisplayHelperMixin.make_admin_display_method(
        "display_contributors", "Contributors"
    )
    admin_display_file_count = AdminDisplayHelperMixin.make_count_display_method("files", "Files")

    @admin.display(description="Languages", ordering="languages")
    def admin_display_languages(self, obj):
        """Display languages as a comma-separated string."""
        return obj.display_languages or "—"

    @admin.display(description="Date", ordering="date")
    def display_date(self, obj):
        """Display formatted date."""
        return obj.display_date

    def get_queryset(self, request):
        """Optimize queryset with prefetch_related for related objects."""
        return (
            super()
            .get_queryset(request)
            .select_related()
            .prefetch_related("people", "roles__person", "subjects", "collections", "files", "external_references")
        )

    # Override get_form to pass language choices to the custom form
    def get_form(self, request, obj=None, **kwargs) -> typing.Type[ItemAdminForm]:
        """
        Dynamically sets the choices for the 'languages' field on the form class.
        """
        form_class_base = super().get_form(request, obj, **kwargs)

        # Cast the form class to inform Pyright of its specific type.
        form_class = typing.cast(typing.Type[ItemAdminForm], form_class_base)

        # Declare the expected type of the choices variable.
        choices: _Choices

        language_choices_method = getattr(self, "get_language_choices", None)

        if callable(language_choices_method):
            # Call the dynamic method.
            result = language_choices_method()

            # Use an assertion. This is a runtime check that also proves to
            # Pyright that 'result' is a list, satisfying the type checker.
            assert isinstance(result, list), f"get_language_choices must return a list, but got {type(result).__name__}"

            # Now, Pyright knows `result` is a list, and the assignment is safe.
            choices = result
        else:
            # The fallback list already has the correct type.
            choices = [
                ("en", "English"),
                ("es", "Spanish"),
                ("fr", "French"),
                ("de", "German"),
                ("it", "Italian"),
                ("zh", "Chinese"),
                ("ja", "Japanese"),
                ("ar", "Arabic"),
                ("ru", "Russian"),
                ("pt", "Portuguese"),
                ("ko", "Korean"),
                ("hi", "Hindi"),
                ("pl", "Polish"),
                ("nl", "Dutch"),
                ("sv", "Swedish"),
                ("el", "Greek"),
                ("he", "Hebrew"),
                ("id", "Indonesian"),
                ("tr", "Turkish"),
                ("uk", "Ukrainian"),
            ]

        # The assignment is now fully type-safe.
        form_class.base_fields["languages"].choices = choices

        return form_class

    def save_model(self, request, obj, form, change):
        """Custom save logic for Item model."""
        # The custom form `ItemAdminForm` handles the cleaning and saving of
        # the 'languages' field to ensure it's a list for the JSONField.
        # The explicit conversion logic from the original `save_model` is now redundant.
        super().save_model(request, obj, form, change)

    # Actions
    @admin.action(description="Export selected items to CSV")
    def export_to_csv(self, request, queryset):
        """Export selected items to CSV."""
        # This would integrate with your export system
        count = queryset.count()
        self.message_user(request, f"Exported {count} items to CSV.")

    actions = ["export_to_csv"]


@admin.register(Song)
class SongAdmin(ItemAdmin):
    """Admin configuration for the Song model."""

    # Override list_display to exclude item_type (it's always SONG) and add song-specific fields
    list_display = (
        "title",
        "identifier",
        "display_date",
        "display_duration",
        "admin_display_artists",
        "admin_display_composers",
        "admin_display_lyricists",
        "admin_display_languages",
        "updated_at",
    )

    list_filter = (
        "year",
        "duration_seconds",
        "bpm",
        "key_signature",
        "track_number",
        "disc_number",
        "created_at",
        "updated_at",
        "collections",
        "subjects",
    )

    # Add song-specific search fields
    search_fields = list(ItemAdmin.search_fields) + [
        "key_signature",
        "roles__person__authorized_name",
    ]

    # Extend base fieldsets with song-specific fields
    fieldsets = (
        [
            ItemAdmin.base_fieldsets[0],  # Core info
            (
                "Song Details",
                {
                    "fields": (
                        ("duration_seconds", "bpm"),
                        "key_signature",
                        ("track_number", "disc_number"),
                    ),
                    "classes": ("wide",),
                },
            ),
        ]
        + ItemAdmin.base_fieldsets[1:]
    )  # Date, relationships, notes, timestamps

    # Custom admin display methods for song-specific fields
    admin_display_artists = AdminDisplayHelperMixin.make_admin_display_method("display_artists", "Artists")
    admin_display_composers = AdminDisplayHelperMixin.make_admin_display_method("display_composers", "Composers")
    admin_display_lyricists = AdminDisplayHelperMixin.make_admin_display_method("display_lyricists", "Lyricists")

    @admin.display(description="Duration", ordering="duration_seconds")
    def display_duration(self, obj):
        """Display formatted duration."""
        return obj.display_duration or "—"

    @admin.display(description="Track Info")
    def display_track_info(self, obj):
        """Display track and disc information."""
        parts = []
        if obj.disc_number and obj.disc_number > 1:
            parts.append(f"D{obj.disc_number}")
        if obj.track_number:
            parts.append(f"T{obj.track_number}")
        return "/".join(parts) if parts else "—"

    def get_queryset(self, request):
        """Optimize queryset for song-specific relationships."""
        return (
            super()
            .get_queryset(request)
            .prefetch_related(
                "roles__person",
            )
        )


@admin.register(Book)
class BookAdmin(ItemAdmin):
    """Admin configuration for the Book model."""

    # Override list_display to exclude item_type and add book-specific fields
    list_display = (
        "title",
        "identifier",
        "display_date",
        "admin_display_authors",
        "admin_display_editors",
        "publisher",
        "display_isbn",
        "page_count",
        "admin_display_languages",
        "updated_at",
    )

    list_filter = (
        "year",
        "publisher",
        "format",
        "page_count",
        "created_at",
        "updated_at",
        "collections",
        "subjects",
    )

    # Add book-specific search fields
    search_fields = list(ItemAdmin.search_fields) + [
        "isbn_10",
        "isbn_13",
        "publisher",
        "edition",
        "format",
    ]

    # Extend base fieldsets with book-specific fields
    fieldsets = (
        [
            ItemAdmin.base_fieldsets[0],  # Core info
            (
                "Book Details",
                {
                    "fields": (
                        ("isbn_10", "isbn_13"),
                        ("publisher", "edition"),
                        ("page_count", "format"),
                    ),
                    "classes": ("wide",),
                },
            ),
        ]
        + ItemAdmin.base_fieldsets[1:]
    )  # Date, relationships, notes, timestamps

    # Custom admin display methods for book-specific fields
    admin_display_authors = AdminDisplayHelperMixin.make_admin_display_method("display_authors", "Authors")
    admin_display_editors = AdminDisplayHelperMixin.make_admin_display_method("display_editors", "Editors")

    @admin.display(description="ISBN")
    def display_isbn(self, obj):
        """Display formatted ISBN."""
        return obj.display_isbn or "—"

    @admin.display(description="Has ISBN", boolean=True)
    def admin_display_has_isbn(self, obj):
        """Display whether the book has an ISBN."""
        return obj.has_isbn

    @admin.display(description="Publication Info")
    def display_publication_info(self, obj):
        """Display combined publication information."""
        parts = []
        if obj.publisher:
            parts.append(obj.publisher)
        if obj.edition:
            parts.append(f"({obj.edition})")
        if obj.format:
            parts.append(f"[{obj.format}]")
        return " ".join(parts) if parts else "—"

    def save_model(self, request, obj, form, change):
        """Custom save logic for Book model."""
        # Clean ISBN fields
        if obj.isbn_10:
            obj.isbn_10 = obj.isbn_10.replace("-", "").replace(" ", "")
        if obj.isbn_13:
            obj.isbn_13 = obj.isbn_13.replace("-", "").replace(" ", "")

        super().save_model(request, obj, form, change)

    # Actions
    @admin.action(description="Look up ISBN information")
    def lookup_isbn_info(self, request, queryset):
        """Look up ISBN information for selected books."""
        # This would integrate with ISBN lookup services
        books_with_isbn = queryset.exclude(isbn_10="", isbn_13="")
        count = books_with_isbn.count()

        if count == 0:
            self.message_user(request, "No books with ISBN found in selection.", level="WARNING")
        else:
            self.message_user(request, f"Queued {count} books for ISBN lookup.")

    actions = ["export_to_csv", "lookup_isbn_info"]
