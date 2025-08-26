from django.contrib import admin
from simple_history.admin import SimpleHistoryAdmin
from django.db import models  # Required for formfield_for_dbfield to check JSONField type
from django_select2.forms import Select2Widget, Select2MultipleWidget  # Import Select2 widgets
from django import forms  # Required for custom form field

from .models import (
    Book,
    Collection,
    File,
    FileBlock,
    FileReplica,
    Item,
    Person,
    Song,
    Subject,
    Role,
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
        (None, {"fields": (("name", "path"), "size", "mime_type")}),
        ("Hashes", {"fields": (("md5", "sha1"), "sha256")}),
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


@admin.register(Role)
class RoleAdmin(SimpleHistoryAdmin):
    """Admin configuration for the Role model."""

    search_fields = ("name",)  # Enable search for the 'name' field in the list view
    list_display = ("name", "person", "item", "created_at", "updated_at")
    list_filter = ("name",)
    readonly_fields = ("created_at", "updated_at")
    # 'name' is a CharField with choices, not a ForeignKey, so it cannot be in autocomplete_fields.
    autocomplete_fields = ("person", "item")

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """
        Overrides the default formfield for ChoiceFields to use Select2Widget for the 'name' field.
        This makes the role name dropdown searchable.
        """
        if db_field.name == "name":
            kwargs["widget"] = Select2Widget
        return super().formfield_for_choice_field(db_field, request, **kwargs)


class RoleInline(admin.TabularInline):
    """Inline for managing Person roles for an Item."""

    model = Role
    fk_name = "item"  # Explicitly define the foreign key linking Role to Item
    extra = 1
    # 'name' is a CharField with choices, not a ForeignKey, so it cannot be in autocomplete_fields.
    autocomplete_fields = ("person",)
    fields = ("person", "name", "created_at", "updated_at")
    readonly_fields = ("created_at", "updated_at")

    def formfield_for_choice_field(self, db_field, request, **kwargs):
        """
        Overrides the default formfield for ChoiceFields to use Select2Widget for the 'name' field
        within the inline form.
        """
        if db_field.name == "name":
            kwargs["widget"] = Select2Widget
        return super().formfield_for_choice_field(db_field, request, **kwargs)


# Helper to create admin_display_ methods dynamically
def _make_admin_display_method(role_property_name, short_description):
    """
    Creates an admin display method for a given role property on an Item subclass.
    This reduces boilerplate for admin_display_X methods.
    """

    def _admin_display(self, obj):
        return getattr(obj, role_property_name)

    _admin_display.short_description = short_description
    return _admin_display


@admin.register(Item)
class ItemAdmin(admin.ModelAdmin):
    """Base admin configuration for models inheriting from Item."""

    inlines = (RoleInline,)

    # Define common list_display elements that subclasses can extend.
    # This explicitly excludes 'item_type' for subclasses which filter it out.
    _base_list_display_elements = (
        "title",
        "identifier",
        "date",
        "admin_display_languages",
        "admin_display_creators",
        "admin_display_contributors",
        "created_at",
        "updated_at",
    )
    # ItemAdmin's own list_display (includes item_type)
    list_display = (
        "title",
        "identifier",
        "item_type",
        "date",
        "admin_display_languages",
        "admin_display_creators",
        "admin_display_contributors",
        "created_at",
        "updated_at",
    )
    list_filter = ("item_type", "date")
    search_fields = ("title", "identifier", "description", "people__authorized_name")
    readonly_fields = ("created_at", "updated_at", "date")
    ordering = ("-date", "identifier")
    prepopulated_fields = {"identifier": ("title",)}

    # Define common fieldsets structure
    _base_fieldsets = [
        (None, {"fields": (("title", "identifier"), "description", "languages")}),
        (
            "Date Information",
            {"fields": (("year", "month", "day"), "date")},
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    ]
    fieldsets = _base_fieldsets
    admin_display_creators = _make_admin_display_method("display_creators", "Creators")
    admin_display_contributors = _make_admin_display_method("display_contributors", "Contributors")

    @admin.display(description="Languages", ordering="languages")
    def admin_display_languages(self, obj):
        """Formats the languages list for display in the admin list view."""
        if obj.languages:
            return ", ".join(obj.languages)
        return "—"

    class LanguagesSelect2MultipleFormField(forms.MultipleChoiceField):
        """
        A custom form field for JSONField that stores a list of strings.
        It uses a Select2MultipleWidget to allow for a user-friendly multi-select
        interface with the ability to add new tags.
        """

        widget = Select2MultipleWidget(
            attrs={
                "data-tags": "true",  # Allows users to type new options
                "data-placeholder": "Select or type languages",
                "data-minimum-input-length": 0,  # Shows options immediately on focus
            }
        )

        def __init__(self, *args, **kwargs):
            # Get the initial value, which is the list of languages from the DB.
            initial = kwargs.get("initial", [])

            # Fetch all distinct languages already saved in the database to use as choices.
            # This ensures that existing tags are valid options for the MultipleChoiceField
            # and provides suggestions for the Select2 widget.
            # We exclude null values to avoid issues with JSONField lookups.
            all_db_languages_qs = Item.objects.exclude(languages__isnull=True).values_list("languages", flat=True)

            # Aggregate all distinct languages from the database
            all_db_languages = set()
            for lang_list in all_db_languages_qs:
                if isinstance(lang_list, list):  # Ensure it's a list from JSONField
                    all_db_languages.update(lang_list)

            # Combine initial values with all distinct database languages for choices.
            # This ensures that any language already saved for the current item is
            # considered a valid choice, and also provides a comprehensive list
            # of previously used languages for suggestions in the Select2 dropdown.
            # Convert to a sorted list of 2-tuples (value, label).
            combined_choices = sorted(list(set(initial).union(all_db_languages)))
            kwargs["choices"] = [(lang, lang) for lang in combined_choices]

            super().__init__(*args, **kwargs)

        def clean(self, value):
            """
            Cleans the input value. The `Select2MultipleWidget` with `data-tags`
            submits a list of strings. This method ensures the value is a list
            of strings, which is the format expected by the JSONField.
            """
            if not value:
                return []
            return [str(item) for item in value]

    def formfield_for_dbfield(self, db_field, request, **kwargs):
        """
        Overrides the default formfield for the 'languages' JSONField to use
        the custom LanguagesSelect2MultipleFormField.
        """
        if db_field.name == "languages" and isinstance(db_field, models.JSONField):
            # Use the custom form field defined above
            return self.LanguagesSelect2MultipleFormField(required=not db_field.blank, **kwargs)
        return super().formfield_for_dbfield(db_field, request, **kwargs)


@admin.register(Song)
class SongAdmin(SimpleHistoryAdmin, ItemAdmin):
    """Admin configuration for the Song model."""

    # Use the base list_display elements from ItemAdmin and add specific ones
    list_display = ItemAdmin._base_list_display_elements + (
        "admin_display_artists",
        "admin_display_composers",
        "admin_display_lyricists",
    )
    list_filter = ("date",)  # Do not show item_type for SongAdmin

    # Dynamically create the admin_display methods
    admin_display_artists = _make_admin_display_method("display_artists", "Artists")
    admin_display_composers = _make_admin_display_method("display_composers", "Composers")
    admin_display_lyricists = _make_admin_display_method("display_lyricists", "Lyricists")


@admin.register(Book)
class BookAdmin(SimpleHistoryAdmin, ItemAdmin):
    """Admin configuration for the Book model."""

    # Use the base list_display elements from ItemAdmin and add specific ones
    list_display = ItemAdmin._base_list_display_elements + ("admin_display_authors",)
    list_filter = ("date",)  # Do not show item_type for BookAdmin

    # Extend ItemAdmin's fieldsets
    fieldsets = (
        ItemAdmin._base_fieldsets[:1]
        + [  # Convert the tuple to a list for concatenation
            ("Book Details", {"fields": ("subtitle", ("isbn_10", "isbn_13"))}),
        ]
        + ItemAdmin._base_fieldsets[1:]
    )

    # Dynamically create the admin_display method
    admin_display_authors = _make_admin_display_method("display_authors", "Authors")
