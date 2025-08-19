from django import forms
from django.contrib import admin
from django.utils.html import format_html
from simple_history.admin import SimpleHistoryAdmin
from .models import Creator, Subject, Collection, File, FileStorage, Item, Song, Book

LANG_CHOICES = [
    ("en", "English"),
    ("vi", "Vietnamese"),
    ("ja", "Japanese"),
    ("fr", "French"),
    ("es", "Spanish"),
    ("de", "German"),
    ("zh", "Chinese"),
]


class CreatorAdmin(SimpleHistoryAdmin):
    search_fields = ("name",)
    list_display = ("name", "created_at", "updated_at")
    ordering = ("name",)


class SubjectAdmin(SimpleHistoryAdmin):
    search_fields = ("name",)
    list_display = ("name", "created_at", "updated_at")
    ordering = ("name",)


class CollectionAdmin(SimpleHistoryAdmin):
    search_fields = ("name",)
    list_display = ("name", "created_at", "updated_at")
    ordering = ("name",)


class FileStorageInline(admin.TabularInline):
    model = FileStorage
    fk_name = "file"
    extra = 0
    fields = ("storage_name", "path", "is_primary", "sha1", "sha256", "updated_at")
    readonly_fields = ("updated_at",)
    show_change_link = True


class FileAdminForm(forms.ModelForm):
    class Meta:
        model = File
        fields = "__all__"


class FileAdmin(SimpleHistoryAdmin):
    form = FileAdminForm
    inlines = (FileStorageInline,)
    list_display = (
        "name",
        "size",
        "mime_type",
        "linked_song_count",
        "linked_book_count",
        "primary_storage_link",
        "updated_at",
    )
    search_fields = ("name", "sha1", "sha256", "path")
    list_filter = ("mime_type",)
    readonly_fields = ("created_at", "updated_at")
    ordering = ("name", "-updated_at")
    fieldsets = (
        (None, {"fields": ("name", "path", "size", "mime_type")}),
        ("Hashes", {"fields": ("sha1", "sha256")}),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
    autocomplete_fields = ()

    @admin.display(description="Primary Storage")
    def primary_storage_link(self, obj):
        primary = obj.get_primary_storage()
        if not primary:
            return "-"
        url = f"/admin/{primary._meta.app_label}/{primary._meta.model_name}/{primary.pk}/change/"
        return format_html('<a href="{}">{}: {}</a>', url, primary.storage_name, primary.path)

    @admin.display(description="Songs")
    def linked_song_count(self, obj):
        app = obj._meta.app_label
        qurl = f"/admin/{app}/song/?files__id__exact={obj.pk}"
        count = Song.objects.filter(files__id=obj.pk).count()
        return format_html('<a href="{}">Songs: {}</a>', qurl, count)

    @admin.display(description="Books")
    def linked_book_count(self, obj):
        app = obj._meta.app_label
        qurl = f"/admin/{app}/book/?files__id__exact={obj.pk}"
        count = Book.objects.filter(files__id=obj.pk).count()
        return format_html('<a href="{}">Books: {}</a>', qurl, count)


class LanguageWidget(forms.SelectMultiple):
    template_name = "admin/widgets/languages.html"

    def get_context(self, name, value, attrs):
        context = super().get_context(name, value, attrs)
        context["widget"]["choices"] = [
            {"value": choice[0], "label": choice[1]} for choice in self.choices
        ]
        return context


class BaseItemForm(forms.ModelForm):
    languages = forms.MultipleChoiceField(
        choices=LANG_CHOICES, required=False, widget=LanguageWidget
    )

    class Meta:
        model = Item
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        initial = []
        if self.instance and getattr(self.instance, "languages", None):
            initial = [str(x).lower() for x in self.instance.languages]
        self.fields["languages"].initial = initial

    def clean_languages(self):
        data = self.cleaned_data.get("languages", [])
        return [str(x).lower() for x in data]


class BaseItemAdmin(SimpleHistoryAdmin):
    form = BaseItemForm
    list_display = ("title", "date", "www", "languages_display", "created_at", "updated_at")
    search_fields = ("title", "creators__name", "subjects__name", "collections__name", "www")
    list_filter = ("collections", "creators", "subjects", "date")
    readonly_fields = ("created_at", "updated_at", "date")
    ordering = ("-date", "title")
    fieldsets = (
        (None, {"fields": (("title", "languages"), "www")}),
        (
            "Relations",
            {
                "fields": (
                    ("collections", "creators"),
                    ("subjects", "files"),
                )
            },
        ),
        (
            "Details",
            {
                "fields": (
                    ("year", "month", "day"),
                    "date",
                )
            },
        ),
        ("Timestamps", {"fields": ("created_at", "updated_at")}),
    )
    autocomplete_fields = ("collections", "creators", "subjects", "files")

    @admin.display(description="Languages")
    def languages_display(self, obj):
        if not obj.languages:
            return "-"
        return ", ".join([str(x).upper() for x in obj.languages])


class SongAdmin(BaseItemAdmin):
    pass


class BookAdmin(BaseItemAdmin):
    pass


admin.site.register(Creator, CreatorAdmin)
admin.site.register(Subject, SubjectAdmin)
admin.site.register(Collection, CollectionAdmin)
admin.site.register(File, FileAdmin)
admin.site.register(FileStorage, SimpleHistoryAdmin)
admin.site.register(Song, SongAdmin)
admin.site.register(Book, BookAdmin)
