from django.contrib import admin
from .models import Item, File, StorageLocation, FileStorage, Song

# Inline for File inside Item
class FileInline(admin.TabularInline):
    model = File
    extra = 1
    show_change_link = True  # adds link to file detail page

class FileStorageInline(admin.TabularInline):
    model = FileStorage
    extra = 1
    show_change_link = True  # adds link to file detail page

@admin.register(Item)
class ItemAdmin(admin.ModelAdmin):
    inlines = [FileInline]
    list_display = ('title', 'id', 'created_at', 'updated_at')
    search_fields = ('title',)

@admin.register(File)
class FileAdmin(admin.ModelAdmin):
    list_display = ('name', 'item', 'size', 'mime_type', 'created_at')
    search_fields = ('name', 'item__title')
    autocomplete_fields = ['item']
    inlines = [FileStorageInline]

@admin.register(StorageLocation)
class StorageLocationAdmin(admin.ModelAdmin):
    list_display = ('name', 'backend', 'active', 'created_at')
    search_fields = ('name', 'backend')

@admin.register(FileStorage)
class FileStorageAdmin(admin.ModelAdmin):
    list_display = ('file', 'storage_location', 'stored_path', 'is_primary', 'created_at')
    search_fields = ('file__name', 'storage_location__name')
    list_filter = ('is_primary', 'storage_location')
    autocomplete_fields = ['file']

@admin.register(Song)
class SongAdmin(admin.ModelAdmin):
    # Song inherits from Item, so re-use the Item inline setup
    inlines = [FileInline]
    list_display = ('title', 'id', 'created_at', 'updated_at')
    search_fields = ('title',)
