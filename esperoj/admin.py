from django.contrib import admin
from .models import Song
from simple_history.admin import SimpleHistoryAdmin

@admin.register(Song)
class SongAdmin(SimpleHistoryAdmin):
    pass
