from django.db import models
from django.utils import timezone
from simple_history.models import HistoricalRecords


class Song(models.Model):
    title = models.CharField(max_length=255)
    history = HistoricalRecords()

    def __str__(self):
        return f"{self.title}"
