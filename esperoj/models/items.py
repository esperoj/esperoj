import datetime

from django.core.exceptions import ValidationError
from django.db.models.signals import pre_save
from django.dispatch import receiver
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import CheckConstraint, Index, Q

from .base import BaseModel
from .entities import Person


class ItemType(models.TextChoices):
    """All objects a person can contribute to are a type of Item."""

    MUSICAL_WORK = "MUSICAL_WORK", "Musical Work"
    RECORDING = "RECORDING", "Recording"
    BOOK = "BOOK", "Book"


class ContributionRole(models.TextChoices):
    """A single, unified list of all possible contribution roles."""

    COMPOSER = "COMPOSER", "Composer"
    LYRICIST = "LYRICIST", "Lyricist"
    AUTHOR = "AUTHOR", "Author"
    EDITOR = "EDITOR", "Editor"
    TRANSLATOR = "TRANSLATOR", "Translator"
    ARTIST = "ARTIST", "Artist"  # The performer
    PRODUCER = "PRODUCER", "Producer"
    ENGINEER = "ENGINEER", "Engineer"


class Contribution(BaseModel):
    """
    The single, definitive through model connecting a Person to an Item.
    """

    person = models.ForeignKey(Person, on_delete=models.CASCADE, related_name="contributions")
    item = models.ForeignKey("Item", on_delete=models.CASCADE, related_name="contributions")
    role = models.CharField(max_length=20, choices=ContributionRole.choices)

    class Meta:
        unique_together = [["person", "item", "role"]]
        ordering = ["role"]
        verbose_name = "Contribution"
        verbose_name_plural = "Contributions"


class Item(BaseModel):
    """
    The single, concrete base model for ALL cataloged objects.
    """

    identifier = models.SlugField(max_length=255, unique=True)
    item_type = models.CharField(max_length=20, choices=ItemType.choices, editable=False)
    people = models.ManyToManyField(Person, through=Contribution, related_name="items", blank=True)
    # --- Date Fields ---
    year = models.IntegerField(
        null=True, blank=True, help_text="Use a negative number for BC years (e.g., -44 for 44 BC)."
    )
    month = models.PositiveSmallIntegerField(
        null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(12)]
    )
    day = models.PositiveSmallIntegerField(
        null=True, blank=True, validators=[MinValueValidator(1), MaxValueValidator(31)]
    )
    date = models.DateField(null=True, blank=True, editable=False)

    class Meta:
        ordering = ["-date"]
        indexes = [
            Index(fields=["-date"]),
            Index(fields=["identifier"]),
            Index(fields=["item_type"]),
        ]
        constraints = [
            CheckConstraint(condition=Q(month__isnull=True) | Q(year__isnull=False), name="month_requires_year"),  # type: ignore
            CheckConstraint(condition=Q(day__isnull=True) | Q(month__isnull=False), name="day_requires_month"),  # type: ignore
        ]

    def __str__(self):
        return self.identifier

    def clean(self):
        """Validation logic that the database cannot handle, like invalid dates."""
        super().clean()
        if self.year and self.month and self.day:
            try:
                # This validates that the date is real (e.g., not February 30th)
                datetime.date(self.year, self.month, self.day)
            except ValueError as e:
                raise ValidationError({"day": f"Invalid date: {e}"})

    def get_people_by_role(self, role: ContributionRole) -> models.QuerySet[Person]:
        """Helper to get people for a specific role."""
        return self.people.filter(contributions__role=role, contributions__item=self)

    @property
    def creators(self) -> models.QuerySet[Person]:
        """Subclasses MUST override this to define their creator roles."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement the 'creators' property.")

    @property
    def contributors(self) -> models.QuerySet[Person]:
        """Contributors are all people who are not creators."""
        creator_pks = self.creators.values_list("pk", flat=True)
        return self.people.exclude(pk__in=creator_pks)


@receiver(pre_save)
def update_item_date(sender, instance, **kwargs):
    """
    Automatically sets the denormalized 'date' field before saving an Item.
    This signal runs for Item and all its subclasses.
    """
    if isinstance(instance, Item):
        if instance.year and instance.year > 0:
            # Use 1 for month/day if they are not provided
            month = instance.month or 1
            day = instance.day or 1
            try:
                instance.date = datetime.date(instance.year, month, day)
            except ValueError:
                # Handles cases like month=2, day=30
                instance.date = None
        else:
            # Handles BC years or years where no date should be set
            instance.date = None


class MusicalWork(Item):
    """An abstract musical composition, modeled as an Item."""

    class Meta:
        verbose_name = "Musical Work"
        verbose_name_plural = "Musical Works"

    def save(self, *args, **kwargs):
        self.item_type = ItemType.MUSICAL_WORK
        super().save(*args, **kwargs)

    @property
    def creators(self) -> models.QuerySet[Person]:
        """For a MusicalWork, creators are Composers and Lyricists."""
        return self.people.filter(
            contributions__item=self, contributions__role__in=[ContributionRole.COMPOSER, ContributionRole.LYRICIST]
        )

    @property
    def composers(self) -> models.QuerySet[Person]:
        return self.get_people_by_role(ContributionRole.COMPOSER)

    @property
    def lyricists(self) -> models.QuerySet[Person]:
        return self.get_people_by_role(ContributionRole.LYRICIST)


class Recording(Item):
    """A specific recorded performance, modeled as an Item."""

    work = models.ForeignKey(
        Item, on_delete=models.PROTECT, related_name="recordings", limit_choices_to={"item_type": ItemType.MUSICAL_WORK}
    )

    class Meta:
        verbose_name = "Recording"
        verbose_name_plural = "Recordings"

    def save(self, *args, **kwargs):
        self.item_type = ItemType.RECORDING
        super().save(*args, **kwargs)

    def clean(self):
        super().clean()
        if self.work and self.work.item_type != ItemType.MUSICAL_WORK:
            raise ValidationError("A recording can only be linked to a Musical Work.")

    @property
    def creators(self) -> models.QuerySet[Person]:
        """For a Recording, the creators are the performing Artists."""
        return self.get_people_by_role(ContributionRole.ARTIST)

    @property
    def artists(self):
        return self.creators


class Book(Item):
    """A book, modeled as an Item."""

    isbn_10 = models.CharField(max_length=10, blank=True)
    isbn_13 = models.CharField(max_length=13, blank=True)

    class Meta:
        verbose_name = "Book"
        verbose_name_plural = "Books"

    def save(self, *args, **kwargs):
        self.item_type = ItemType.BOOK
        super().save(*args, **kwargs)

    @property
    def creators(self) -> models.QuerySet[Person]:
        """For a Book, the creators are the Authors."""
        return self.get_people_by_role(ContributionRole.AUTHOR)

    @property
    def authors(self):
        return self.creators
