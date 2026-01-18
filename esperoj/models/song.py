class Song(Item):
    """A musical composition and/or recording.

    This model merges the concepts of a musical work and a recording into a single
    entity. It represents both the abstract song (music and lyrics) and its
    recorded performance.

    Attributes:
        album: The album or release this song belongs to.
    """

    album = models.CharField(
        max_length=255,
        blank=True,
        default="",
        help_text="The album or release this song belongs to.",
    )

    class Meta:
        db_table = "song"
        verbose_name = "Song"
        verbose_name_plural = "Songs"
        ordering = ["title"]
        indexes = [
            Index(fields=["album"]),
        ]

    def save(self, *args, **kwargs) -> None:
        """Sets the type before saving.

        Args:
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        self.type = ItemType.SONG
        super().save(*args, **kwargs)

    @property
    def creators(self) -> "QuerySet[Agent]":
        """For a Song, primary creators are Artists.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.ARTIST)

    @property
    def composers(self) -> "QuerySet[Agent]":
        """Returns all agents credited as composers for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.COMPOSER)

    @property
    def display_composers(self) -> str:
        """Returns a semicolon-separated string of composers.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.composers)

    @property
    def lyricists(self) -> "QuerySet[Agent]":
        """Returns all agents credited as lyricists for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.LYRICIST)

    @property
    def display_lyricists(self) -> str:
        """Returns a semicolon-separated string of lyricists.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.lyricists)

    @property
    def artists(self) -> "QuerySet[Agent]":
        """Returns all performing artists for this song.

        Returns:
            A queryset of Agent instances.
        """
        from .relationships import ItemRoleName

        return self.get_agents_by_role(ItemRoleName.ARTIST)

    @property
    def display_artists(self) -> str:
        """Returns a semicolon-separated string of artists.

        Returns:
            A string for display.
        """
        return self._get_agents_display_string(self.artists)
