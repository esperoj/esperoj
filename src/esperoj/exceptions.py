"""Module contains exceptions."""


class InvalidRecordError(Exception):
    """Exception raised for invalid records.

    This exception should be raised when a record in the database or storage is invalid or does not meet the required criteria.
    """


class RecordNotFoundError(Exception):
    """Exception raised for missing records.

    This exception should be raised when a record that is expected to be in the database or storage cannot be found.
    """
