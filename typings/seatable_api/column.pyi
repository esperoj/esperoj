"""
This type stub file was generated by pyright.
"""

NULL_LIST = ...

class ColumnValue:
    """
    This is for the computation of the comparison between the input value and cell value from table
    such as >, <, =, >=, <=, !=, which is supposed to fit different column types
    """
    def __init__(self, column_value, column_type=...) -> None: ...
    def equal(self, value):  # -> bool:
        ...
    def unequal(self, value):  # -> bool:
        ...
    def greater_equal_than(self, value): ...
    def greater_than(self, value): ...
    def less_equal_than(self, value): ...
    def less_than(self, value): ...
    def like(self, value):
        """fuzzy search"""
        ...

class StringColumnValue(ColumnValue):
    """
    the return data of string column value is type of string, including column type of
    text, creator, single-select, url, email,....., and support the computation of
    = ,!=, and like(fuzzy search)
    """
    def like(self, value):  # -> bool:
        ...

class NumberDateColumnValue(ColumnValue):
    """
    the returned data of number-date-column is digit number, or datetime obj, including the
    type of number, ctime, date, mtime, support the computation of =, > ,< ,>=, <=, !=
    """
    def greater_equal_than(self, value):  # -> Literal[False]:
        ...
    def greater_than(self, value):  # -> Literal[False]:
        ...
    def less_equal_than(self, value):  # -> Literal[False]:
        ...
    def less_than(self, value):  # -> Literal[False]:
        ...
    def raise_error(self): ...

class ListColumnValue(ColumnValue):
    """
    the returned data of list-column value is a list like data structure, including the
    type of multiple-select, image, collaborator and so on, support the computation of
    =, != which should be decided by in or not in expression
    """
    def equal(self, value):  # -> bool:
        ...
    def unequal(self, value):  # -> bool:
        ...

class BoolColumnValue(ColumnValue):
    """
    the returned data of bool-column value is should be True or False, such as check-box
    type column. If the value from table shows None, treat it as False
    """
    def equal(self, value): ...
    def unequal(self, value): ...

class BaseColumn:
    def parse_input_value(self, value): ...
    def parse_table_value(self, value):  # -> ColumnValue:
        ...

class TextColumn(BaseColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_table_value(self, value):  # -> StringColumnValue:
        ...

class LongTextColumn(TextColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_table_value(self, value):  # -> StringColumnValue:
        ...

class NumberColumn(BaseColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_input_value(self, value):  # -> float | int | Literal['']:
        ...
    def parse_table_value(self, value):  # -> NumberDateColumnValue:
        ...
    def raise_input_error(self, value): ...

class DateColumn(BaseColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_input_value(self, time_str):  # -> datetime | Literal[''] | None:
        ...
    def parse_table_value(self, time_str):  # -> NumberDateColumnValue:
        ...
    def raise_error(self, value): ...

class CTimeColumn(DateColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def get_local_time(self, time_str):  # -> datetime:
        ...
    def parse_table_value(self, time_str):  # -> NumberDateColumnValue:
        ...

class MTimeColumn(CTimeColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_table_value(self, time_str):  # -> NumberDateColumnValue:
        ...

class CheckBoxColumn(BaseColumn):
    def __init__(self) -> None: ...
    def __str__(self) -> str: ...
    def parse_input_value(self, value):  # -> bool | None:
        ...
    def parse_table_value(self, value):  # -> BoolColumnValue:
        ...
    def raise_error(self, value): ...

class MultiSelectColumn(BaseColumn):
    def __init__(self) -> None: ...
    def parse_table_value(self, value):  # -> ListColumnValue:
        ...

COLUMN_MAP = ...

def get_column_by_type(column_type): ...
