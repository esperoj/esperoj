"""
This type stub file was generated by pyright.
"""

class EmailSender:
    def __init__(self, detail) -> None: ...
    def send_msg(self, msg, **kwargs):  # -> None:
        ...

class WechatSender:
    def __init__(self, detail) -> None: ...
    def send_msg(self, msg):  # -> None:
        ...

def get_sender_by_account(account):  # -> EmailSender | WechatSender | None:
    ...
