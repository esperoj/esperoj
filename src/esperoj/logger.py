import logging


class EsperojLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)
        # Add any additional configurations for the logger here

    # Implement any additional methods or configurations as needed
