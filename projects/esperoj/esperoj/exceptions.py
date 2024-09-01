"""Module contains exceptions."""

class ShareUploadError(Exception):
    def __init__(self, host, status_code=None, server_message=None, message="Failed to upload"):
        self.host = host
        self.status_code = status_code
        self.server_message = server_message
        super().__init__(f"{message} to {host}. Status: {status_code}. Server message: {server_message}.")

class ReplicationError(Exception):
    """Raised when the replication of one or more files fails."""
    def __init__(self, host, status_code=None, server_message=None, message="Failed to upload"):
        self.host = host
        self.status_code = status_code
        self.server_message = server_message
        super().__init__(f"{message} to {host}. Status: {status_code}. Server message: {server_message}.")

class VerificationError(Exception):
    """Raised when the verification of one or more files fails."""
    def __init__(self, host, status_code=None, server_message=None, message="Failed to upload"):
        self.host = host
        self.status_code = status_code
        self.server_message = server_message
        super().__init__(f"{message} to {host}. Status: {status_code}. Server message: {server_message}.")
