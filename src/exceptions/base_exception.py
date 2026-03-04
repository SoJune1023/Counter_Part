from typing import Dict, Optional, Union

class BaseException(Exception):
    message: str
    http_status: int
    details: Optional[Union[str, Dict[str, str]]]

    def __init__(
        self,
        message: str,
        http_status: int,
        *,
        details: Union[str, Dict[str, str]] = None
    ) -> None:
        super().__init__(message)

        self.message = message
        self.http_status = http_status
        self.details = details
    
    def to_dict(
        self
    ) -> Dict[str, Union[str, Dict[str, str]]]:
        """Get information of exception."""
        payload: Dict[str, Union[str, Dict[str, str]]] = {
            "error": self.message,
            "http_status": self.http_status
        }
        if self.details is not None:
            payload['details'] = self.details
        return payload
