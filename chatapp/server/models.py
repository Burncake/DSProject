from dataclasses import dataclass
from typing import Optional

@dataclass
class User:
    id: str
    display_name: str

@dataclass
class Message:
    message_id: str
    from_user_id: str
    to_user_id: str          # user-to-user DM (groups later)
    text: str
    sent_ts: int             # unix ms
    delivered: bool = False  # set True if recipient was online
