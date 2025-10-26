from dataclasses import dataclass
from typing import Optional, Set

@dataclass
class Group:
    name: str               # Group name (starts with #)
    creator_id: str         # User ID of creator
    member_ids: Set[str]    # Set of member user IDs
    created_ts: int         # Unix timestamp in ms

@dataclass
class User:
    id: str
    display_name: str

@dataclass
class Message:
    message_id: str
    from_user_id: str
    to_user_id: str          # user-to-user DM or group name for group messages
    text: str
    sent_ts: int             # unix ms
    is_group_message: bool = False  # True if this is a group message
    delivered_to: Set[str] = None   # Set of user IDs who received the message
    
    def __post_init__(self):
        if self.delivered_to is None:
            self.delivered_to = set()
