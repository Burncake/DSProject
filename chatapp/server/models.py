from dataclasses import dataclass
from typing import Optional, Set

@dataclass
class Group:
    """Represents a chat group in the system.
    
    Attributes:
        name (str): Unique group name (starts with #)
        creator_id (str): User ID of the group creator
        member_ids (Set[str]): Set of user IDs who are members of this group
        created_ts (int): Unix timestamp in milliseconds when group was created
    """
    name: str
    creator_id: str
    member_ids: Set[str]
    created_ts: int

@dataclass
class User:
    """Represents a user in the chat system.
    
    Attributes:
        id (str): Unique identifier for the user
        display_name (str): User's chosen display name
    """
    id: str
    display_name: str

@dataclass
class Message:
    """Represents a chat message in the system.
    
    A message can be either a direct message (DM) between users or
    a group message sent to all members of a group.
    
    Attributes:
        message_id (str): Unique identifier for the message
        from_user_id (str): ID of the user who sent the message
        to_user_id (str): For DMs: recipient's user ID; For group messages: group name
        text (str): Content of the message
        sent_ts (int): Unix timestamp in milliseconds when message was sent
        is_group_message (bool): True if sent to a group, False if a direct message
        delivered_to (Set[str]): Set of user IDs who have received the message
    """
    message_id: str
    from_user_id: str
    to_user_id: str
    text: str
    sent_ts: int
    is_group_message: bool = False
    delivered_to: Set[str] = None
    
    def __post_init__(self):
        if self.delivered_to is None:
            self.delivered_to = set()
