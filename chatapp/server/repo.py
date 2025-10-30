import json, os
from typing import Dict, Optional, Iterable, Set
from .models import User, Message, Group
from ..utils.logger import setup_logger

logger = setup_logger('chatapp.repo')

class UsersRepo:
    """Repository for managing user data in JSONL format."""
    
    def __init__(self, path: str):
        """Initialize users repository.
        
        Args:
            path (str): Path to JSONL file storing user data
            
        Side Effects:
            - Creates directory structure if not exists
            - Loads existing users from file
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.users_by_id: Dict[str, User] = {}
        self._load()

    def _load(self):
        """Load users from JSONL file into memory.
        
        Side Effects:
            - Populates users_by_id dictionary
        """
        if not os.path.exists(self.path): return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                rec = json.loads(line)
                self.users_by_id[rec["id"]] = User(**rec)

    def append_user(self, user: User):
        """Add new user to repository.
        
        Args:
            user (User): User object to store
            
        Side Effects:
            - Appends user to JSONL file
            - Updates in-memory dictionary
            - Logs user registration
        """
        line = json.dumps({"id": user.id, "display_name": user.display_name}, ensure_ascii=False)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n"); f.flush(); os.fsync(f.fileno())
        self.users_by_id[user.id] = user
        logger.info(f"New user registered: {user.display_name} (ID: {user.id})")

    def get(self, user_id: str) -> Optional[User]:
        """Get user by ID.
        
        Args:
            user_id (str): User's unique identifier
            
        Returns:
            Optional[User]: User object if found, None otherwise
        """
        return self.users_by_id.get(user_id)

    def all(self) -> Iterable[User]:
        """Get all users.
        
        Returns:
            Iterable[User]: Iterator of all user objects
        """
        return self.users_by_id.values()
        
    def find_by_display_name(self, display_name: str) -> Optional[User]:
        """Find user by display name (case sensitive).
        
        Args:
            display_name (str): User's display name to search for
            
        Returns:
            Optional[User]: User object if found, None otherwise
        """
        for user in self.users_by_id.values():
            if user.display_name == display_name:
                return user
        return None


class MessagesRepo:
    """Repository for managing message data with delivery tracking."""
    
    def __init__(self, path: str):
        """Initialize messages repository.
        
        Args:
            path (str): Path to JSONL file storing message data
            
        Side Effects:
            - Creates directory structure if not exists
            - Loads existing messages from file
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self._messages = []
        self._load()

    def _load(self):
        """Load messages from JSONL file with backward compatibility support.
        
        Handles:
            - Old format with boolean 'delivered' field
            - New format with 'delivered_to' set
            - Missing 'is_group_message' field
            
        Side Effects:
            - Populates _messages list with normalized Message objects
        """
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                # Handle both old 'delivered' and new 'delivered_to' fields
                if "delivered" in rec:
                    delivered_to = set() if not rec["delivered"] else {rec["to_user_id"]}
                    del rec["delivered"]
                    rec["delivered_to"] = delivered_to
                elif "delivered_to" in rec:
                    rec["delivered_to"] = set(rec["delivered_to"])
                else:
                    rec["delivered_to"] = set()
                    
                # Add is_group_message field if not present
                if "is_group_message" not in rec:
                    rec["is_group_message"] = rec["to_user_id"].startswith("#")
                    
                self._messages.append(Message(**rec))

    def append(self, m: Message):
        """Append new message to repository.
        
        Args:
            m (Message): Message object to store
            
        Side Effects:
            - Appends message to JSONL file
            - Updates in-memory message list
            - Logs message storage with type (DM/group)
        """
        rec = {
            "message_id": m.message_id,
            "from_user_id": m.from_user_id,
            "to_user_id": m.to_user_id,
            "text": m.text,
            "sent_ts": m.sent_ts,
            "is_group_message": m.is_group_message,
            "delivered_to": list(m.delivered_to),  # Convert set to list for JSON
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush(); os.fsync(f.fileno())
        self._messages.append(m)
        if m.is_group_message:
            logger.info(f"New group message saved: {m.message_id} from {m.from_user_id} to group {m.to_user_id}")
        else:
            logger.info(f"New direct message saved: {m.message_id} from {m.from_user_id} to {m.to_user_id}")

    def get_undelivered_messages(self, user_id: str, groups_repo=None) -> list[Message]:
        """Get all undelivered messages for a user.
        
        Retrieves both direct messages and group messages, checking group
        membership when groups_repo is provided.
        
        Args:
            user_id (str): User's ID to get messages for
            groups_repo (GroupsRepo, optional): Repository to verify group membership
            
        Returns:
            list[Message]: List of undelivered messages for the user
            
        Side Effects:
            - Logs count and types of undelivered messages found
        """
        undelivered = []
        for msg in self._messages:
            if msg.is_group_message:
                # For group messages, check if:
                # 1. Message is from a group (#)
                # 2. User hasn't received it yet
                # 3. User is a member of the group (if groups_repo is provided)
                if (msg.to_user_id.startswith('#') and 
                    user_id not in msg.delivered_to and
                    (groups_repo is None or groups_repo.is_member(msg.to_user_id, user_id))):
                    undelivered.append(msg)
            else:
                # For DMs, check if:
                # 1. Message is for this user
                # 2. User hasn't received it yet
                if msg.to_user_id == user_id and user_id not in msg.delivered_to:
                    undelivered.append(msg)
        
        if undelivered:
            logger.info(f"Found {len(undelivered)} undelivered messages for user {user_id}")
            logger.debug(f"Message types - DM: {sum(1 for m in undelivered if not m.is_group_message)}, " +
                        f"Group: {sum(1 for m in undelivered if m.is_group_message)}")
        return undelivered

    def mark_delivered(self, message_id: str, user_id: str):
        """Mark message as delivered to specific user.
        
        Args:
            message_id (str): ID of message to mark
            user_id (str): ID of user who received the message
            
        Side Effects:
            - Updates delivered_to set in memory
            - Rewrites entire messages file
            - Logs delivery status
        """
        # Update in memory
        for msg in self._messages:
            if msg.message_id == message_id:
                msg.delivered_to.add(user_id)
                logger.info(f"Message {message_id} marked as delivered to user {user_id}")
                break

        # Rewrite the entire file
        # This is not efficient for large files but works for this demo
        with open(self.path, "w", encoding="utf-8") as f:
            for msg in self._messages:
                rec = {
                    "message_id": msg.message_id,
                    "from_user_id": msg.from_user_id,
                    "to_user_id": msg.to_user_id,
                    "text": msg.text,
                    "sent_ts": msg.sent_ts,
                    "is_group_message": msg.is_group_message,
                    "delivered_to": list(msg.delivered_to),
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def get_conversation_messages(self, user_id: str, chat_id: str, is_group: bool, limit: int = 50) -> list[Message]:
        """Get message history for a conversation.
        
        Args:
            user_id (str): ID of requesting user
            chat_id (str): ID of other user or group name
            is_group (bool): True if group chat, False if DM
            limit (int, optional): Max messages to return. Defaults to 50
            
        Returns:
            list[Message]: List of messages, newest last, up to limit
        """
        messages = []
        
        if is_group:
            # Get all messages for this group
            for msg in self._messages:
                if msg.is_group_message and msg.to_user_id == chat_id:
                    messages.append(msg)
        else:
            # Get DM messages between user_id and chat_id
            for msg in self._messages:
                if not msg.is_group_message:
                    if (msg.from_user_id == user_id and msg.to_user_id == chat_id) or \
                       (msg.from_user_id == chat_id and msg.to_user_id == user_id):
                        messages.append(msg)
        
        # Sort by timestamp and limit
        messages.sort(key=lambda m: m.sent_ts)
        return messages[-limit:] if limit > 0 else messages

class GroupsRepo:
    """Repository for managing chat groups and their memberships."""
    
    def __init__(self, path: str):
        """Initialize groups repository.
        
        Args:
            path (str): Path to JSONL file storing group data
            
        Side Effects:
            - Creates directory structure if not exists
            - Loads existing groups from file
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.groups_by_name: Dict[str, Group] = {}
        self._load()

    def _load(self):
        """Load groups from JSONL file into memory.
        
        Side Effects:
            - Populates groups_by_name dictionary
            - Converts member_ids from list to set
        """
        if not os.path.exists(self.path): 
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): 
                    continue
                rec = json.loads(line)
                # Convert member_ids from list to set
                rec['member_ids'] = set(rec['member_ids'])
                self.groups_by_name[rec["name"]] = Group(**rec)

    def _save_group(self, group: Group):
        """Save single group to JSONL file.
        
        Args:
            group (Group): Group object to save
            
        Side Effects:
            - Appends group data to JSONL file
            - Converts member_ids set to list for JSON
        """
        # Convert set to list for JSON serialization
        rec = {
            "name": group.name,
            "creator_id": group.creator_id,
            "member_ids": list(group.member_ids),
            "created_ts": group.created_ts
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def create_group(self, name: str, creator_id: str, created_ts: int) -> Group:
        """Create a new chat group.
        
        Args:
            name (str): Unique name for the group
            creator_id (str): User ID of group creator
            created_ts (int): Timestamp of group creation
            
        Returns:
            Group: Newly created group object
            
        Raises:
            ValueError: If group with given name already exists
            
        Side Effects:
            - Creates new group in memory
            - Saves group to JSONL file
            - Logs group creation
        """
        if name in self.groups_by_name:
            logger.warning(f"Attempt to create existing group: {name}")
            raise ValueError(f"Group {name} already exists")
        
        group = Group(
            name=name,
            creator_id=creator_id,
            member_ids={creator_id},  # Creator is first member
            created_ts=created_ts
        )
        self.groups_by_name[name] = group
        self._save_group(group)
        logger.info(f"New group created: {name} by user {creator_id}")
        return group

    def add_member(self, group_name: str, user_id: str) -> bool:
        """Add a member to an existing group.
        
        Args:
            group_name (str): Name of group to add member to
            user_id (str): ID of user to add
            
        Returns:
            bool: True if user was added, False if already a member
            
        Raises:
            ValueError: If group does not exist
            
        Side Effects:
            - Updates group membership in memory
            - Rewrites groups file with updated membership
            - Logs member addition
        """
        group = self.groups_by_name.get(group_name)
        if not group:
            logger.warning(f"Attempt to join non-existent group: {group_name}")
            raise ValueError(f"Group {group_name} does not exist")
        
        if user_id in group.member_ids:
            logger.debug(f"User {user_id} already in group {group_name}")
            return False

        # Update in memory
        group.member_ids.add(user_id)
        logger.info(f"Added user {user_id} to group {group_name}")
        
        # Rewrite the file with updated group
        # This is not efficient for large files but works for this demo
        with open(self.path, "w", encoding="utf-8") as f:
            for g in self.groups_by_name.values():
                rec = {
                    "name": g.name,
                    "creator_id": g.creator_id,
                    "member_ids": list(g.member_ids),
                    "created_ts": g.created_ts
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        logger.debug(f"Updated group {group_name} in storage")
        
        return True

    def get_group(self, name: str) -> Optional[Group]:
        """Get a group by its name.
        
        Args:
            name (str): Name of group to retrieve
            
        Returns:
            Optional[Group]: Group object if found, None otherwise
        """
        return self.groups_by_name.get(name)

    def get_user_groups(self, user_id: str) -> list[Group]:
        """Get all groups that a user is a member of.
        
        Args:
            user_id (str): ID of user to get groups for
            
        Returns:
            list[Group]: List of groups where user is a member
        """
        return [
            group for group in self.groups_by_name.values()
            if user_id in group.member_ids
        ]

    def exists(self, name: str) -> bool:
        """Check if a group exists by name.
        
        Args:
            name (str): Name of group to check
            
        Returns:
            bool: True if group exists, False otherwise
        """
        return name in self.groups_by_name

    def is_member(self, group_name: str, user_id: str) -> bool:
        """Check if a user is a member of a specific group.
        
        Args:
            group_name (str): Name of group to check
            user_id (str): ID of user to check membership for
            
        Returns:
            bool: True if user is a member, False if not or if group doesn't exist
        """
        group = self.groups_by_name.get(group_name)
        return group is not None and user_id in group.member_ids
