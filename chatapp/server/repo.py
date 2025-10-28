import json, os
from typing import Dict, Optional, Iterable, Set
from .models import User, Message, Group

class UsersRepo:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.users_by_id: Dict[str, User] = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.path): return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                rec = json.loads(line)
                self.users_by_id[rec["id"]] = User(**rec)

    def append_user(self, user: User):
        line = json.dumps({"id": user.id, "display_name": user.display_name}, ensure_ascii=False)
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(line + "\n"); f.flush(); os.fsync(f.fileno())
        self.users_by_id[user.id] = user

    def get(self, user_id: str) -> Optional[User]:
        return self.users_by_id.get(user_id)

    def all(self) -> Iterable[User]:
        return self.users_by_id.values()
        
    def find_by_display_name(self, display_name: str) -> Optional[User]:
        """Find a user by their display name (case sensitive)"""
        for user in self.users_by_id.values():
            if user.display_name == display_name:
                return user
        return None


class MessagesRepo:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self._messages = []
        self._load()

    def _load(self):
        """Load all messages from file"""
        if not os.path.exists(self.path):
            return
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                rec = json.loads(line)
                # Handle both old 'delivered' and new 'delivered_to' fields
                if "delivered" in rec:
                    # Convert old boolean delivered to set
                    delivered_to = set() if not rec["delivered"] else {rec["to_user_id"]}
                    del rec["delivered"]
                    rec["delivered_to"] = delivered_to
                elif "delivered_to" in rec:
                    # Convert delivered_to from list to set
                    rec["delivered_to"] = set(rec["delivered_to"])
                else:
                    rec["delivered_to"] = set()
                    
                # Add is_group_message field if not present (for backwards compatibility)
                if "is_group_message" not in rec:
                    rec["is_group_message"] = rec["to_user_id"].startswith("#")
                    
                self._messages.append(Message(**rec))

    def append(self, m: Message):
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

    def get_undelivered_messages(self, user_id: str) -> list[Message]:
        """Get all undelivered messages for a user"""
        undelivered = []
        for msg in self._messages:
            if msg.is_group_message:
                # For group messages, check if user is in the group and hasn't received the message
                if msg.to_user_id.startswith('#') and user_id not in msg.delivered_to:
                    undelivered.append(msg)
            else:
                # For DMs, check if message is for this user and hasn't been delivered
                if msg.to_user_id == user_id and user_id not in msg.delivered_to:
                    undelivered.append(msg)
        return undelivered

    def mark_delivered(self, message_id: str, user_id: str):
        """Mark a message as delivered to a specific user"""
        # Update in memory
        for msg in self._messages:
            if msg.message_id == message_id:
                msg.delivered_to.add(user_id)
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
        """Get message history for a conversation"""
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
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path
        self.groups_by_name: Dict[str, Group] = {}
        self._load()

    def _load(self):
        """Load groups from JSONL file"""
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
        """Save a single group to file"""
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
        """Create a new group"""
        if name in self.groups_by_name:
            raise ValueError(f"Group {name} already exists")
        
        group = Group(
            name=name,
            creator_id=creator_id,
            member_ids={creator_id},  # Creator is first member
            created_ts=created_ts
        )
        self.groups_by_name[name] = group
        self._save_group(group)
        return group

    def add_member(self, group_name: str, user_id: str) -> bool:
        """Add a member to a group. Returns True if user was added, False if already a member"""
        group = self.groups_by_name.get(group_name)
        if not group:
            raise ValueError(f"Group {group_name} does not exist")
        
        if user_id in group.member_ids:
            return False

        # Update in memory
        group.member_ids.add(user_id)
        
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
        
        return True

    def get_group(self, name: str) -> Optional[Group]:
        """Get a group by name"""
        return self.groups_by_name.get(name)

    def get_user_groups(self, user_id: str) -> list[Group]:
        """Get all groups that a user is a member of"""
        return [
            group for group in self.groups_by_name.values()
            if user_id in group.member_ids
        ]

    def exists(self, name: str) -> bool:
        """Check if a group exists"""
        return name in self.groups_by_name

    def is_member(self, group_name: str, user_id: str) -> bool:
        """Check if a user is a member of a group"""
        group = self.groups_by_name.get(group_name)
        return group is not None and user_id in group.member_ids
