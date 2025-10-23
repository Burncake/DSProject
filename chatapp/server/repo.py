import json, os
from typing import Dict, Optional, Iterable
from .models import User, Message

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


class MessagesRepo:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.path = path

    def append(self, m: Message):
        rec = {
            "message_id": m.message_id,
            "from_user_id": m.from_user_id,
            "to_user_id": m.to_user_id,
            "text": m.text,
            "sent_ts": m.sent_ts,
            "delivered": m.delivered,
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            f.flush(); os.fsync(f.fileno())
