import asyncio
from typing import Dict, Optional

class Hub:
    def __init__(self):
        # user_id -> asyncio.Queue of outbound messages (ChatEnvelope protobuf)
        self.queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()

    async def register_queue(self, user_id: str) -> asyncio.Queue:
        async with self._lock:
            q = asyncio.Queue()
            self.queues[user_id] = q
            print(f"[HUB] Registered queue for user {user_id}")
            print(f"[HUB] Active users: {list(self.queues.keys())}")
            return q

    async def remove_queue(self, user_id: str):
        async with self._lock:
            self.queues.pop(user_id, None)

    async def send_to_user(self, user_id, envelope):
        async with self._lock:
            q = self.queues.get(user_id)
            print(f"[HUB] Sending message to user {user_id}")
            print(f"[HUB] User has queue: {q is not None}")
            print(f"[HUB] Active users: {list(self.queues.keys())}")
        if q:
            await q.put(envelope)
            print(f"[HUB] Message sent to user {user_id}")
            return True
        print(f"[HUB] Failed to send message to user {user_id} - not online")
        return False
