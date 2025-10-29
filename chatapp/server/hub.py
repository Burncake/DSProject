import asyncio
from typing import Dict, Optional
from ..utils.logger import setup_logger

logger = setup_logger('chatapp.hub')

class Hub:
    def __init__(self):
        # user_id -> asyncio.Queue of outbound messages (ChatEnvelope protobuf)
        self.queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()
        logger.info("Message Hub initialized")

    async def register_queue(self, user_id: str) -> asyncio.Queue:
        async with self._lock:
            q = asyncio.Queue()
            self.queues[user_id] = q
            logger.info(f"Registered queue for user {user_id}")
            logger.debug(f"Active users: {list(self.queues.keys())}")
            return q

    async def remove_queue(self, user_id: str):
        async with self._lock:
            self.queues.pop(user_id, None)
            logger.info(f"Removed queue for user {user_id}")
            logger.debug(f"Remaining active users: {list(self.queues.keys())}")

    async def send_to_user(self, user_id, envelope):
        async with self._lock:
            q = self.queues.get(user_id)
            logger.debug(f"Attempting to send message to user {user_id}")
            logger.debug(f"User has queue: {q is not None}")
            logger.debug(f"Active users: {list(self.queues.keys())}")
        
        if q:
            await q.put(envelope)
            logger.info(f"Message sent to user {user_id}")
            return True
            
        logger.warning(f"Failed to send message to user {user_id} - not online")
        return False
