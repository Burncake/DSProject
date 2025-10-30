import asyncio
from typing import Dict, Optional
from ..utils.logger import setup_logger

logger = setup_logger('chatapp.hub')

class Hub:
    """Message routing hub for real-time message delivery.
    
    Manages active user connections and message queues for delivering
    messages to online users. Each connected user has a dedicated
    asyncio Queue for message delivery.
    """
    
    def __init__(self):
        """Initialize message hub.
        
        Creates empty dictionary of user queues and thread-safe lock.
        
        Attributes:
            queues (Dict[str, asyncio.Queue]): Maps user IDs to their message queues
            _lock (asyncio.Lock): Ensures thread-safe queue operations
        """
        self.queues: Dict[str, asyncio.Queue] = {}
        self._lock = asyncio.Lock()
        logger.info("Message Hub initialized")

    async def register_queue(self, user_id: str) -> asyncio.Queue:
        """Register a new message queue for a user.
        
        Creates and stores a new asyncio Queue for delivering messages
        to the specified user.
        
        Args:
            user_id (str): ID of user to create queue for
            
        Returns:
            asyncio.Queue: New queue for user's messages
            
        Side Effects:
            - Creates new queue in self.queues
            - Logs registration and active users
        """
        async with self._lock:
            q = asyncio.Queue()
            self.queues[user_id] = q
            logger.info(f"Registered queue for user {user_id}")
            logger.debug(f"Active users: {list(self.queues.keys())}")
            return q

    async def remove_queue(self, user_id: str):
        """Remove a user's message queue.
        
        Removes the message queue for a user, typically called when
        they disconnect from the server.
        
        Args:
            user_id (str): ID of user whose queue to remove
            
        Side Effects:
            - Removes queue from self.queues if it exists
            - Logs removal and remaining active users
        """
        async with self._lock:
            self.queues.pop(user_id, None)
            logger.info(f"Removed queue for user {user_id}")
            logger.debug(f"Remaining active users: {list(self.queues.keys())}")

    async def send_to_user(self, user_id, envelope):
        """Send a message to a specific user if they are online.
        
        Attempts to deliver a message to a user by placing it in their
        message queue if they are currently connected.
        
        Args:
            user_id (str): ID of user to send message to
            envelope: ChatEnvelope protobuf message to deliver
            
        Returns:
            bool: True if message was queued, False if user not online
            
        Side Effects:
            - Places message in user's queue if they are online
            - Logs delivery attempt and status
        """
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
