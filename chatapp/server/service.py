import asyncio, time, uuid, contextlib, grpc, logging, os
from grpc import aio
from typing import AsyncIterable
from ..proto import chat_pb2, chat_pb2_grpc
from .repo import UsersRepo, MessagesRepo, GroupsRepo
from .models import User, Message, Group
from .hub import Hub

# Set up logging configuration
logger = logging.getLogger('chatapp.server')
logger.setLevel(logging.DEBUG)

# Create formatters and handlers
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.INFO)

# File handler - ensure log directory exists
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
file_handler = logging.FileHandler(os.path.join(log_dir, 'server.log'))
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    """gRPC service implementation for chat functionality.
    
    Handles user registration, messaging, group management and real-time
    message delivery through streaming connections.
    """
    
    def __init__(self, users_repo: UsersRepo, messages_repo: MessagesRepo, groups_repo: GroupsRepo, hub: Hub):
        """Initialize chat service with required repositories and message hub.
        
        Args:
            users_repo (UsersRepo): Repository for user management
            messages_repo (MessagesRepo): Repository for message storage
            groups_repo (GroupsRepo): Repository for group management
            hub (Hub): Real-time message delivery hub
            
        Attributes:
            users: User repository instance
            messages: Message repository instance
            groups: Group repository instance
            hub: Message hub instance
            groups_lock: Lock for thread-safe group operations
        """
        self.users = users_repo
        self.messages = messages_repo
        self.groups = groups_repo
        self.hub = hub
        self.groups_lock = asyncio.Lock()

    async def RegisterUser(self, request: chat_pb2.RegisterRequest, context: aio.ServicerContext):
        """Register a new user in the chat system.
        
        Args:
            request (RegisterRequest): Contains desired display_name
            context (ServicerContext): gRPC service context
            
        Returns:
            RegisterResponse: Contains assigned user_id on success
            
        Raises:
            ALREADY_EXISTS: If display_name is already taken
            
        Side Effects:
            - Creates new user in repository
            - Logs registration attempt and result
        """
        # Check if user already exists
        existing_user = self.users.find_by_display_name(request.display_name)
        if existing_user:
            logger.error(f"RegisterUser: User '{request.display_name}' registration failed (name already exists)")
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, f"User {request.display_name} already exists")
            
        # Create new user
        user_id = uuid.uuid4().hex[:12]
        self.users.append_user(User(id=user_id, display_name=request.display_name))
        logger.info(f"RegisterUser: User '{request.display_name}' registered successfully with ID '{user_id}'")
        return chat_pb2.RegisterResponse(user_id=user_id)
        
    async def LoginUser(self, request: chat_pb2.LoginRequest, context: aio.ServicerContext):
        """Authenticate a user by display name.
        
        Args:
            request (LoginRequest): Contains user's display_name
            context (ServicerContext): gRPC service context
            
        Returns:
            LoginResponse: Contains success status, user_id if successful,
                         or error message if failed
            
        Note:
            This is a simple implementation that only verifies the display
            name exists. A production system would use proper authentication.
        """
        user = self.users.find_by_display_name(request.display_name)
        if not user:
            return chat_pb2.LoginResponse(
                success=False,
                error_message=f"User {request.display_name} not found"
            )
            
        return chat_pb2.LoginResponse(
            success=True,
            user_id=user.id
        )

    async def SearchUsers(self, request, context):
        """Search for users by display name.
        
        Performs a case-insensitive substring search on user display names.
        
        Args:
            request (SearchUsersRequest): Contains search query
            context (ServicerContext): gRPC service context
            
        Returns:
            SearchUsersResponse: List of matching users with their IDs
                               and display names
            
        Note:
            Current implementation is a simple substring search.
            Could be enhanced with more sophisticated search in future.
        """
        q = (request.query or "").lower()
        matched = []
        for u in self.users.users_by_id.values():
            if q in u.display_name.lower():
                matched.append(chat_pb2.User(id=u.id, display_name=u.display_name))
        return chat_pb2.SearchUsersResponse(users=matched)

    async def CreateGroup(self, request: chat_pb2.CreateGroupRequest, context: aio.ServicerContext):
        """Create a new chat group.
        
        Args:
            request (CreateGroupRequest): Contains group_name and creator_user_id
            context (ServicerContext): gRPC service context
            
        Returns:
            CreateGroupResponse: Contains success status and error message if failed
            
        Notes:
            - Group names must start with '#'
            - Creator is automatically added as first member
            - Group creation is protected by lock for thread safety
            
        Side Effects:
            - Creates new group in repository if successful
            - Logs creation attempt and result
        """
        group_name = request.group_name
        creator_id = request.creator_user_id

        # Validate group name
        if not group_name.startswith("#"):
            logger.error(f"CreateGroup: Invalid group name format '{group_name}' from user '{creator_id}'")
            return chat_pb2.CreateGroupResponse(
                success=False,
                error_message="Group name must start with #"
            )

        try:
            async with self.groups_lock:
                # Create new group with creator as first member
                self.groups.create_group(
                    name=group_name,
                    creator_id=creator_id,
                    created_ts=int(time.time() * 1000)
                )
                logger.info(f"CreateGroup: User '{creator_id}' created group '{group_name}'")
                return chat_pb2.CreateGroupResponse(success=True)
        except ValueError as e:
            logger.error(f"CreateGroup: Failed to create group '{group_name}' by user '{creator_id}': {str(e)}")
            return chat_pb2.CreateGroupResponse(
                success=False,
                error_message=str(e)
            )

    async def JoinGroup(self, request: chat_pb2.JoinGroupRequest, context: aio.ServicerContext):
        """Add a user to an existing group.
        
        Args:
            request (JoinGroupRequest): Contains group_name and user_id
            context (ServicerContext): gRPC service context
            
        Returns:
            JoinGroupResponse: Contains success status and error message if failed
            
        Notes:
            - Checks if group exists
            - Verifies user is not already a member
            - Protected by lock for thread safety
            
        Side Effects:
            - Updates group membership in repository if successful
        """
        group_name = request.group_name
        user_id = request.user_id

        try:
            async with self.groups_lock:
                if not self.groups.exists(group_name):
                    return chat_pb2.JoinGroupResponse(
                        success=False,
                        error_message="Group does not exist"
                    )
                
                # Add user to group
                added = self.groups.add_member(group_name, user_id)
                if not added:
                    return chat_pb2.JoinGroupResponse(
                        success=False,
                        error_message="User is already a member of this group"
                    )
                
            return chat_pb2.JoinGroupResponse(success=True)
        except ValueError as e:
            return chat_pb2.JoinGroupResponse(
                success=False,
                error_message=str(e)
            )

    async def ListGroups(self, request: chat_pb2.ListGroupsRequest, context: aio.ServicerContext):
        """List all existing chat groups.
        
        Args:
            request (ListGroupsRequest): Empty request
            context (ServicerContext): gRPC service context
            
        Returns:
            ListGroupsResponse: List of all groups with their names
                              and member IDs
        """
        groups = []
        for g in self.groups.groups_by_name.values():
            groups.append(chat_pb2.Group(name=g.name, member_ids=list(g.member_ids)))
        return chat_pb2.ListGroupsResponse(groups=groups)

    async def ListUserGroups(self, request: chat_pb2.ListUserGroupsRequest, context: aio.ServicerContext):
        """List all groups that a specific user is a member of.
        
        Args:
            request (ListUserGroupsRequest): Contains user_id
            context (ServicerContext): gRPC service context
            
        Returns:
            ListUserGroupsResponse: List of groups the user is a member of,
                                  including group names and member IDs
        """
        user_id = request.user_id
        groups = []
        for g in self.groups.get_user_groups(user_id):
            groups.append(chat_pb2.Group(name=g.name, member_ids=list(g.member_ids)))
        return chat_pb2.ListUserGroupsResponse(groups=groups)

    async def OpenStream(self, request_iterator: AsyncIterable[chat_pb2.ChatEnvelope], context: aio.ServicerContext):
        """Open a bidirectional streaming connection with a client.
        
        Establishes a persistent connection for real-time message exchange.
        Manages message delivery, offline message queuing, and stream lifecycle.
        
        Protocol Flow:
        1. Client sends initial SYSTEM message with from_user_id
        2. Server registers message queue for the user
        3. Server delivers any pending offline messages
        4. Bidirectional streaming begins for real-time messages
        
        Args:
            request_iterator: Stream of incoming messages from client
            context: gRPC service context
            
        Yields:
            ChatEnvelope: Messages to be delivered to the client
            
        Side Effects:
            - Registers user's message queue in hub
            - Delivers queued offline messages
            - Processes incoming messages
            - Updates message delivery status
            - Logs connection lifecycle events
            
        Notes:
            - Connection remains active until client disconnects
            - Handles both direct and group messages
            - Provides delivery acknowledgments
            - Ensures proper cleanup on disconnect
        """
        first = await anext(request_iterator)
        if first.type != chat_pb2.SYSTEM or not first.from_user_id:
            logger.error("ChatStream: Invalid first message - not SYSTEM or missing user_id")
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "First message must be SYSTEM with from_user_id")
        user_id = first.from_user_id
        logger.info(f"ChatStream: User '{user_id}' connected to stream")

        # Register user's queue for real-time messages
        q = await self.hub.register_queue(user_id)

        # Send any undelivered messages that were queued while user was offline
        undelivered = self.messages.get_undelivered_messages(user_id, self.groups)
        if undelivered:
            logger.info(f"Found {len(undelivered)} undelivered messages for {user_id}")
            for msg in undelivered:
                # Group membership is already checked in get_undelivered_messages
                if msg.is_group_message:
                    envelope = chat_pb2.ChatEnvelope(
                        type=chat_pb2.SEND_GROUP,
                        from_user_id=msg.from_user_id,
                        group_id=msg.to_user_id,
                        message_id=msg.message_id,
                        text=msg.text,
                        sent_ts=msg.sent_ts,
                    )
                else:
                    envelope = chat_pb2.ChatEnvelope(
                        type=chat_pb2.SEND_DM,
                        from_user_id=msg.from_user_id,
                        to_user_id=msg.to_user_id,
                        message_id=msg.message_id,
                        text=msg.text,
                        sent_ts=msg.sent_ts,
                    )
                
                await q.put(envelope)
                self.messages.mark_delivered(msg.message_id, user_id)
                print(f"[SERVICE] Delivered offline message {msg.message_id} to {user_id}")

        async def reader():
            """Process incoming messages from the client stream.
            
            Handles various message types:
            - Direct Messages (SEND_DM): Deliver to recipient or queue if offline
            - Group Messages (SEND_GROUP): Broadcast to all online group members
            - System Messages: Process control messages
            
            For each message:
            - Assigns unique message ID if not provided
            - Attempts real-time delivery to online recipients
            - Stores messages for offline delivery
            - Sends delivery acknowledgments back to sender
            - Validates group membership for group messages
            
            Side Effects:
                - Stores messages in repository
                - Updates message delivery status
                - Sends acknowledgments via hub
                - Logs message processing status
            """
            async for incoming in request_iterator:
                msg_id = incoming.message_id or uuid.uuid4().hex
                current_time = int(time.time() * 1000)

                if incoming.type == chat_pb2.SEND_DM and incoming.to_user_id and incoming.text:
                    # Handle Direct Message
                    msg = Message(
                        message_id=msg_id,
                        from_user_id=user_id,
                        to_user_id=incoming.to_user_id,
                        text=incoming.text,
                        sent_ts=current_time,
                    )
                    logger.debug(f"ChatStream: Received DM from '{user_id}' to '{msg.to_user_id}': {msg.message_id}")
                    
                    # try live delivery
                    delivered = await self.hub.send_to_user(msg.to_user_id, chat_pb2.ChatEnvelope(
                        type=chat_pb2.SEND_DM,
                        from_user_id=msg.from_user_id,
                        to_user_id=msg.to_user_id,
                        message_id=msg.message_id,
                        text=msg.text,
                        sent_ts=msg.sent_ts,
                    ))
                    msg.delivered = bool(delivered)
                    self.messages.append(msg)
                    logger.debug(f"ChatStream: Forward DM to '{msg.to_user_id}' status: {'delivered' if delivered else 'queued'}")

                    # ack back to sender
                    ack = chat_pb2.ChatEnvelope(
                        type=chat_pb2.ACK,
                        from_user_id="server",
                        to_user_id=user_id,
                        message_id=msg.message_id,
                        text="delivered" if delivered else "queued",
                        sent_ts=current_time,
                    )
                    await self.hub.send_to_user(user_id, ack)

                elif incoming.type == chat_pb2.SEND_GROUP and incoming.group_id and incoming.text:
                    # Handle Group Message
                    group_name = incoming.group_id
                    async with self.groups_lock:
                        group = self.groups.get_group(group_name)
                        if not group:
                            # Group doesn't exist - send error ACK
                            ack = chat_pb2.ChatEnvelope(
                                type=chat_pb2.ACK,
                                from_user_id="server",
                                to_user_id=user_id,
                                message_id=msg_id,
                                text="error: group not found",
                                sent_ts=current_time,
                            )
                            await self.hub.send_to_user(user_id, ack)
                            continue

                        if not self.groups.is_member(group_name, user_id):
                            # User not in group - send error ACK
                            ack = chat_pb2.ChatEnvelope(
                                type=chat_pb2.ACK,
                                from_user_id="server",
                                to_user_id=user_id,
                                message_id=msg_id,
                                text="error: not a member of this group",
                                sent_ts=current_time,
                            )
                            await self.hub.send_to_user(user_id, ack)
                            continue

                        # Create group message
                        msg = Message(
                            message_id=msg_id,
                            from_user_id=user_id,
                            to_user_id=group_name,  # Use group_name as to_user_id for group messages
                            text=incoming.text,
                            sent_ts=current_time,
                            is_group_message=True,
                            delivered_to=set()  # Start with empty set of delivered users
                        )

                        # Broadcast to all group members
                        group_msg = chat_pb2.ChatEnvelope(
                            type=chat_pb2.SEND_GROUP,
                            from_user_id=user_id,
                            group_id=group_name,
                            message_id=msg_id,
                            text=incoming.text,
                            sent_ts=current_time,
                        )
                        logger.debug(f"ChatStream: Received group message from '{user_id}' to '{group_name}': {msg_id}")

                        delivered_count = 0
                        for member_id in group.member_ids:
                            if member_id != user_id:  # Skip sender
                                delivered = await self.hub.send_to_user(member_id, group_msg)
                                logger.debug(f"ChatStream: Broadcast group message to '{member_id}' status: {'delivered' if delivered else 'queued'}")
                                if delivered:
                                    delivered_count += 1
                                    msg.delivered_to.add(member_id)

                        # Always mark as delivered for sender
                        msg.delivered_to.add(user_id)
                        
                        # Save message after delivery attempts
                        self.messages.append(msg)

                        # Send ACK to sender
                        total_members = len(group.member_ids) - 1  # exclude sender
                        ack = chat_pb2.ChatEnvelope(
                            type=chat_pb2.ACK,
                            from_user_id="server",
                            to_user_id=user_id,
                            message_id=msg_id,
                            text=f"delivered to {delivered_count}/{total_members} members",
                            sent_ts=current_time,
                        )
                        await self.hub.send_to_user(user_id, ack)

                else:
                    # default ACK for unknown event
                    ack = chat_pb2.ChatEnvelope(
                        type=chat_pb2.ACK, from_user_id="server", to_user_id=user_id,
                        message_id=incoming.message_id or uuid.uuid4().hex, text="ack",
                        sent_ts=int(time.time()*1000)
                    )
                    await self.hub.send_to_user(user_id, ack)

        reader_task = asyncio.create_task(reader())
        try:
            while True:
                out_msg = await q.get()
                yield out_msg
        finally:
            reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await reader_task
            await self.hub.remove_queue(user_id)
            logger.info(f"ChatStream: User '{user_id}' disconnected from stream")

async def _merge_streams(writer_async_gen, reader_coro):
    """Merge an async generator with a coroutine running in background.
    
    A utility function that allows running a background task (reader)
    while yielding items from an async generator (writer).
    
    Args:
        writer_async_gen: Async generator producing items to yield
        reader_coro: Coroutine to run in background
        
    Yields:
        Items from writer_async_gen
        
    Side Effects:
        - Creates background task for reader_coro
        - Ensures proper cleanup of background task on exit
        - Suppresses CancelledError during cleanup
    """
    task = asyncio.create_task(reader_coro)
    try:
        async for item in writer_async_gen:
            yield item
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
