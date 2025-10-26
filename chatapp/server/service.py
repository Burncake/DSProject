import asyncio, time, uuid, contextlib, grpc
from grpc import aio
from typing import AsyncIterable
from ..proto import chat_pb2, chat_pb2_grpc
from .repo import UsersRepo, MessagesRepo, GroupsRepo
from .models import User, Message, Group
from .hub import Hub

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def __init__(self, users_repo: UsersRepo, messages_repo: MessagesRepo, groups_repo: GroupsRepo, hub: Hub):
        self.users = users_repo
        self.messages = messages_repo
        self.groups = groups_repo
        self.hub = hub
        self.groups_lock = asyncio.Lock()

    async def RegisterUser(self, request: chat_pb2.RegisterRequest, context: aio.ServicerContext):
        # Check if user already exists
        existing_user = self.users.find_by_display_name(request.display_name)
        if existing_user:
            await context.abort(grpc.StatusCode.ALREADY_EXISTS, f"User {request.display_name} already exists")
            
        # Create new user
        user_id = uuid.uuid4().hex[:12]
        self.users.append_user(User(id=user_id, display_name=request.display_name))
        return chat_pb2.RegisterResponse(user_id=user_id)
        
    async def LoginUser(self, request: chat_pb2.LoginRequest, context: aio.ServicerContext):
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
        # stub for now — returns everyone whose display_name contains query (case-insensitive)
        q = (request.query or "").lower()
        matched = []
        for u in self.users.users_by_id.values():
            if q in u.display_name.lower():
                matched.append(chat_pb2.User(id=u.id, display_name=u.display_name))
        return chat_pb2.SearchUsersResponse(users=matched)

    async def CreateGroup(self, request: chat_pb2.CreateGroupRequest, context: aio.ServicerContext):
        group_name = request.group_name
        creator_id = request.creator_user_id

        # Validate group name
        if not group_name.startswith("#"):
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
                return chat_pb2.CreateGroupResponse(success=True)
        except ValueError as e:
            return chat_pb2.CreateGroupResponse(
                success=False,
                error_message=str(e)
            )

    async def JoinGroup(self, request: chat_pb2.JoinGroupRequest, context: aio.ServicerContext):
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

    async def OpenStream(self, request_iterator: AsyncIterable[chat_pb2.ChatEnvelope], context: aio.ServicerContext):
        """
        Protocol: client must send a first SYSTEM message with from_user_id set.
        Server registers a queue for that user and starts forwarding messages from the queue back to the client.
        Any SEND_* received will be echoed back to the sender for now (to prove streaming works).
        """
        first = await anext(request_iterator)
        if first.type != chat_pb2.SYSTEM or not first.from_user_id:
            await context.abort(grpc.StatusCode.UNAUTHENTICATED, "First message must be SYSTEM with from_user_id")
        user_id = first.from_user_id
        print(f"[SERVICE] New connection from user {user_id}")

        # Register user's queue for real-time messages
        q = await self.hub.register_queue(user_id)

        # Send any undelivered messages that were queued while user was offline
        undelivered = self.messages.get_undelivered_messages(user_id)
        if undelivered:
            print(f"[SERVICE] Found {len(undelivered)} undelivered messages for {user_id}")
            for msg in undelivered:
                # Check if it's a group message and if user is still a member
                if msg.is_group_message:
                    if not self.groups.is_member(msg.to_user_id, user_id):
                        continue  # Skip if user is no longer in the group
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
                    print(f"[SERVICE] Message {msg.message_id} to {msg.to_user_id}: {'delivered' if delivered else 'queued'}")

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

                        delivered_count = 0
                        for member_id in group.member_ids:
                            if member_id != user_id:  # Skip sender
                                if await self.hub.send_to_user(member_id, group_msg):
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

async def _merge_streams(writer_async_gen, reader_coro):
    # Utility: run a background reader while yielding from writer generator.
    task = asyncio.create_task(reader_coro)
    try:
        async for item in writer_async_gen:
            yield item
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
