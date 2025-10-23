import asyncio, time, uuid, contextlib, grpc
from grpc import aio
from typing import AsyncIterable
from ..proto import chat_pb2, chat_pb2_grpc
from .repo import UsersRepo, MessagesRepo
from .models import User, Message
from .hub import Hub

class ChatService(chat_pb2_grpc.ChatServiceServicer):
    def __init__(self, users_repo: UsersRepo, messages_repo: MessagesRepo, hub: Hub):
        self.users = users_repo
        self.messages = messages_repo
        self.hub = hub

    async def RegisterUser(self, request: chat_pb2.RegisterRequest, context: aio.ServicerContext):
        # create a new user id always (simple demo). Later you can check duplicates by display_name if you want.
        user_id = uuid.uuid4().hex[:12]
        self.users.append_user(User(id=user_id, display_name=request.display_name))
        return chat_pb2.RegisterResponse(user_id=user_id)

    async def SearchUsers(self, request, context):
        # stub for now — returns everyone whose display_name contains query (case-insensitive)
        q = (request.query or "").lower()
        matched = []
        for u in self.users.users_by_id.values():
            if q in u.display_name.lower():
                matched.append(chat_pb2.User(id=u.id, display_name=u.display_name))
        return chat_pb2.SearchUsersResponse(users=matched)

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

        q = await self.hub.register_queue(user_id)

        async def reader():
            async for incoming in request_iterator:
                if incoming.type == chat_pb2.SEND_DM and incoming.to_user_id and incoming.text:
                    # persist
                    msg = Message(
                        message_id=incoming.message_id or uuid.uuid4().hex,
                        from_user_id=user_id,
                        to_user_id=incoming.to_user_id,
                        text=incoming.text,
                        sent_ts=int(time.time()*1000),
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

                    # ack back to sender
                    ack = chat_pb2.ChatEnvelope(
                        type=chat_pb2.ACK,
                        from_user_id="server",
                        to_user_id=user_id,
                        message_id=msg.message_id,
                        text="delivered" if delivered else "queued",
                        sent_ts=int(time.time() * 1000),
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
