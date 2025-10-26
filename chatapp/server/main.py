import asyncio
from grpc import aio
from ..proto import chat_pb2_grpc
from .service import ChatService
from .repo import UsersRepo, MessagesRepo, GroupsRepo
from .hub import Hub

async def serve(host="127.0.0.1", port=50051):
    server = aio.server()
    users_repo = UsersRepo("chatapp/data/users.jsonl")
    messages_repo = MessagesRepo("chatapp/data/messages.jsonl")
    groups_repo = GroupsRepo("chatapp/data/groups.jsonl")
    hub = Hub()
    chat_pb2_grpc.add_ChatServiceServicer_to_server(
        ChatService(users_repo, messages_repo, groups_repo, hub), server
    )
    listen_addr = f"{host}:{port}"
    server.add_insecure_port(listen_addr)
    print(f"gRPC server listening on {listen_addr}")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
