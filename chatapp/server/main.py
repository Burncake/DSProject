import asyncio
import os
from grpc import aio
from ..proto import chat_pb2_grpc
from .service import ChatService, logger  # Reuse the same logger
from .repo import UsersRepo, MessagesRepo, GroupsRepo
from .hub import Hub

async def serve(host="127.0.0.1", port=50051):
    """Start the chat server.
    
    Sets up and runs the gRPC server with chat service implementation.
    Initializes all required components:
    - User repository
    - Message repository
    - Group repository
    - Message delivery hub
    
    Args:
        host (str): Hostname to bind server to. Defaults to localhost.
        port (int): Port number to listen on. Defaults to 50051.
        
    Side Effects:
        - Creates data directories if needed
        - Starts gRPC server
        - Logs server startup progress
    """
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
    logger.info(f"Server starting, listening on {listen_addr}")
    await server.start()
    logger.info(f"Server is now running on {listen_addr}")
    await server.wait_for_termination()

if __name__ == "__main__":
    asyncio.run(serve())
