import asyncio, time, uuid, re, typer
import grpc
from grpc import aio
from ..proto import chat_pb2, chat_pb2_grpc

app = typer.Typer(help="Simple gRPC chat client (DMs)")

async def _run(display_name: str, host: str, port: int, register: bool = False):
    """Main client loop handling connection and chat operations.
    
    Connects to chat server and provides interactive CLI interface for:
    - User registration/login
    - Direct messaging
    - Group creation and management
    - User search
    
    Args:
        display_name (str): User's display name (will prompt if empty)
        host (str): Chat server hostname
        port (int): Chat server port
        register (bool): True to register new user, False to try login first
        
    Side Effects:
        - Connects to gRPC server
        - Creates user if registration requested
        - Starts interactive CLI session
    """
    chan = aio.insecure_channel(f"{host}:{port}")
    stub = chat_pb2_grpc.ChatServiceStub(chan)

    # Variable to store user_id
    user_id = None

    if not display_name:
        display_name = input("Enter your display name: ").strip()

    # Try login first if not registering
    if not register:
        try:
            login_response = await stub.LoginUser(chat_pb2.LoginRequest(display_name=display_name))
            if login_response.success:
                user_id = login_response.user_id
                print(f"Logged in as {display_name} ({user_id})")
            else:
                print(f"Login failed: {login_response.error_message}")
                if input("Would you like to register as a new user? (y/n): ").lower() == 'y':
                    register = True
                else:
                    return
        except grpc.aio.AioRpcError as e:
            print(f"Error during login: {e.details()}")
            if input("Would you like to register as a new user? (y/n): ").lower() == 'y':
                register = True
            else:
                return

    # Register if needed
    if register:
        try:
            reg = await stub.RegisterUser(chat_pb2.RegisterRequest(display_name=display_name))
            user_id = reg.user_id
            print(f"Registered as {display_name} ({user_id})")
        except grpc.aio.AioRpcError as e:
            if e.code() == grpc.StatusCode.ALREADY_EXISTS:
                print(f"Error: {e.details()}")
            else:
                print(f"Error during registration: {e.details()}")
            return

    # Check if we have a valid user_id
    if not user_id:
        print("Error: Failed to obtain user ID")
        return

    # cache of name -> id to make /dm faster
    name_cache = {}

    async def resolve_name(name: str) -> str | None:
        """Resolve display name to user ID.
        
        Searches for user by display name, with preference for exact matches.
        Caches successful resolutions for future use.
        
        Args:
            name (str): Display name to resolve
            
        Returns:
            str | None: User ID if found, None if no matching user
            
        Side Effects:
            - Updates name_cache with resolved IDs
            - Prints feedback about name resolution
        """
        # query server for substring match; prefer exact display_name
        resp = await stub.SearchUsers(chat_pb2.SearchUsersRequest(query=name))
        exact = [u for u in resp.users if u.display_name == name]
        if exact:
            name_cache[name] = exact[0].id
            return exact[0].id
        if resp.users:
            # first match fallback
            pick = resp.users[0]
            print(f"[hint] Using first match: {pick.display_name} -> {pick.id}")
            name_cache[pick.display_name] = pick.id
            return pick.id
        print("[warn] No user found")
        return None

    async def outgoing():
        """Generate outgoing chat messages from user input.
        
        Implements command processing for:
        - /search: Find users by display name
        - /dm: Send direct message to user
        - /create-group: Create new chat group
        - /join-group: Join existing group
        - /group: Send message to group
        - /list-groups: Show joined groups
        - /help: Show available commands
        
        Yields:
            ChatEnvelope: Protocol messages for server communication
            
        Side Effects:
            - Reads from standard input
            - Prints command responses and help text
            - Updates name cache with resolved user IDs
        """
        # First SYSTEM identify
        yield chat_pb2.ChatEnvelope(type=chat_pb2.SYSTEM, from_user_id=user_id, sent_ts=int(time.time()*1000))

        # Interactive loop
        loop = asyncio.get_event_loop()
        while True:
            line = await loop.run_in_executor(None, input, "")
            line = line.strip()

            # /search <query>
            if line.startswith("/search "):
                q = line[len("/search "):].strip()
                resp = await stub.SearchUsers(chat_pb2.SearchUsersRequest(query=q))
                if not resp.users:
                    print("[search] No matches")
                else:
                    for u in resp.users:
                        print(f"[search] {u.display_name} ({u.id})")
                continue

            # /list-users - ask server for all users (empty query)
            if line.strip() == "/list-users":
                resp = await stub.SearchUsers(chat_pb2.SearchUsersRequest(query=""))
                if not resp.users:
                    print("[list-users] No users found")
                else:
                    print("[list-users] Users:")
                    for u in resp.users:
                        print(f" - {u.display_name} ({u.id})")
                continue

            # /create-group #name
            if line.startswith("/create-group "):
                group_name = line[len("/create-group "):].strip()
                if not group_name.startswith("#"):
                    print("[error] Group name must start with #")
                    continue
                resp = await stub.CreateGroup(chat_pb2.CreateGroupRequest(
                    group_name=group_name,
                    creator_user_id=user_id
                ))
                if resp.success:
                    print(f"[group] Created group {group_name}")
                else:
                    print(f"[error] Failed to create group: {resp.error_message}")
                continue

            # /join-group #name
            if line.startswith("/join-group "):
                group_name = line[len("/join-group "):].strip()
                resp = await stub.JoinGroup(chat_pb2.JoinGroupRequest(
                    group_name=group_name,
                    user_id=user_id
                ))
                if resp.success:
                    print(f"[group] Joined group {group_name}")
                else:
                    print(f"[error] Failed to join group: {resp.error_message}")
                continue

            # /list-groups - list groups the current user is a member of (server-side)
            if line.strip() == "/list-groups":
                # Call server RPC ListUserGroups with our user_id
                try:
                    resp = await stub.ListUserGroups(chat_pb2.ListUserGroupsRequest(user_id=user_id))
                except grpc.aio.AioRpcError as e:
                    print(f"[list-groups] RPC error: {e.details()}")
                    continue

                if not resp.groups:
                    print("[list-groups] No groups found")
                else:
                    print("[list-groups] Groups:")
                    for g in resp.groups:
                        members = ",".join(g.member_ids)
                        print(f" - {g.name} members={members}")
                continue

            # /dm @name message...
            m = re.match(r"^/dm\s+@?(\S+)\s+(.+)$", line)
            if m:
                target_name, msg_text = m.group(1), m.group(2)
                target_id = name_cache.get(target_name) or await resolve_name(target_name)
                if not target_id:
                    continue
                yield chat_pb2.ChatEnvelope(
                    type=chat_pb2.SEND_DM,
                    from_user_id=user_id,
                    to_user_id=target_id,
                    message_id=uuid.uuid4().hex,
                    text=msg_text,
                    sent_ts=int(time.time()*1000),
                )
                continue
                
            # /group #name message...
            m = re.match(r"^/group\s+#(\S+)\s+(.+)$", line)
            if m:
                group_name = "#" + m.group(1)
                msg_text = m.group(2)
                yield chat_pb2.ChatEnvelope(
                    type=chat_pb2.SEND_GROUP,
                    from_user_id=user_id,
                    group_id=group_name,
                    message_id=uuid.uuid4().hex,
                    text=msg_text,
                    sent_ts=int(time.time()*1000),
                )
                continue

            # default: local help
            if line in {"/help", "help"}:
                print("Commands:\n"
                      "  /search <query>\n"
                      "  /dm @<display_name> <message>\n"
                      "  /create-group #<name>\n"
                      "  /join-group #<name>\n"
                      "  /group #<name> <message>\n"
                      "  /help")
                continue

            print('Type "/help" for commands.')

    # Reader: print all incoming (ACKs and DMs)
    async def reader(call):
        """Process incoming messages from server stream.
        
        Handles different message types:
        - SEND_DM: Direct messages from other users
        - SEND_GROUP: Messages sent to groups
        - ACK: Delivery acknowledgments
        - Other: Debug information
        
        Args:
            call: Active gRPC stream call
            
        Side Effects:
            - Prints formatted messages to console
        """
        async for env in call:
            if env.type == chat_pb2.SEND_DM:
                print(f"[DM] from {env.from_user_id}: {env.text}")
            elif env.type == chat_pb2.SEND_GROUP:
                print(f"[GROUP {env.group_id}] {env.from_user_id}: {env.text}")
            elif env.type == chat_pb2.ACK:
                print(f"[ACK] {env.message_id} {env.text}")
            else:
                print(f"[IN] type={env.type} from={env.from_user_id} text={env.text}")

    call = stub.OpenStream(outgoing())
    await reader(call)

@app.command("run")
def run_cmd(
    name: str = "",
    host: str = "127.0.0.1",
    port: int = 50051,
    register: bool = False
):
    """
    Run the chat client.
    
    Args:
        name: Display name to use
        host: Server hostname
        port: Server port
        register: If True, register as new user. If False, try to login first
    """
    asyncio.run(_run(name, host, port, register))

if __name__ == "__main__":
    app()
