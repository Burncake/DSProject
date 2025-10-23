import asyncio, time, uuid, re, typer
from grpc import aio
from ..proto import chat_pb2, chat_pb2_grpc

app = typer.Typer(help="Simple gRPC chat client (DMs)")

async def _run(display_name: str, host: str, port: int):
    chan = aio.insecure_channel(f"{host}:{port}")
    stub = chat_pb2_grpc.ChatServiceStub(chan)

    # 1) Register
    reg = await stub.RegisterUser(chat_pb2.RegisterRequest(display_name=display_name))
    user_id = reg.user_id
    print(f"Registered as {display_name} ({user_id})")

    # cache of name -> id to make /dm faster
    name_cache = {}

    async def resolve_name(name: str) -> str | None:
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

            # default: local help
            if line in {"/help", "help"}:
                print("Commands:\n  /search <query>\n  /dm @<display_name> <message>\n  /help")
                continue

            print('Type "/help" for commands.')

    # Reader: print all incoming (ACKs and DMs)
    async def reader(call):
        async for env in call:
            if env.type == chat_pb2.SEND_DM:
                print(f"[DM] from {env.from_user_id}: {env.text}")
            elif env.type == chat_pb2.ACK:
                print(f"[ACK] {env.message_id} {env.text}")
            else:
                print(f"[IN] type={env.type} from={env.from_user_id} text={env.text}")

    call = stub.OpenStream(outgoing())
    await reader(call)

@app.command("run")
def run_cmd(name: str = "alice", host: str = "127.0.0.1", port: int = 50051):
    asyncio.run(_run(name, host, port))

if __name__ == "__main__":
    app()
