import asyncio, time, uuid
import typer
from grpc import aio
from ..proto import chat_pb2, chat_pb2_grpc

app = typer.Typer(help="Simple gRPC chat client (demo)")

async def _run(display_name: str, host: str, port: int):
    chan = aio.insecure_channel(f"{host}:{port}")
    stub = chat_pb2_grpc.ChatServiceStub(chan)

    # 1) Register
    reg = await stub.RegisterUser(chat_pb2.RegisterRequest(display_name=display_name))
    user_id = reg.user_id
    print(f"Registered as {display_name} ({user_id})")

    # 2) Open bidi stream. First message must be SYSTEM with from_user_id.
    async def outgoing():
        yield chat_pb2.ChatEnvelope(
            type=chat_pb2.SYSTEM, from_user_id=user_id, sent_ts=int(time.time()*1000)
        )
        # send one sample message so you see an ACK come back
        await asyncio.sleep(0.1)
        yield chat_pb2.ChatEnvelope(
            type=chat_pb2.SEND_DM, from_user_id=user_id, message_id=uuid.uuid4().hex,
            text="hello from client", sent_ts=int(time.time()*1000)
        )
        # keep the stream open: read from stdin in a loop (optional)
        while True:
            line = await asyncio.get_event_loop().run_in_executor(None, input, "")
            yield chat_pb2.ChatEnvelope(
                type=chat_pb2.SEND_DM, from_user_id=user_id, message_id=uuid.uuid4().hex,
                text=line, sent_ts=int(time.time()*1000)
            )

    # 3) Consume server messages
    call = stub.OpenStream(outgoing())
    async for envelope in call:
        print(f"[IN] type={envelope.type} from={envelope.from_user_id} text={envelope.text}")

@app.command()
def run(name: str = "alice", host: str = "127.0.0.1", port: int = 50051):
    asyncio.run(_run(name, host, port))

if __name__ == "__main__":
    app()
