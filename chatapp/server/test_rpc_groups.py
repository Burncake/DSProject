import unittest
import tempfile
import os
import json
import asyncio
from chatapp.server.repo import UsersRepo, MessagesRepo, GroupsRepo
from chatapp.server.service import ChatService
from chatapp.server.hub import Hub
from chatapp.proto import chat_pb2

class TestRPCGroups(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.users_file = os.path.join(self.temp_dir, "users.jsonl")
        self.messages_file = os.path.join(self.temp_dir, "messages.jsonl")
        self.groups_file = os.path.join(self.temp_dir, "groups.jsonl")

    def tearDown(self):
        for p in [self.users_file, self.messages_file, self.groups_file]:
            if os.path.exists(p):
                os.remove(p)
        try:
            os.rmdir(self.temp_dir)
        except Exception:
            pass

    def test_list_user_groups_rpc(self):
        users_repo = UsersRepo(self.users_file)
        messages_repo = MessagesRepo(self.messages_file)
        groups_repo = GroupsRepo(self.groups_file)
        hub = Hub()
        service = ChatService(users_repo, messages_repo, groups_repo, hub)

        # create groups
        groups_repo.create_group('#g1', 'u1', 1)
        groups_repo.add_member('#g1', 'u2')
        groups_repo.create_group('#g2', 'u2', 2)

        async def call():
            resp = await service.ListUserGroups(chat_pb2.ListUserGroupsRequest(user_id='u2'), None)
            return resp

        resp = asyncio.run(call())
        names = sorted([g.name for g in resp.groups])
        self.assertEqual(names, ['#g1', '#g2'])

    def test_list_groups_rpc(self):
        users_repo = UsersRepo(self.users_file)
        messages_repo = MessagesRepo(self.messages_file)
        groups_repo = GroupsRepo(self.groups_file)
        hub = Hub()
        service = ChatService(users_repo, messages_repo, groups_repo, hub)

        groups_repo.create_group('#g1', 'u1', 1)
        groups_repo.create_group('#g2', 'u2', 2)

        async def call():
            resp = await service.ListGroups(chat_pb2.ListGroupsRequest(), None)
            return resp

        resp = asyncio.run(call())
        names = sorted([g.name for g in resp.groups])
        self.assertEqual(names, ['#g1', '#g2'])

if __name__ == '__main__':
    unittest.main()
