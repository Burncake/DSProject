import asyncio
import time
import uuid
import threading
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import grpc
from grpc import aio
from ..proto import chat_pb2, chat_pb2_grpc


class ChatGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("gRPC Chat Application")
        
        # State variables
        self.user_id = None
        self.display_name = None
        self.stub = None
        self.channel = None
        self.stream_task = None
        self.loop = None
        self.name_cache = {}
        self.current_chat = None  # Can be user_id or group_name
        self.current_chat_type = None  # 'dm' or 'group'
        self.outgoing_queue = None  # Queue for outgoing messages
        
        # Create UI
        self.create_login_frame()
        
    def create_login_frame(self):
        """Create the login screen"""
        self.root.geometry("500x350")
        self.login_frame = ttk.Frame(self.root, padding="20")
        self.login_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title = ttk.Label(self.login_frame, text="gRPC Chat", font=("Arial", 24, "bold"))
        title.pack(pady=20)
        
        # Login form
        form_frame = ttk.Frame(self.login_frame)
        form_frame.pack(pady=20)
        
        ttk.Label(form_frame, text="Display Name:", font=("Arial", 12)).grid(row=0, column=0, padx=10, pady=10, sticky=tk.W)
        self.name_entry = ttk.Entry(form_frame, width=30, font=("Arial", 12))
        self.name_entry.grid(row=0, column=1, padx=10, pady=10)
        self.name_entry.focus()
        
        ttk.Label(form_frame, text="Server:", font=("Arial", 12)).grid(row=1, column=0, padx=10, pady=10, sticky=tk.W)
        self.host_entry = ttk.Entry(form_frame, width=30, font=("Arial", 12))
        self.host_entry.insert(0, "127.0.0.1:50051")
        self.host_entry.grid(row=1, column=1, padx=10, pady=10)
        
        # Buttons
        btn_frame = ttk.Frame(self.login_frame)
        btn_frame.pack(pady=20)
        
        ttk.Button(btn_frame, text="Login", command=self.login, width=15).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="Register", command=self.register, width=15).pack(side=tk.LEFT, padx=5)
        
        # Status label
        self.login_status = ttk.Label(self.login_frame, text="", foreground="red")
        self.login_status.pack(pady=10)
        
        # Bind Enter key
        self.name_entry.bind('<Return>', lambda e: self.login())
        
    def create_chat_frame(self):
        """Create the main chat interface"""
        self.root.geometry("900x700")
        self.chat_frame = ttk.Frame(self.root)
        self.chat_frame.pack(fill=tk.BOTH, expand=True)
        
        # Top bar with user info and logout
        top_frame = ttk.Frame(self.chat_frame, padding="5")
        top_frame.pack(fill=tk.X, side=tk.TOP)
        
        ttk.Label(top_frame, text=f"Logged in as: {self.display_name}", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=10)
        ttk.Button(top_frame, text="Logout", command=self.logout).pack(side=tk.RIGHT, padx=10)
        
        # Main container
        main_container = ttk.Frame(self.chat_frame)
        main_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Left sidebar for contacts and groups
        left_frame = ttk.Frame(main_container, width=250)
        left_frame.pack(fill=tk.BOTH, side=tk.LEFT, padx=(0, 5))
        left_frame.pack_propagate(False)
        
        # Notebook for tabs
        self.notebook = ttk.Notebook(left_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # Users tab
        users_frame = ttk.Frame(self.notebook)
        self.notebook.add(users_frame, text="Users")
        
        # Search users
        search_frame = ttk.Frame(users_frame)
        search_frame.pack(fill=tk.X, padx=5, pady=5)
        self.user_search_entry = ttk.Entry(search_frame)
        self.user_search_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        ttk.Button(search_frame, text="Search", command=self.search_users, width=8).pack(side=tk.RIGHT)
        self.user_search_entry.bind('<Return>', lambda e: self.search_users())
        
        # Users listbox
        self.users_listbox = tk.Listbox(users_frame, font=("Arial", 10))
        self.users_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.users_listbox.bind('<<ListboxSelect>>', self.on_user_select)
        
        # Groups tab
        groups_frame = ttk.Frame(self.notebook)
        self.notebook.add(groups_frame, text="Groups")
        
        # Group actions
        group_actions_frame = ttk.Frame(groups_frame)
        group_actions_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Button(group_actions_frame, text="Create Group", command=self.show_create_group_dialog).pack(fill=tk.X, pady=2)
        ttk.Button(group_actions_frame, text="Join Group", command=self.show_join_group_dialog).pack(fill=tk.X, pady=2)
        
        # Groups listbox
        self.groups_listbox = tk.Listbox(groups_frame, font=("Arial", 10))
        self.groups_listbox.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.groups_listbox.bind('<<ListboxSelect>>', self.on_group_select)
        
        # Right side - chat area
        right_frame = ttk.Frame(main_container)
        right_frame.pack(fill=tk.BOTH, expand=True, side=tk.RIGHT)
        
        # Chat header
        self.chat_header = ttk.Label(right_frame, text="Select a user or group to chat", 
                                      font=("Arial", 12, "bold"), background="#e0e0e0", padding=10)
        self.chat_header.pack(fill=tk.X)
        
        # Chat display area
        self.chat_display = scrolledtext.ScrolledText(right_frame, wrap=tk.WORD, 
                                                       font=("Arial", 10), state=tk.DISABLED)
        self.chat_display.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure tags for different message types
        self.chat_display.tag_config("sent", foreground="#0066cc", font=("Arial", 10, "bold"))
        self.chat_display.tag_config("received", foreground="#009900", font=("Arial", 10, "bold"))
        self.chat_display.tag_config("system", foreground="#666666", font=("Arial", 9, "italic"))
        self.chat_display.tag_config("error", foreground="#cc0000", font=("Arial", 10, "bold"))
        self.chat_display.tag_config("timestamp", foreground="#888888", font=("Arial", 8))
        
        # Message input area
        input_frame = ttk.Frame(right_frame)
        input_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.message_entry = ttk.Entry(input_frame, font=("Arial", 11))
        self.message_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))
        self.message_entry.bind('<Return>', lambda e: self.send_message())
        
        self.send_button = ttk.Button(input_frame, text="Send", command=self.send_message, width=10)
        self.send_button.pack(side=tk.RIGHT)
        self.send_button.config(state=tk.DISABLED)
        
    def login(self):
        """Handle login"""
        display_name = self.name_entry.get().strip()
        server = self.host_entry.get().strip()
        
        if not display_name:
            self.login_status.config(text="Please enter a display name")
            return
            
        self.login_status.config(text="Connecting...", foreground="blue")
        self.root.update()
        
        # Start async operations in a new thread
        thread = threading.Thread(target=self._login_async, args=(display_name, server, False))
        thread.daemon = True
        thread.start()
        
    def register(self):
        """Handle registration"""
        display_name = self.name_entry.get().strip()
        server = self.host_entry.get().strip()
        
        if not display_name:
            self.login_status.config(text="Please enter a display name")
            return
            
        self.login_status.config(text="Registering...", foreground="blue")
        self.root.update()
        
        # Start async operations in a new thread
        thread = threading.Thread(target=self._login_async, args=(display_name, server, True))
        thread.daemon = True
        thread.start()
        
    def _login_async(self, display_name, server, register):
        """Async login/register logic"""
        try:
            # Create new event loop for this thread
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            
            # Initialize outgoing message queue
            self.outgoing_queue = asyncio.Queue()
            
            # Connect to server
            self.channel = aio.insecure_channel(server)
            self.stub = chat_pb2_grpc.ChatServiceStub(self.channel)
            
            if register:
                # Register user
                response = self.loop.run_until_complete(
                    self.stub.RegisterUser(chat_pb2.RegisterRequest(display_name=display_name))
                )
                self.user_id = response.user_id
                self.display_name = display_name
            else:
                # Login user
                response = self.loop.run_until_complete(
                    self.stub.LoginUser(chat_pb2.LoginRequest(display_name=display_name))
                )
                if response.success:
                    self.user_id = response.user_id
                    self.display_name = display_name
                else:
                    self.root.after(0, lambda: self.login_status.config(
                        text=f"Login failed: {response.error_message}", foreground="red"))
                    return
            
            # Start message stream
            self.stream_task = self.loop.create_task(self._message_stream())
            
            # Switch to chat UI
            self.root.after(0, self._switch_to_chat)
            
            # Run event loop
            self.loop.run_forever()
            
        except grpc.aio.AioRpcError as e:
            self.root.after(0, lambda: self.login_status.config(
                text=f"Error: {e.details()}", foreground="red"))
        except Exception as e:
            self.root.after(0, lambda: self.login_status.config(
                text=f"Error: {str(e)}", foreground="red"))
            
    def _switch_to_chat(self):
        """Switch from login to chat interface"""
        self.login_frame.destroy()
        self.create_chat_frame()
        self.search_users()  # Load initial user list
        self.load_user_groups()  # Load user's groups
        
    async def _message_stream(self):
        """Handle bidirectional message stream"""
        async def outgoing():
            # Send initial SYSTEM message
            yield chat_pb2.ChatEnvelope(
                type=chat_pb2.SYSTEM,
                from_user_id=self.user_id,
                sent_ts=int(time.time() * 1000)
            )
            
            # Read from outgoing queue and send messages
            while True:
                try:
                    envelope = await self.outgoing_queue.get()
                    yield envelope
                except asyncio.CancelledError:
                    break
        
        try:
            call = self.stub.OpenStream(outgoing())
            async for envelope in call:
                # Handle incoming messages
                self.root.after(0, lambda env=envelope: self._handle_incoming_message(env))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Stream error: {e}")
            self.root.after(0, lambda: self._display_system_message(f"Connection error: {str(e)}"))
            
    def _handle_incoming_message(self, envelope):
        """Handle incoming message from stream"""
        if envelope.type == chat_pb2.SEND_DM:
            # Direct message received
            sender_name = self._get_user_name(envelope.from_user_id)
            self._display_received_message(sender_name, envelope.text, envelope.sent_ts, 'dm', envelope.from_user_id)
            
        elif envelope.type == chat_pb2.SEND_GROUP:
            # Group message received
            sender_name = self._get_user_name(envelope.from_user_id)
            self._display_received_message(sender_name, envelope.text, envelope.sent_ts, 'group', envelope.group_id)
            
        elif envelope.type == chat_pb2.ACK:
            # Acknowledgment
            self._display_system_message(f"[ACK] {envelope.text}")
            
    def _get_user_name(self, user_id):
        """Get display name from user_id (use cache or return ID)"""
        if user_id == self.user_id:
            return "You"
        for name, uid in self.name_cache.items():
            if uid == user_id:
                return name
        return user_id
        
    def search_users(self):
        """Search for users"""
        query = self.user_search_entry.get().strip()
        
        async def _search():
            try:
                response = await self.stub.SearchUsers(chat_pb2.SearchUsersRequest(query=query))
                users = [(u.display_name, u.id) for u in response.users if u.id != self.user_id]
                self.root.after(0, lambda: self._update_users_list(users))
            except Exception as e:
                print(f"Search error: {e}")
                
        if self.loop:
            asyncio.run_coroutine_threadsafe(_search(), self.loop)
            
    def _update_users_list(self, users):
        """Update users listbox"""
        self.users_listbox.delete(0, tk.END)
        for display_name, user_id in users:
            self.users_listbox.insert(tk.END, display_name)
            self.name_cache[display_name] = user_id
    
    def load_user_groups(self):
        """Load groups that the user is a member of"""
        async def _load():
            try:
                response = await self.stub.GetUserGroups(chat_pb2.GetUserGroupsRequest(user_id=self.user_id))
                groups = [g.name for g in response.groups]
                self.root.after(0, lambda: self._update_groups_list(groups))
            except Exception as e:
                print(f"Error loading groups: {e}")
                
        if self.loop:
            asyncio.run_coroutine_threadsafe(_load(), self.loop)
    
    def _update_groups_list(self, groups):
        """Update groups listbox"""
        self.groups_listbox.delete(0, tk.END)
        for group_name in groups:
            self.groups_listbox.insert(tk.END, group_name)
            
    def on_user_select(self, event):
        """Handle user selection from list"""
        selection = self.users_listbox.curselection()
        if selection:
            user_name = self.users_listbox.get(selection[0])
            user_id = self.name_cache.get(user_name)
            if user_id:
                self.current_chat = user_id
                self.current_chat_type = 'dm'
                self.chat_header.config(text=f"Chat with {user_name}")
                self.send_button.config(state=tk.NORMAL)
                self.message_entry.focus()
                
                # Load message history
                self._load_message_history(user_id, False)
                
    def on_group_select(self, event):
        """Handle group selection from list"""
        selection = self.groups_listbox.curselection()
        if selection:
            group_name = self.groups_listbox.get(selection[0])
            self.current_chat = group_name
            self.current_chat_type = 'group'
            self.chat_header.config(text=f"Group: {group_name}")
            self.send_button.config(state=tk.NORMAL)
            self.message_entry.focus()
            
            # Load message history
            self._load_message_history(group_name, True)
    
    def _load_message_history(self, chat_id, is_group):
        """Load message history for a chat"""
        # Clear current chat display
        self.chat_display.config(state=tk.NORMAL)
        self.chat_display.delete(1.0, tk.END)
        self.chat_display.config(state=tk.DISABLED)
        
        async def _load():
            try:
                response = await self.stub.GetMessages(chat_pb2.GetMessagesRequest(
                    user_id=self.user_id,
                    chat_id=chat_id,
                    is_group=is_group,
                    limit=50
                ))
                
                # Display messages in chronological order
                for envelope in response.messages:
                    if envelope.type == chat_pb2.SEND_DM or envelope.type == chat_pb2.SEND_GROUP:
                        if envelope.from_user_id == self.user_id:
                            # Message sent by current user
                            self.root.after(0, lambda env=envelope: 
                                self._display_sent_message(env.text, env.sent_ts))
                        else:
                            # Message from other user
                            sender_name = self._get_user_name(envelope.from_user_id)
                            msg_type = 'group' if is_group else 'dm'
                            self.root.after(0, lambda env=envelope, sn=sender_name, mt=msg_type, cid=chat_id: 
                                self._display_history_message(sn, env.text, env.sent_ts))
                
            except Exception as e:
                print(f"Error loading history: {e}")
                self.root.after(0, lambda: self._display_system_message(f"Error loading history: {str(e)}"))
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(_load(), self.loop)
            
    def show_create_group_dialog(self):
        """Show dialog to create a new group"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Create Group")
        dialog.geometry("300x150")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text="Group Name (must start with #):", font=("Arial", 10)).pack(pady=10, padx=10)
        name_entry = ttk.Entry(dialog, width=30, font=("Arial", 11))
        name_entry.pack(pady=5, padx=10)
        name_entry.insert(0, "#")
        name_entry.focus()
        
        status_label = ttk.Label(dialog, text="", foreground="red")
        status_label.pack(pady=5)
        
        def create():
            group_name = name_entry.get().strip()
            if not group_name.startswith("#"):
                status_label.config(text="Group name must start with #")
                return
                
            async def _create():
                try:
                    response = await self.stub.CreateGroup(chat_pb2.CreateGroupRequest(
                        group_name=group_name,
                        creator_user_id=self.user_id
                    ))
                    if response.success:
                        self.root.after(0, lambda: self._add_group_to_list(group_name))
                        self.root.after(0, dialog.destroy)
                        self.root.after(0, lambda: self._display_system_message(f"Created group {group_name}"))
                    else:
                        self.root.after(0, lambda: status_label.config(text=response.error_message))
                except Exception as e:
                    self.root.after(0, lambda: status_label.config(text=str(e)))
                    
            if self.loop:
                asyncio.run_coroutine_threadsafe(_create(), self.loop)
        
        ttk.Button(dialog, text="Create", command=create).pack(pady=10)
        name_entry.bind('<Return>', lambda e: create())
        
    def show_join_group_dialog(self):
        """Show dialog to join an existing group"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Join Group")
        dialog.geometry("300x150")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text="Group Name:", font=("Arial", 10)).pack(pady=10, padx=10)
        name_entry = ttk.Entry(dialog, width=30, font=("Arial", 11))
        name_entry.pack(pady=5, padx=10)
        name_entry.insert(0, "#")
        name_entry.focus()
        
        status_label = ttk.Label(dialog, text="", foreground="red")
        status_label.pack(pady=5)
        
        def join():
            group_name = name_entry.get().strip()
            if not group_name.startswith("#"):
                status_label.config(text="Group name must start with #")
                return
                
            async def _join():
                try:
                    response = await self.stub.JoinGroup(chat_pb2.JoinGroupRequest(
                        group_name=group_name,
                        user_id=self.user_id
                    ))
                    if response.success:
                        self.root.after(0, lambda: self._add_group_to_list(group_name))
                        self.root.after(0, dialog.destroy)
                        self.root.after(0, lambda: self._display_system_message(f"Joined group {group_name}"))
                    else:
                        self.root.after(0, lambda: status_label.config(text=response.error_message))
                except Exception as e:
                    self.root.after(0, lambda: status_label.config(text=str(e)))
                    
            if self.loop:
                asyncio.run_coroutine_threadsafe(_join(), self.loop)
        
        ttk.Button(dialog, text="Join", command=join).pack(pady=10)
        name_entry.bind('<Return>', lambda e: join())
        
    def _add_group_to_list(self, group_name):
        """Add group to the groups listbox if not already there"""
        items = self.groups_listbox.get(0, tk.END)
        if group_name not in items:
            self.groups_listbox.insert(tk.END, group_name)
            
    def send_message(self):
        """Send a message"""
        if not self.current_chat:
            return
            
        message_text = self.message_entry.get().strip()
        if not message_text:
            return
            
        self.message_entry.delete(0, tk.END)
            
        async def _send():
            try:
                message_id = uuid.uuid4().hex
                current_time = int(time.time() * 1000)
                
                if self.current_chat_type == 'dm':
                    envelope = chat_pb2.ChatEnvelope(
                        type=chat_pb2.SEND_DM,
                        from_user_id=self.user_id,
                        to_user_id=self.current_chat,
                        message_id=message_id,
                        text=message_text,
                        sent_ts=current_time
                    )
                else:  # group
                    envelope = chat_pb2.ChatEnvelope(
                        type=chat_pb2.SEND_GROUP,
                        from_user_id=self.user_id,
                        group_id=self.current_chat,
                        message_id=message_id,
                        text=message_text,
                        sent_ts=current_time
                    )
                
                # Put message in outgoing queue
                await self.outgoing_queue.put(envelope)
                
                # Display locally
                self.root.after(0, lambda: self._display_sent_message(message_text, current_time))
                
            except Exception as e:
                self.root.after(0, lambda: self._display_system_message(f"Error sending: {str(e)}"))
        
        if self.loop:
            asyncio.run_coroutine_threadsafe(_send(), self.loop)
            
    def _display_sent_message(self, text, timestamp):
        """Display a sent message in the chat area"""
        self.chat_display.config(state=tk.NORMAL)
        
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
        self.chat_display.insert(tk.END, f"[{time_str}] ", "timestamp")
        self.chat_display.insert(tk.END, "You: ", "sent")
        self.chat_display.insert(tk.END, f"{text}\n")
        
        self.chat_display.see(tk.END)
        self.chat_display.config(state=tk.DISABLED)
        
    def _display_received_message(self, sender, text, timestamp, msg_type, chat_id):
        """Display a received message in the chat area"""
        # Only display if it's for the current chat
        if (msg_type == 'dm' and self.current_chat == chat_id and self.current_chat_type == 'dm') or \
           (msg_type == 'group' and self.current_chat == chat_id and self.current_chat_type == 'group'):
            
            self.chat_display.config(state=tk.NORMAL)
            
            time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
            self.chat_display.insert(tk.END, f"[{time_str}] ", "timestamp")
            self.chat_display.insert(tk.END, f"{sender}: ", "received")
            self.chat_display.insert(tk.END, f"{text}\n")
            
            self.chat_display.see(tk.END)
            self.chat_display.config(state=tk.DISABLED)
    
    def _display_history_message(self, sender, text, timestamp):
        """Display a historical message in the chat area (used when loading history)"""
        self.chat_display.config(state=tk.NORMAL)
        
        time_str = time.strftime('%H:%M:%S', time.localtime(timestamp / 1000))
        self.chat_display.insert(tk.END, f"[{time_str}] ", "timestamp")
        self.chat_display.insert(tk.END, f"{sender}: ", "received")
        self.chat_display.insert(tk.END, f"{text}\n")
        
        self.chat_display.see(tk.END)
        self.chat_display.config(state=tk.DISABLED)
            
    def _display_system_message(self, text):
        """Display a system message"""
        self.chat_display.config(state=tk.NORMAL)
        self.chat_display.insert(tk.END, f"{text}\n", "system")
        self.chat_display.see(tk.END)
        self.chat_display.config(state=tk.DISABLED)
        
    def logout(self):
        """Handle logout"""
        if self.loop:
            if self.stream_task:
                self.loop.call_soon_threadsafe(self.stream_task.cancel)
            self.loop.call_soon_threadsafe(self.loop.stop)
            
        if self.channel:
            asyncio.run(self.channel.close())
            
        self.chat_frame.destroy()
        self.user_id = None
        self.display_name = None
        self.stub = None
        self.channel = None
        self.stream_task = None
        self.loop = None
        self.current_chat = None
        self.current_chat_type = None
        
        self.create_login_frame()
        
    def on_closing(self):
        """Handle window closing"""
        if self.loop:
            if self.stream_task:
                self.loop.call_soon_threadsafe(self.stream_task.cancel)
            self.loop.call_soon_threadsafe(self.loop.stop)
        self.root.destroy()


def main():
    root = tk.Tk()
    app = ChatGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()


if __name__ == "__main__":
    main()
