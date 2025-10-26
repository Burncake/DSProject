# Chat Application (gRPC)

Ứng dụng chat phân tán sử dụng gRPC, hỗ trợ chat 1-1 và chat nhóm với khả năng gửi tin nhắn offline.

## Tính năng

### 1. Chat 1-1 (Direct Messages)
- Gửi và nhận tin nhắn trực tiếp giữa hai người dùng
- Hỗ trợ tin nhắn offline (người nhận sẽ nhận được tin khi online)
- Theo dõi trạng thái đã gửi của tin nhắn

### 2. Chat Nhóm (Group Chat)
- Tạo nhóm chat mới
- Thêm thành viên vào nhóm
- Gửi tin nhắn đến toàn bộ thành viên nhóm
- Hỗ trợ tin nhắn offline cho các thành viên không online

### 3. Quản lý người dùng
- Đăng ký người dùng mới
- Tìm kiếm người dùng theo tên hiển thị
- Xem danh sách người dùng

## Cài đặt và Chạy

### Yêu cầu
- Python 3.7 trở lên
- gRPC và các dependencies (được liệt kê trong requirements.txt)

### Cài đặt Dependencies
```bash
pip install grpcio grpcio-tools
```

### Chạy Server
```bash
cd project1/DSProject
python -m chatapp.server.main
```
Server sẽ chạy tại địa chỉ: `127.0.0.1:50051`

### Chạy Client
```bash
cd project1/DSProject
python -m chatapp.client.cli
```

## Sử dụng Client

### Chat 1-1
1. Đăng ký tài khoản:
```
/register <display_name>
```
Ví dụ: `/register Alice`

2. Gửi tin nhắn tới người dùng khác:
```
/dm <display_name> <message>
```
Ví dụ: `/dm Bob Hello Bob!`

### Chat Nhóm
1. Tạo nhóm mới:
```
/create_group <group_name>
```
Ví dụ: `/create_group #general`
(Lưu ý: Tên nhóm phải bắt đầu bằng #)

2. Thêm thành viên vào nhóm:
```
/add_member <group_name> <display_name>
```
Ví dụ: `/add_member #general Bob`

3. Gửi tin nhắn đến nhóm:
```
/group_msg <group_name> <message>
```
Ví dụ: `/group_msg #general Hello everyone!`

### Các lệnh khác
- `/help` - Hiển thị danh sách các lệnh
- `/list_users` - Xem danh sách người dùng
- `/list_groups` - Xem danh sách nhóm bạn tham gia

## Cấu trúc dự án
```
chatapp/
├── __init__.py
├── client/
│   ├── __init__.py
│   └── cli.py           # Command-line interface cho client
├── data/
│   ├── groups.jsonl     # Lưu trữ thông tin nhóm
│   ├── messages.jsonl   # Lưu trữ tin nhắn
│   └── users.jsonl      # Lưu trữ thông tin người dùng
├── proto/
│   ├── __init__.py
│   ├── chat.proto      # Định nghĩa gRPC service
│   ├── chat_pb2.py     # Generated Python code
│   └── chat_pb2_grpc.py
└── server/
    ├── __init__.py
    ├── hub.py          # Quản lý kết nối client
    ├── main.py         # Entry point của server
    ├── models.py       # Định nghĩa data models
    ├── repo.py         # Data persistence
    └── service.py      # Implement gRPC service
```

## Lưu trữ dữ liệu
- Dữ liệu được lưu trong các file JSONL trong thư mục `data/`
- Mỗi dòng trong file là một JSON object
- Hỗ trợ persistence và có thể khôi phục sau khi restart server

## Xử lý lỗi
- Server sẽ giữ các tin nhắn cho người dùng offline
- Tự động gửi lại tin nhắn khi người dùng online
- Theo dõi trạng thái gửi của mỗi tin nhắn

## Đóng góp
Nếu bạn muốn đóng góp vào dự án:
1. Fork repository
2. Tạo branch mới
3. Commit changes
4. Tạo Pull Request

## License
[MIT License](LICENSE)
