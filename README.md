🔌 Python Client-Server Application with User Management and Messaging
This project is a socket-based client-server application written in Python, designed for local TCP communication. Beyond basic command processing, it introduces user authentication and a simple private messaging system, simulating a minimalistic chat environment.

🧠 Key Features
TCP socket communication over localhost (127.0.0.1)

User system with registration, login, and session handling

Data persistence using PostgreSQL (previously JSON-based)

Private messaging between authenticated users

Command interface with built-in server commands:

help – display available commands

uptime – show how long the server has been running

info – return server version

stop – safely shut down the server

⚙️ How It Works
Clients connect to the server and authenticate using a simple text-based protocol. After logging in, users can issue commands or send/receive private messages.

All user credentials, sessions, and messages are now persisted in a PostgreSQL database (users, messages tables), allowing more robust data integrity and testability.

🧪 Testing
This project includes a suite of automated tests written using Python’s built-in unittest framework.

Test coverage includes:

Core user operations (registration, login, logout)

Authentication and session handling

Password change functionality

Message sending, reading, and deletion

Admin operations (viewing and deleting messages as admin)

Integration scenarios (e.g. full user workflows)

✅ Tests run against a dedicated PostgreSQL test database (test_mailbox), isolated from production data.
All data is reset before each test using SQL TRUNCATE statements and reloaded from JSON fixtures to ensure consistent, repeatable test runs.# L103-Connection-Pool
