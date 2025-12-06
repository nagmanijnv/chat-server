# Rust TCP Chat Server

A simple **asynchronous TCP chat server** written in Rust using **Tokio**.  
Listens for a TCP chat client on specific socket address.
Supports multiple clients, broadcasts messages, and handles client join/leave events.

---

## Features

- Fully Asynchronous using **Tokio**
- Uses `HashMap` + `mpsc` channels with Mutex to protect race conditions
- Separate tokio tasks for reading from server and writing to server
- Broadcast messages to all connected client
- Graceful handling of client disconnections
- Timestamped messages
- Configurable server port via command-line argument

---

## Build & Run Commands
Commands to run and build the tcp chat server

### Build Using:
```bash
cargo build
```

### Run Using: 
```bash
cargo run <port>
```