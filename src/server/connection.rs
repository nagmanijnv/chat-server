use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, tcp};
use tokio::sync::{Mutex, mpsc};
use tracing::{error, info};

use crate::server::message::format_message;

use super::broadcast::{add_client_and_broadcast, broadcast_msg, remove_client_and_broadcast};

pub type ClientsChannelSenderMap = Arc<Mutex<HashMap<usize, mpsc::Sender<String>>>>;

pub async fn connection_listener(address: String) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(&address).await?;
    info!("Server started on address: {}", address);

    let clients: ClientsChannelSenderMap = Arc::new(Mutex::new(HashMap::new()));

    // This is just incrementing the ids, irrespective of clients left the chat
    // It will always be incremental
    let mut client_id_counter: usize = 0;

    loop {
        // Accepting new connection
        let (socket, _) = listener.accept().await?;
        client_id_counter += 1;
        let client_id = client_id_counter;

        // Split the socket stream
        let (reader, writer) = socket.into_split();

        // Channel to send and receive messages
        let (tx, rx) = mpsc::channel::<String>(100);
        info!("Client {} connected", client_id);

        // Add client to list
        add_client_and_broadcast(&clients, client_id, tx).await;

        // Writer thread
        let writer_clients_map = clients.clone();
        tokio::spawn(writer_handler(writer, rx, writer_clients_map, client_id));

        // Reader thread
        let reader_clients_map = clients.clone();
        tokio::spawn(reader_handler(reader, reader_clients_map, client_id));
    }
}

/// Receives message from channel
/// Write message to tcp writer stream
/// If any error, remove client from clients map as it is disconnected
async fn writer_handler(
    // tcp writer stream
    mut writer: tcp::OwnedWriteHalf,
    // message receiver
    mut rx: mpsc::Receiver<String>,
    // list of clients already connected
    clients_map: ClientsChannelSenderMap,
    // client_id of the writer, where server will write the message
    client_id: usize,
) {
    while let Some(msg) = rx.recv().await {
        if let Err(err) = writer.write_all(msg.as_bytes()).await {
            error!("Write error to client {}: {}", client_id, err);

            remove_client_and_broadcast(client_id, &clients_map).await;
            break;
        }
        let _ = writer.write_all(b"\n").await;
    }
}

async fn reader_handler(
    // tcp read stream
    reader: tcp::OwnedReadHalf,
    // list of clients already connected
    clients_map: ClientsChannelSenderMap,
    // sender id => client_id of the sender
    client_id: usize,
) {
    let mut reader = BufReader::new(reader);
    let mut buf = String::new();
    loop {
        buf.clear();

        // Get number of bytes read
        let n = match reader.read_line(&mut buf).await {
            Ok(n) => n,
            Err(err) => {
                error!("Error reading from client {}: {}", client_id, err);

                remove_client_and_broadcast(client_id, &clients_map).await;
                break;
            }
        };

        // Client disconnected -> remove from map
        if n == 0 {
            info!("Client {} disconnected.", client_id);

            remove_client_and_broadcast(client_id, &clients_map).await;
            break;
        }

        info!("Received from {client_id}: {}", buf.trim());
        let msg = format_message(buf.trim(), client_id);
        broadcast_msg(&clients_map, msg, client_id).await;
    }
}
