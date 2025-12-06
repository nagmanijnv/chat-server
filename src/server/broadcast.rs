use tokio::sync::mpsc;
use tracing::info;

use crate::server::message::format_message;

use super::connection::ClientsChannelSenderMap;

// Broadcast message to all the connected clients
pub async fn broadcast_msg(clients_map: &ClientsChannelSenderMap, msg: String, client_id: usize) {
    let clients = clients_map.lock().await;

    for (id, tx) in clients.iter() {
        // skip the client for broadcasting, who sent the message
        if id != &client_id {
            info!("Broadcatsing to {id}");
            let _ = tx.send(msg.clone()).await;
        }
    }
}

// Remove client and broadcast the message
pub async fn remove_client_and_broadcast(client_id: usize, clients_map: &ClientsChannelSenderMap) {
    {
        let mut clients = clients_map.lock().await;
        clients.remove(&client_id);
    }

    // Notify all clients
    broadcast_msg(
        clients_map,
        format_message("left", client_id),
        client_id,
    )
    .await;
}

// Add new client and broadcast the message
pub async fn add_client_and_broadcast(
    clients_map: &ClientsChannelSenderMap,
    client_id: usize,
    tx: mpsc::Sender<String>,
) {
    // Add client to map
    {
        let mut clients = clients_map.lock().await;
        clients.insert(client_id, tx);
    }

    // Notify all clients
    broadcast_msg(
        &clients_map,
        format_message("joined", client_id),
        client_id,
    )
    .await;
}
