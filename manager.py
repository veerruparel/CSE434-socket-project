import json
import socket
import sys


# Stores information about every registered peer.
peers = {}


def send_response(manager_socket, address, status, message):
    """Send a JSON response to a peer."""
    response = {
        "status": status,
        "message": message
    }

    manager_socket.sendto(
        json.dumps(response).encode("utf-8"),
        address
    )


def handle_register(manager_socket, message, sender_address):
    """Process a register request from a peer."""

    peer_name = message.get("peer_name")
    peer_ip = message.get("ip")
    m_port = message.get("m_port")
    p_port = message.get("p_port")

    print(f"[MANAGER] Processing REGISTER for {peer_name}")

    # Peer names must be alphabetic and at most 15 characters.
    if (
        not isinstance(peer_name, str)
        or not peer_name.isalpha()
        or len(peer_name) > 15
    ):
        print("[MANAGER] REGISTER failed: invalid peer name")
        send_response(
            manager_socket,
            sender_address,
            "FAILURE",
            "Invalid peer name"
        )
        return

    # A peer name may only be registered once.
    if peer_name in peers:
        print("[MANAGER] REGISTER failed: duplicate peer name")
        send_response(
            manager_socket,
            sender_address,
            "FAILURE",
            "Peer name is already registered"
        )
        return

    # Ports used by peer processes must be unique.
    for existing_peer in peers.values():
        existing_ports = {
            existing_peer["m_port"],
            existing_peer["p_port"]
        }

        if m_port in existing_ports or p_port in existing_ports:
            print("[MANAGER] REGISTER failed: duplicate port")
            send_response(
                manager_socket,
                sender_address,
                "FAILURE",
                "Port is already in use by another peer"
            )
            return

    # m-port and p-port must also be different from each other.
    if m_port == p_port:
        print("[MANAGER] REGISTER failed: m-port and p-port are identical")
        send_response(
            manager_socket,
            sender_address,
            "FAILURE",
            "m-port and p-port must be different"
        )
        return

    # Store the new peer.
    peers[peer_name] = {
        "ip": peer_ip,
        "m_port": m_port,
        "p_port": p_port,
        "state": "Free"
    }

    print(f"[MANAGER] REGISTER successful: {peer_name} -> Free")
    print(f"[MANAGER] Registered peers: {list(peers.keys())}")

    send_response(
        manager_socket,
        sender_address,
        "SUCCESS",
        f"{peer_name} registered successfully"
    )


def main():
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <port>")
        sys.exit(1)

    try:
        manager_port = int(sys.argv[1])
    except ValueError:
        print("Error: port must be an integer.")
        sys.exit(1)

    manager_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    try:
        manager_socket.bind(("", manager_port))
    except OSError as error:
        print(f"Error binding manager to port {manager_port}: {error}")
        manager_socket.close()
        sys.exit(1)

    print(f"[MANAGER] Started on UDP port {manager_port}")
    print("[MANAGER] Waiting for messages...")

    try:
        while True:
            data, sender_address = manager_socket.recvfrom(65535)

            try:
                message = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                print("[MANAGER] Received invalid message")
                send_response(
                    manager_socket,
                    sender_address,
                    "FAILURE",
                    "Invalid message format"
                )
                continue

            command = message.get("command")

            print(
                f"[MANAGER] Received {command} from "
                f"{sender_address[0]}:{sender_address[1]}"
            )

            if command == "register":
                handle_register(
                    manager_socket,
                    message,
                    sender_address
                )
            else:
                print(f"[MANAGER] Unknown command: {command}")
                send_response(
                    manager_socket,
                    sender_address,
                    "FAILURE",
                    "Unknown command"
                )

    except KeyboardInterrupt:
        print("\n[MANAGER] Manager stopped.")

    finally:
        manager_socket.close()


if __name__ == "__main__":
    main()