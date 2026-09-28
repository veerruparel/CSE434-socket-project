import json
import random
import socket
import sys


# Keeps track of all registered peers
peers = {}

dht_exists = False
dht_complete = False
dht_leader = None
dht_members = []
dht_size = 0
dht_year = None
waiting_for_dht_complete = False


def send_response(manager_socket, address, response):
    manager_socket.sendto(
        json.dumps(response).encode("utf-8"),
        address
    )


def handle_register(manager_socket, message, sender_address):
    peer_name = message.get("peer_name")
    peer_ip = message.get("ip")
    m_port = message.get("m_port")
    p_port = message.get("p_port")

    print(f"[MANAGER] Processing REGISTER for {peer_name}")

    if (
        not isinstance(peer_name, str)
        or not peer_name.isalpha()
        or len(peer_name) > 15
    ):
        print("[MANAGER] REGISTER failed: invalid peer name")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Invalid peer name"
            }
        )
        return

    if not isinstance(peer_ip, str):
        print("[MANAGER] REGISTER failed: invalid IPv4 address")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Invalid IPv4 address"
            }
        )
        return

    try:
        socket.inet_aton(peer_ip)
    except OSError:
        print("[MANAGER] REGISTER failed: invalid IPv4 address")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Invalid IPv4 address"
            }
        )
        return

    if not isinstance(m_port, int) or not isinstance(p_port, int):
        print("[MANAGER] REGISTER failed: invalid port")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Ports must be integers"
            }
        )
        return

    if not (1 <= m_port <= 65535 and 1 <= p_port <= 65535):
        print("[MANAGER] REGISTER failed: port outside valid range")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Port outside valid range"
            }
        )
        return

    if m_port == p_port:
        print("[MANAGER] REGISTER failed: m-port and p-port must be different")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "m-port and p-port must be different"
            }
        )
        return

    # Peer names cannot be registered more than once
    if peer_name in peers:
        print("[MANAGER] REGISTER failed: duplicate peer name")
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Peer name is already registered"
            }
        )
        return

    # Each peer needs its own ports
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
                {
                    "status": "FAILURE",
                    "message": "Port is already in use by another peer"
                }
            )
            return

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
        {
            "status": "SUCCESS",
            "message": f"{peer_name} registered successfully"
        }
    )


def handle_setup_dht(manager_socket, message, sender_address):
    global dht_exists
    global dht_complete
    global dht_leader
    global dht_members
    global dht_size
    global dht_year
    global waiting_for_dht_complete

    peer_name = message.get("peer_name")
    n = message.get("n")
    year = message.get("year")

    print(
        f"[MANAGER] Processing SETUP-DHT from {peer_name}: "
        f"n={n}, year={year}"
    )

    if peer_name not in peers:
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Peer is not registered"
            }
        )
        return

    if not isinstance(n, int) or n < 3:
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "DHT size must be at least 3"
            }
        )
        return

    if len(peers) < n:
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Not enough registered peers"
            }
        )
        return

    if dht_exists:
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "A DHT already exists"
            }
        )
        return

    free_peers = [
        name
        for name, info in peers.items()
        if name != peer_name and info["state"] == "Free"
    ]

    if len(free_peers) < n - 1:
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Not enough Free peers"
            }
        )
        return

    # Pick the other peers that will be part of the DHT
    selected_names = random.sample(free_peers, n - 1)

    peers[peer_name]["state"] = "Leader"

    for name in selected_names:
        peers[name]["state"] = "InDHT"

    dht_leader = peer_name
    dht_members = [peer_name] + selected_names
    dht_size = n
    dht_year = year
    dht_exists = True
    dht_complete = False
    waiting_for_dht_complete = True

    selected_peers = []

    for name in dht_members:
        info = peers[name]

        selected_peers.append(
            {
                "peer_name": name,
                "ip": info["ip"],
                "p_port": info["p_port"]
            }
        )

    print("[MANAGER] SETUP-DHT successful")
    print(f"[MANAGER] Leader: {dht_leader}")
    print(f"[MANAGER] DHT members: {dht_members}")

    for name in dht_members:
        print(f"[MANAGER] {name}: {peers[name]['state']}")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "DHT peer selection successful",
            "n": n,
            "year": year,
            "peers": selected_peers
        }
    )


def handle_dht_complete(manager_socket, message, sender_address):
    global dht_complete
    global waiting_for_dht_complete

    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing DHT-COMPLETE from {peer_name}")

    if peer_name != dht_leader:
        print("[MANAGER] DHT-COMPLETE failed: sender is not leader")

        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Peer is not the DHT leader"
            }
        )
        return

    dht_complete = True
    waiting_for_dht_complete = False

    print("[MANAGER] DHT construction complete")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "DHT construction complete"
        }
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

    if not (1 <= manager_port <= 65535):
        print("Error: port must be between 1 and 65535.")
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
                send_response(
                    manager_socket,
                    sender_address,
                    {
                        "status": "FAILURE",
                        "message": "Invalid message format"
                    }
                )
                continue

            command = message.get("command")

            print(
                f"[MANAGER] Received {command} from "
                f"{sender_address[0]}:{sender_address[1]}"
            )

            # Manager waits for setup to finish before accepting other commands
            if waiting_for_dht_complete and command != "dht-complete":
                print(
                    f"[MANAGER] Rejecting {command}: "
                    "waiting for DHT-COMPLETE"
                )

                send_response(
                    manager_socket,
                    sender_address,
                    {
                        "status": "FAILURE",
                        "message": "DHT construction is in progress"
                    }
                )
                continue

            if command == "register":
                handle_register(
                    manager_socket,
                    message,
                    sender_address
                )

            elif command == "setup-dht":
                handle_setup_dht(
                    manager_socket,
                    message,
                    sender_address
                )

            elif command == "dht-complete":
                handle_dht_complete(
                    manager_socket,
                    message,
                    sender_address
                )

            else:
                send_response(
                    manager_socket,
                    sender_address,
                    {
                        "status": "FAILURE",
                        "message": "Unknown command"
                    }
                )

    except KeyboardInterrupt:
        print("\n[MANAGER] Manager stopped.")

    finally:
        manager_socket.close()


if __name__ == "__main__":
    main()