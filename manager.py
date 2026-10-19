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

# Set while the manager waits for dht-rebuilt or teardown-complete
pending_command = None
pending_peer = None
pending_op = None


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

    if peers[peer_name]["state"] != "Free":
        send_response(
            manager_socket,
            sender_address,
            {
                "status": "FAILURE",
                "message": "Peer must be Free to set up a DHT"
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


def failure(manager_socket, sender_address, text):
    print(f"[MANAGER] FAILURE: {text}")
    send_response(
        manager_socket,
        sender_address,
        {
            "status": "FAILURE",
            "message": text
        }
    )


def peer_tuple(name):
    return {
        "peer_name": name,
        "ip": peers[name]["ip"],
        "p_port": peers[name]["p_port"]
    }


def refresh_states():
    # Leader and InDHT follow the member list, everyone else is Free
    for name in dht_members:
        peers[name]["state"] = "InDHT"

    if dht_leader in peers:
        peers[dht_leader]["state"] = "Leader"


def clear_dht():
    global dht_exists
    global dht_complete
    global dht_leader
    global dht_members
    global dht_size
    global dht_year

    for name in dht_members:
        if name in peers:
            peers[name]["state"] = "Free"

    dht_exists = False
    dht_complete = False
    dht_leader = None
    dht_members = []
    dht_size = 0
    dht_year = None


def handle_query_dht(manager_socket, message, sender_address):
    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing QUERY-DHT from {peer_name}")

    if not dht_exists or not dht_complete:
        failure(manager_socket, sender_address, "DHT is not set up")
        return

    if peer_name not in peers:
        failure(manager_socket, sender_address, "Peer is not registered")
        return

    if peers[peer_name]["state"] != "Free":
        failure(manager_socket, sender_address, "Peer is not Free")
        return

    # Any peer in the DHT can start the query
    target = random.choice(dht_members)

    print(f"[MANAGER] QUERY-DHT successful: start at {target}")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "DHT peer selected",
            "peer": peer_tuple(target)
        }
    )


def handle_leave_dht(manager_socket, message, sender_address):
    global pending_command
    global pending_peer
    global pending_op

    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing LEAVE-DHT from {peer_name}")

    if not dht_exists or not dht_complete:
        failure(manager_socket, sender_address, "DHT does not exist")
        return

    if peer_name not in dht_members:
        failure(manager_socket, sender_address, "Peer is not in the DHT")
        return

    # A ring needs at least two peers to stay a ring
    if len(dht_members) <= 2:
        failure(manager_socket, sender_address, "DHT is too small to leave; use teardown-dht")
        return

    pending_command = "dht-rebuilt"
    pending_peer = peer_name
    pending_op = "leave"

    print(f"[MANAGER] LEAVE-DHT accepted, waiting for DHT-REBUILT from {peer_name}")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "Leave accepted"
        }
    )


def handle_join_dht(manager_socket, message, sender_address):
    global pending_command
    global pending_peer
    global pending_op

    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing JOIN-DHT from {peer_name}")

    if not dht_exists or not dht_complete:
        failure(manager_socket, sender_address, "DHT does not exist")
        return

    if peer_name not in peers:
        failure(manager_socket, sender_address, "Peer is not registered")
        return

    if peers[peer_name]["state"] != "Free":
        failure(manager_socket, sender_address, "Peer is not Free")
        return

    pending_command = "dht-rebuilt"
    pending_peer = peer_name
    pending_op = "join"

    print(f"[MANAGER] JOIN-DHT accepted, waiting for DHT-REBUILT from {peer_name}")

    # The joining peer contacts the leader to start the rebuild
    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "Join accepted",
            "leader": peer_tuple(dht_leader)
        }
    )


def handle_dht_rebuilt(manager_socket, message, sender_address):
    global dht_leader
    global dht_members
    global dht_size
    global pending_command
    global pending_peer
    global pending_op

    peer_name = message.get("peer_name")
    new_leader = message.get("new_leader")

    print(f"[MANAGER] Processing DHT-REBUILT from {peer_name}, new leader {new_leader}")

    if pending_command != "dht-rebuilt" or peer_name != pending_peer:
        failure(manager_socket, sender_address, "Peer did not start the rebuild")
        return

    if pending_op == "leave":
        new_members = [name for name in dht_members if name != peer_name]
    else:
        new_members = dht_members + [peer_name]

    if new_leader not in new_members:
        failure(manager_socket, sender_address, "New leader is not in the DHT")
        return

    if pending_op == "leave":
        peers[peer_name]["state"] = "Free"

    dht_members = new_members
    dht_leader = new_leader
    dht_size = len(dht_members)
    refresh_states()

    pending_command = None
    pending_peer = None
    pending_op = None

    print(f"[MANAGER] DHT rebuilt: leader {dht_leader}, members {dht_members}")

    for name in peers:
        print(f"[MANAGER] {name}: {peers[name]['state']}")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "DHT rebuilt"
        }
    )


def handle_teardown_dht(manager_socket, message, sender_address):
    global pending_command
    global pending_peer
    global pending_op

    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing TEARDOWN-DHT from {peer_name}")

    if not dht_exists or not dht_complete:
        failure(manager_socket, sender_address, "DHT does not exist")
        return

    if peer_name != dht_leader:
        failure(manager_socket, sender_address, "Peer is not the DHT leader")
        return

    pending_command = "teardown-complete"
    pending_peer = peer_name
    pending_op = None

    print("[MANAGER] TEARDOWN-DHT accepted, waiting for TEARDOWN-COMPLETE")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "Teardown accepted"
        }
    )


def handle_teardown_complete(manager_socket, message, sender_address):
    global pending_command
    global pending_peer

    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing TEARDOWN-COMPLETE from {peer_name}")

    if pending_command != "teardown-complete" or peer_name != pending_peer:
        failure(manager_socket, sender_address, "Peer is not the DHT leader")
        return

    clear_dht()

    pending_command = None
    pending_peer = None

    print("[MANAGER] DHT deleted, all DHT peers are Free")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": "DHT deleted"
        }
    )


def handle_deregister(manager_socket, message, sender_address):
    peer_name = message.get("peer_name")

    print(f"[MANAGER] Processing DEREGISTER from {peer_name}")

    if peer_name not in peers:
        failure(manager_socket, sender_address, "Peer is not registered")
        return

    if peers[peer_name]["state"] != "Free":
        failure(manager_socket, sender_address, "Peer is in the DHT")
        return

    del peers[peer_name]

    print(f"[MANAGER] {peer_name} deregistered")
    print(f"[MANAGER] Registered peers: {list(peers.keys())}")

    send_response(
        manager_socket,
        sender_address,
        {
            "status": "SUCCESS",
            "message": f"{peer_name} deregistered"
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

            # Same idea while the DHT is being rebuilt or deleted
            if pending_command is not None and command != pending_command:
                print(
                    f"[MANAGER] Rejecting {command}: "
                    f"waiting for {pending_command.upper()}"
                )

                send_response(
                    manager_socket,
                    sender_address,
                    {
                        "status": "FAILURE",
                        "message": f"Waiting for {pending_command}"
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

            elif command == "query-dht":
                handle_query_dht(manager_socket, message, sender_address)

            elif command == "leave-dht":
                handle_leave_dht(manager_socket, message, sender_address)

            elif command == "join-dht":
                handle_join_dht(manager_socket, message, sender_address)

            elif command == "dht-rebuilt":
                handle_dht_rebuilt(manager_socket, message, sender_address)

            elif command == "teardown-dht":
                handle_teardown_dht(manager_socket, message, sender_address)

            elif command == "teardown-complete":
                handle_teardown_complete(manager_socket, message, sender_address)

            elif command == "deregister":
                handle_deregister(manager_socket, message, sender_address)

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