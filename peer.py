import json
import socket
import sys
import threading


def send_p2p_message(p_socket, peer, message):
    p_socket.sendto(
        json.dumps(message).encode("utf-8"),
        (peer["ip"], peer["p_port"])
    )


def configure_right_neighbor(state):
    if state["id"] is None or not state["dht_peers"]:
        return

    next_id = (state["id"] + 1) % state["ring_size"]
    state["right_neighbor"] = state["dht_peers"][next_id]


def handle_set_id(message, state):
    assigned_id = message.get("id")
    ring_size = message.get("ring_size")
    dht_peers = message.get("peers", [])
    year = message.get("year")

    state["id"] = assigned_id
    state["ring_size"] = ring_size
    state["dht_peers"] = dht_peers
    state["year"] = year

    configure_right_neighbor(state)

    right_neighbor = state["right_neighbor"]

    print(
        f"[PEER {state['peer_name']}] SET-ID received"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"Assigned ID {state['id']}"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"Ring size: {state['ring_size']}"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"Right neighbor: "
        f"{right_neighbor['peer_name']} "
        f"{right_neighbor['ip']}:{right_neighbor['p_port']}"
    )


def listen_for_peer_messages(p_socket, state):
    while True:
        try:
            data, sender_address = p_socket.recvfrom(65535)

            try:
                message = json.loads(data.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(
                    f"\n[PEER {state['peer_name']}] "
                    "Received invalid P2P message"
                )
                continue

            command = message.get("command")

            print(
                f"\n[PEER {state['peer_name']}] "
                f"Received P2P command '{command}' from "
                f"{sender_address[0]}:{sender_address[1]}"
            )

            if command == "set-id":
                handle_set_id(message, state)

            else:
                print(
                    f"[PEER {state['peer_name']}] "
                    f"Unknown P2P command: {command}"
                )

        except OSError:
            break


def send_manager_request(m_socket, manager_address, message):
    m_socket.sendto(
        json.dumps(message).encode("utf-8"),
        manager_address
    )

    data, _ = m_socket.recvfrom(65535)

    try:
        return json.loads(data.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return {
            "status": "FAILURE",
            "message": "Invalid response from manager"
        }


def register_peer(
    m_socket,
    manager_address,
    peer_name,
    peer_ip,
    m_port,
    p_port
):
    print(
        f"[PEER {peer_name}] "
        "Sending REGISTER to manager"
    )

    response = send_manager_request(
        m_socket,
        manager_address,
        {
            "command": "register",
            "peer_name": peer_name,
            "ip": peer_ip,
            "m_port": m_port,
            "p_port": p_port
        }
    )

    status = response.get("status", "FAILURE")

    print(
        f"[PEER {peer_name}] "
        f"REGISTER response: {status}"
    )

    print(
        f"[PEER {peer_name}] "
        f"{response.get('message', '')}"
    )

    return status


def send_set_id_messages(p_socket, state):
    for assigned_id in range(1, state["ring_size"]):
        peer = state["dht_peers"][assigned_id]

        message = {
            "command": "set-id",
            "id": assigned_id,
            "ring_size": state["ring_size"],
            "year": state["year"],
            "peers": state["dht_peers"]
        }

        print(
            f"[PEER {state['peer_name']}] "
            f"Sending SET-ID {assigned_id} "
            f"to {peer['peer_name']}"
        )

        send_p2p_message(
            p_socket,
            peer,
            message
        )


def setup_dht(
    m_socket,
    p_socket,
    manager_address,
    state,
    requested_peer_name,
    n,
    year
):
    if not state["registered"]:
        print("[PEER] This peer is not registered.")
        return

    if requested_peer_name != state["peer_name"]:
        print(
            f"[PEER {state['peer_name']}] "
            "SETUP-DHT peer name must match this peer."
        )
        return

    print(
        f"[PEER {state['peer_name']}] "
        f"Sending SETUP-DHT n={n}, year={year}"
    )

    response = send_manager_request(
        m_socket,
        manager_address,
        {
            "command": "setup-dht",
            "peer_name": requested_peer_name,
            "n": n,
            "year": year
        }
    )

    status = response.get("status", "FAILURE")

    print(
        f"[PEER {state['peer_name']}] "
        f"SETUP-DHT response: {status}"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"{response.get('message', '')}"
    )

    if status != "SUCCESS":
        return

    state["id"] = 0
    state["ring_size"] = response.get("n")
    state["year"] = response.get("year")
    state["dht_peers"] = response.get("peers", [])

    configure_right_neighbor(state)

    print(
        f"[PEER {state['peer_name']}] "
        "Selected DHT peers:"
    )

    for index, peer in enumerate(state["dht_peers"]):
        print(
            f"  ID {index}: "
            f"{peer['peer_name']} "
            f"{peer['ip']}:{peer['p_port']}"
        )

    print(
        f"[PEER {state['peer_name']}] "
        "Assigned leader ID 0"
    )

    right_neighbor = state["right_neighbor"]

    print(
        f"[PEER {state['peer_name']}] "
        f"Right neighbor: "
        f"{right_neighbor['peer_name']} "
        f"{right_neighbor['ip']}:{right_neighbor['p_port']}"
    )

    send_set_id_messages(
        p_socket,
        state
    )


def main():
    if len(sys.argv) != 3:
        print(
            "Usage: python3 peer.py "
            "<manager-ip> <manager-port>"
        )
        sys.exit(1)

    manager_ip = sys.argv[1]

    try:
        manager_port = int(sys.argv[2])
    except ValueError:
        print(
            "Error: manager port must be an integer."
        )
        sys.exit(1)

    if not (1 <= manager_port <= 65535):
        print(
            "Error: manager port must be "
            "between 1 and 65535."
        )
        sys.exit(1)

    manager_address = (
        manager_ip,
        manager_port
    )

    state = {
        "registered": False,
        "peer_name": None,
        "peer_ip": None,
        "m_port": None,
        "p_port": None,
        "id": None,
        "ring_size": None,
        "year": None,
        "dht_peers": [],
        "right_neighbor": None,
        "local_hash_table": {}
    }

    m_socket = None
    p_socket = None

    print("[PEER] Peer started")
    print(
        f"[PEER] Manager address: "
        f"{manager_ip}:{manager_port}"
    )

    try:
        while True:
            command_line = input("peer> ").strip()

            if not command_line:
                continue

            if command_line.lower() == "exit":
                print("[PEER] Peer stopped.")
                break

            parts = command_line.split()
            command = parts[0].lower()

            if command == "register":
                if state["registered"]:
                    print(
                        "[PEER] This peer process "
                        "is already registered."
                    )
                    continue

                if len(parts) != 5:
                    print(
                        "Usage: register "
                        "<peer-name> <IPv4-address> "
                        "<m-port> <p-port>"
                    )
                    continue

                candidate_name = parts[1]
                candidate_ip = parts[2]

                try:
                    candidate_m_port = int(parts[3])
                    candidate_p_port = int(parts[4])
                except ValueError:
                    print(
                        "[PEER] m-port and p-port "
                        "must be integers."
                    )
                    continue

                try:
                    m_socket = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_DGRAM
                    )

                    m_socket.bind(
                        ("", candidate_m_port)
                    )

                    p_socket = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_DGRAM
                    )

                    p_socket.bind(
                        ("", candidate_p_port)
                    )

                except OSError as error:
                    print(
                        "[PEER] Could not bind "
                        f"communication ports: {error}"
                    )

                    if m_socket is not None:
                        m_socket.close()

                    if p_socket is not None:
                        p_socket.close()

                    m_socket = None
                    p_socket = None
                    continue

                status = register_peer(
                    m_socket,
                    manager_address,
                    candidate_name,
                    candidate_ip,
                    candidate_m_port,
                    candidate_p_port
                )

                if status == "SUCCESS":
                    state["registered"] = True
                    state["peer_name"] = candidate_name
                    state["peer_ip"] = candidate_ip
                    state["m_port"] = candidate_m_port
                    state["p_port"] = candidate_p_port

                    listener_thread = threading.Thread(
                        target=listen_for_peer_messages,
                        args=(p_socket, state),
                        daemon=True
                    )

                    listener_thread.start()

                    print(
                        f"[PEER {state['peer_name']}] "
                        f"Listening on m-port "
                        f"{state['m_port']}"
                    )

                    print(
                        f"[PEER {state['peer_name']}] "
                        f"Listening for peers on "
                        f"p-port {state['p_port']}"
                    )

                else:
                    m_socket.close()
                    p_socket.close()

                    m_socket = None
                    p_socket = None

            elif command == "setup-dht":
                if len(parts) != 4:
                    print(
                        "Usage: setup-dht "
                        "<peer-name> <n> <YYYY>"
                    )
                    continue

                try:
                    n = int(parts[2])
                    year = int(parts[3])
                except ValueError:
                    print(
                        "[PEER] n and YYYY "
                        "must be integers."
                    )
                    continue

                setup_dht(
                    m_socket,
                    p_socket,
                    manager_address,
                    state,
                    parts[1],
                    n,
                    year
                )

            else:
                print(
                    f"[PEER] Unknown command: "
                    f"{command}"
                )

    except KeyboardInterrupt:
        print("\n[PEER] Peer stopped.")

    finally:
        if m_socket is not None:
            m_socket.close()

        if p_socket is not None:
            p_socket.close()


if __name__ == "__main__":
    main()