import json
import socket
import sys


def register_peer(
    peer_socket,
    manager_address,
    peer_name,
    peer_ip,
    m_port,
    p_port
):
    """Send a register request to the manager."""

    message = {
        "command": "register",
        "peer_name": peer_name,
        "ip": peer_ip,
        "m_port": m_port,
        "p_port": p_port
    }

    print(f"[PEER {peer_name}] Sending REGISTER to manager")

    peer_socket.sendto(
        json.dumps(message).encode("utf-8"),
        manager_address
    )

    # Manager commands use request/response pairs.
    data, _ = peer_socket.recvfrom(65535)

    response = json.loads(data.decode("utf-8"))

    print(
        f"[PEER {peer_name}] REGISTER response: "
        f"{response['status']}"
    )

    print(f"[PEER {peer_name}] {response['message']}")


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer.py <manager-ip> <manager-port>")
        sys.exit(1)

    manager_ip = sys.argv[1]

    try:
        manager_port = int(sys.argv[2])
    except ValueError:
        print("Error: manager port must be an integer.")
        sys.exit(1)

    manager_address = (manager_ip, manager_port)

    peer_socket = socket.socket(
        socket.AF_INET,
        socket.SOCK_DGRAM
    )

    print("[PEER] Peer started")
    print(f"[PEER] Manager address: {manager_ip}:{manager_port}")

    try:
        while True:
            command_line = input("peer> ").strip()

            if not command_line:
                continue

            if command_line.lower() == "exit":
                print("[PEER] Peer stopped.")
                break

            parts = command_line.split()

            if parts[0] == "register":
                if len(parts) != 5:
                    print(
                        "Usage: register "
                        "<peer-name> <IPv4-address> "
                        "<m-port> <p-port>"
                    )
                    continue

                peer_name = parts[1]
                peer_ip = parts[2]

                try:
                    m_port = int(parts[3])
                    p_port = int(parts[4])
                except ValueError:
                    print("[PEER] m-port and p-port must be integers.")
                    continue

                register_peer(
                    peer_socket,
                    manager_address,
                    peer_name,
                    peer_ip,
                    m_port,
                    p_port
                )

            else:
                print(f"[PEER] Unknown command: {parts[0]}")

    except KeyboardInterrupt:
        print("\n[PEER] Peer stopped.")

    finally:
        peer_socket.close()


if __name__ == "__main__":
    main()