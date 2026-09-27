import socket
import sys


def main():
    # The manager requires exactly one command-line argument:
    # the UDP port on which it will listen.
    if len(sys.argv) != 2:
        print("Usage: python3 manager.py <port>")
        sys.exit(1)

    try:
        manager_port = int(sys.argv[1])
    except ValueError:
        print("Error: port must be an integer.")
        sys.exit(1)

    # Create an IPv4 UDP socket.
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        # Bind to all local network interfaces on the requested port.
        manager_socket.bind(("", manager_port))
    except OSError as error:
        print(f"Error binding manager to port {manager_port}: {error}")
        manager_socket.close()
        sys.exit(1)

    print(f"[MANAGER] Started on UDP port {manager_port}")
    print("[MANAGER] Waiting for messages...")

    try:
        while True:
            data, peer_address = manager_socket.recvfrom(65535)

            message = data.decode("utf-8")

            print(
                f"[MANAGER] Received from "
                f"{peer_address[0]}:{peer_address[1]}: {message}"
            )

    except KeyboardInterrupt:
        print("\n[MANAGER] Manager stopped.")

    finally:
        manager_socket.close()


if __name__ == "__main__":
    main()