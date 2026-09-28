import csv
import json
import socket
import sys
import threading
import time


DATASET_1950 = "StormEvents_details-ftp_v1.0_d1950_c20260323.csv"

REQUIRED_FIELDS = [
    "EVENT_ID",
    "STATE",
    "YEAR",
    "MONTH_NAME",
    "EVENT_TYPE",
    "CZ_TYPE",
    "CZ_NAME",
    "INJURIES_DIRECT",
    "INJURIES_INDIRECT",
    "DEATHS_DIRECT",
    "DEATHS_INDIRECT",
    "DAMAGE_PROPERTY",
    "DAMAGE_CROPS",
    "TOR_F_SCALE"
]


def is_prime(number):
    if number < 2:
        return False

    if number == 2:
        return True

    if number % 2 == 0:
        return False

    divisor = 3

    while divisor * divisor <= number:
        if number % divisor == 0:
            return False

        divisor += 2

    return True


def next_prime(number):
    candidate = number + 1

    while not is_prime(candidate):
        candidate += 1

    return candidate


def send_p2p_message(p_socket, peer, message):
    p_socket.sendto(
        json.dumps(message).encode("utf-8"),
        (peer["ip"], peer["p_port"])
    )


def configure_right_neighbor(state):
    if state["id"] is None or not state["dht_peers"]:
        return

    # Next ID in the ring wraps back to zero
    next_id = (state["id"] + 1) % state["ring_size"]
    state["right_neighbor"] = state["dht_peers"][next_id]


def store_local_record(state, position, record):
    # Keep collisions at the same position instead of losing a record
    if position not in state["local_hash_table"]:
        state["local_hash_table"][position] = []

    state["local_hash_table"][position].append(record)
    state["record_count"] += 1


def handle_set_id(message, state):
    state["id"] = message.get("id")
    state["ring_size"] = message.get("ring_size")
    state["dht_peers"] = message.get("peers", [])
    state["year"] = message.get("year")
    state["hash_table_size"] = message.get("hash_table_size")

    configure_right_neighbor(state)

    right_neighbor = state["right_neighbor"]

    print(f"[PEER {state['peer_name']}] SET-ID received")
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


def handle_store(message, p_socket, state):
    destination_id = message.get("destination_id")
    position = message.get("position")
    record = message.get("record")

    if destination_id == state["id"]:
        store_local_record(
            state,
            position,
            record
        )
        return

    # Pass the record to the next peer in the ring
    send_p2p_message(
        p_socket,
        state["right_neighbor"],
        message
    )


def handle_count_request(message, p_socket, state):
    counts = message.get("counts", {})

    counts[str(state["id"])] = {
        "peer_name": state["peer_name"],
        "count": state["record_count"]
    }

    if state["id"] == 0:
        state["final_counts"] = counts
        state["count_event"].set()
        return

    send_p2p_message(
        p_socket,
        state["right_neighbor"],
        {
            "command": "count-request",
            "counts": counts
        }
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

            if command == "set-id":
                print(
                    f"\n[PEER {state['peer_name']}] "
                    f"Received P2P command 'set-id' from "
                    f"{sender_address[0]}:{sender_address[1]}"
                )

                handle_set_id(
                    message,
                    state
                )

            elif command == "store":
                handle_store(
                    message,
                    p_socket,
                    state
                )

            elif command == "count-request":
                handle_count_request(
                    message,
                    p_socket,
                    state
                )

            else:
                print(
                    f"\n[PEER {state['peer_name']}] "
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


def load_dataset(year):
    if year != 1950:
        raise ValueError(
            "This milestone implementation currently has "
            "the provided 1950 dataset available."
        )

    records = []

    with open(
        DATASET_1950,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as csv_file:
        reader = csv.DictReader(csv_file)

        if reader.fieldnames is None:
            raise ValueError("Dataset has no header.")

        missing_fields = [
            field
            for field in REQUIRED_FIELDS
            if field not in reader.fieldnames
        ]

        if missing_fields:
            raise ValueError(
                "Dataset is missing required fields: "
                + ", ".join(missing_fields)
            )

        # Only keep the fields needed by the project
        for row in reader:
            record = {
                field: row.get(field, "")
                for field in REQUIRED_FIELDS
            }

            records.append(record)

    return records


def send_set_id_messages(p_socket, state):
    for assigned_id in range(1, state["ring_size"]):
        peer = state["dht_peers"][assigned_id]

        message = {
            "command": "set-id",
            "id": assigned_id,
            "ring_size": state["ring_size"],
            "year": state["year"],
            "peers": state["dht_peers"],
            "hash_table_size": state["hash_table_size"]
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


def distribute_records(p_socket, state, records):
    total = len(records)

    print(
        f"[PEER {state['peer_name']}] "
        f"Distributing {total} storm records"
    )

    for record in records:
        event_id = int(record["EVENT_ID"])

        # Hash the event to a position and peer ID
        position = event_id % state["hash_table_size"]
        destination_id = position % state["ring_size"]

        if destination_id == state["id"]:
            store_local_record(
                state,
                position,
                record
            )

        else:
            send_p2p_message(
                p_socket,
                state["right_neighbor"],
                {
                    "command": "store",
                    "destination_id": destination_id,
                    "position": position,
                    "record": record
                }
            )

    print(
        f"[PEER {state['peer_name']}] "
        "Finished sending storm records"
    )


def collect_record_counts(p_socket, state):
    state["count_event"].clear()
    state["final_counts"] = {}

    initial_counts = {
        "0": {
            "peer_name": state["peer_name"],
            "count": state["record_count"]
        }
    }

    # Send the count request once around the ring
    send_p2p_message(
        p_socket,
        state["right_neighbor"],
        {
            "command": "count-request",
            "counts": initial_counts
        }
    )

    completed = state["count_event"].wait(
        timeout=10
    )

    if not completed:
        print(
            f"[PEER {state['peer_name']}] "
            "Timed out waiting for record counts"
        )
        return False

    print("")
    print("[DHT] Record distribution:")

    total = 0

    for peer_id in range(state["ring_size"]):
        info = state["final_counts"][str(peer_id)]

        print(
            f"[DHT] {info['peer_name']} "
            f"(ID {peer_id}): "
            f"{info['count']} records"
        )

        total += info["count"]

    print(f"[DHT] Total records: {total}")

    state["distributed_total"] = total

    return True


def send_dht_complete(
    m_socket,
    manager_address,
    state
):
    print(
        f"[PEER {state['peer_name']}] "
        "Sending DHT-COMPLETE to manager"
    )

    response = send_manager_request(
        m_socket,
        manager_address,
        {
            "command": "dht-complete",
            "peer_name": state["peer_name"]
        }
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"DHT-COMPLETE response: "
        f"{response.get('status', 'FAILURE')}"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"{response.get('message', '')}"
    )


def build_dht(
    m_socket,
    p_socket,
    manager_address,
    state
):
    try:
        records = load_dataset(
            state["year"]
        )
    except (OSError, ValueError) as error:
        print(
            f"[PEER {state['peer_name']}] "
            f"Dataset error: {error}"
        )
        return

    record_count = len(records)

    # Hash table size is the first prime larger than 2 * records
    state["hash_table_size"] = next_prime(
        2 * record_count
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"Loaded {record_count} storm records"
    )

    print(
        f"[PEER {state['peer_name']}] "
        f"Hash table size s = "
        f"{state['hash_table_size']}"
    )

    send_set_id_messages(
        p_socket,
        state
    )

    time.sleep(0.5)

    distribute_records(
        p_socket,
        state,
        records
    )

    time.sleep(1)

    if not collect_record_counts(
        p_socket,
        state
    ):
        return

    # Total should match the number of rows read from the file
    if state["distributed_total"] != record_count:
        print(
            f"[PEER {state['peer_name']}] "
            f"ERROR: expected {record_count} records "
            f"but counted {state['distributed_total']}"
        )
        return

    print(
        f"[PEER {state['peer_name']}] "
        "All records accounted for"
    )

    send_dht_complete(
        m_socket,
        manager_address,
        state
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

    status = response.get(
        "status",
        "FAILURE"
    )

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
    state["dht_peers"] = response.get(
        "peers",
        []
    )

    configure_right_neighbor(state)

    print(
        f"[PEER {state['peer_name']}] "
        "Selected DHT peers:"
    )

    for index, peer in enumerate(
        state["dht_peers"]
    ):
        print(
            f"  ID {index}: "
            f"{peer['peer_name']} "
            f"{peer['ip']}:{peer['p_port']}"
        )

    print(
        f"[PEER {state['peer_name']}] "
        "Assigned leader ID 0"
    )

    right_neighbor = state[
        "right_neighbor"
    ]

    print(
        f"[PEER {state['peer_name']}] "
        f"Right neighbor: "
        f"{right_neighbor['peer_name']} "
        f"{right_neighbor['ip']}:"
        f"{right_neighbor['p_port']}"
    )

    build_dht(
        m_socket,
        p_socket,
        manager_address,
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
        manager_port = int(
            sys.argv[2]
        )
    except ValueError:
        print(
            "Error: manager port must "
            "be an integer."
        )
        sys.exit(1)

    if not (
        1 <= manager_port <= 65535
    ):
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
        "hash_table_size": None,
        "local_hash_table": {},
        "record_count": 0,
        "final_counts": {},
        "distributed_total": 0,
        "count_event": threading.Event()
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
            command_line = input(
                "peer> "
            ).strip()

            if not command_line:
                continue

            if (
                command_line.lower()
                == "exit"
            ):
                print(
                    "[PEER] Peer stopped."
                )
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
                        "<peer-name> "
                        "<IPv4-address> "
                        "<m-port> <p-port>"
                    )
                    continue

                candidate_name = parts[1]
                candidate_ip = parts[2]

                try:
                    candidate_m_port = int(
                        parts[3]
                    )

                    candidate_p_port = int(
                        parts[4]
                    )

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
                        (
                            "",
                            candidate_m_port
                        )
                    )

                    p_socket = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_DGRAM
                    )

                    p_socket.bind(
                        (
                            "",
                            candidate_p_port
                        )
                    )

                except OSError as error:
                    print(
                        "[PEER] Could not bind "
                        f"communication ports: "
                        f"{error}"
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
                        args=(
                            p_socket,
                            state
                        ),
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
        print(
            "\n[PEER] Peer stopped."
        )

    finally:
        if m_socket is not None:
            m_socket.close()

        if p_socket is not None:
            p_socket.close()


if __name__ == "__main__":
    main()