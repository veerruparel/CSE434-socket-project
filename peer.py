import csv
import glob
import json
import os
import random
import socket
import sys
import threading
import time


# Records sent by the leader before it checks the ring counts
BATCH_SIZE = 50

# Seconds to wait for a ring message to come back around
RING_TIMEOUT = 60

# Seconds to wait for the leader to refill the DHT
REBUILD_TIMEOUT = 300

csv.field_size_limit(10 ** 8)

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

    # Start with an empty local table
    state["local_hash_table"] = {}
    state["record_count"] = 0

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


def log(state, text):
    print(f"[PEER {state['peer_name']}] {text}")


def tuple_of(state):
    return {
        "peer_name": state["peer_name"],
        "ip": state["peer_ip"],
        "p_port": state["p_port"]
    }


def clear_ring_state(state):
    state["id"] = None
    state["ring_size"] = None
    state["dht_peers"] = []
    state["right_neighbor"] = None
    state["hash_table_size"] = None
    state["local_hash_table"] = {}
    state["record_count"] = 0


def send_find_response(p_socket, requester, event_id, status, record, id_seq):
    send_p2p_message(
        p_socket,
        requester,
        {
            "command": "find-event-response",
            "status": status,
            "event_id": event_id,
            "record": record,
            "id_seq": id_seq
        }
    )


def handle_find_event(message, p_socket, state):
    event_id = message.get("event_id")
    requester = message.get("requester")
    id_seq = message.get("id_seq", [])
    remaining = message.get("remaining")

    if state["id"] is None:
        log(state, "FIND-EVENT received but this peer is not in a DHT")
        send_find_response(p_socket, requester, event_id, "FAILURE", None, id_seq)
        return

    position = event_id % state["hash_table_size"]
    owner_id = position % state["ring_size"]

    # The first peer starts the id-seq and the set of unvisited ids
    if not id_seq:
        id_seq = [state["id"]]
        remaining = [
            peer_id
            for peer_id in range(state["ring_size"])
            if peer_id != state["id"]
        ]

    log(
        state,
        f"FIND-EVENT {event_id} received: pos={position}, "
        f"owner id={owner_id}, id-seq={id_seq}"
    )

    for record in state["local_hash_table"].get(position, []):
        if int(record["EVENT_ID"]) == event_id:
            log(state, f"Event {event_id} found, sending SUCCESS to {requester['peer_name']}")
            send_find_response(p_socket, requester, event_id, "SUCCESS", record, id_seq)
            return

    # The owner would hold the record, so nobody else has it
    if state["id"] == owner_id or not remaining:
        log(state, f"Event {event_id} not found, sending FAILURE to {requester['peer_name']}")
        send_find_response(p_socket, requester, event_id, "FAILURE", None, id_seq)
        return

    # Pick next peer randomly (hot potato)
    next_id = random.choice(remaining)
    remaining.remove(next_id)
    id_seq.append(next_id)

    next_peer = state["dht_peers"][next_id]

    log(state, f"Forwarding FIND-EVENT {event_id} to id {next_id} ({next_peer['peer_name']})")

    send_p2p_message(
        p_socket,
        next_peer,
        {
            "command": "find-event",
            "event_id": event_id,
            "requester": requester,
            "remaining": remaining,
            "id_seq": id_seq
        }
    )


def handle_find_response(message, state):
    event_id = message.get("event_id")
    id_seq = message.get("id_seq", [])
    seq_text = ", ".join(str(peer_id) for peer_id in id_seq)

    print("")

    if message.get("status") == "SUCCESS":
        print(f"[QUERY] Storm event {event_id} found")

        for field in REQUIRED_FIELDS:
            print(f"[QUERY]   {field.lower()}: {message['record'][field]}")

    else:
        print(f"Storm event {event_id} not found in the DHT.")

    print(f"[QUERY]   id-seq: {seq_text}")

    state["query_event"].set()


def run_teardown(p_socket, state, clear_ring):
    state["teardown_event"].clear()

    log(state, "Sending TEARDOWN to right neighbor")

    send_p2p_message(
        p_socket,
        state["right_neighbor"],
        {
            "command": "teardown",
            "origin": state["peer_name"],
            "clear_ring": clear_ring
        }
    )

    if not state["teardown_event"].wait(timeout=RING_TIMEOUT):
        log(state, "Timed out waiting for TEARDOWN to return")
        return False

    return True


def handle_teardown(message, p_socket, state):
    log(state, f"TEARDOWN received (origin {message['origin']}), deleting local hash table")

    state["local_hash_table"] = {}
    state["record_count"] = 0

    # The peer that started the teardown stops it
    if message["origin"] == state["peer_name"]:
        state["teardown_event"].set()
        return

    send_p2p_message(p_socket, state["right_neighbor"], message)

    if message.get("clear_ring"):
        clear_ring_state(state)


def run_reset_id(p_socket, state, first_id, new_peers):
    state["reset_event"].clear()

    message = {
        "command": "reset-id",
        "id": first_id,
        "ring_size": len(new_peers),
        "peers": new_peers,
        "year": state["year"],
        "hash_table_size": state["hash_table_size"],
        "origin": tuple_of(state)
    }

    log(state, f"Sending RESET-ID {first_id} (ring size {len(new_peers)}) to right neighbor")

    send_p2p_message(p_socket, state["right_neighbor"], message)

    if not state["reset_event"].wait(timeout=RING_TIMEOUT):
        log(state, "Timed out waiting for RESET-ID to return")
        return False

    return True


def handle_reset_id(message, p_socket, state):
    origin = message["origin"]

    # Back at the peer that started the renumbering
    if origin["peer_name"] == state["peer_name"]:
        log(state, "RESET-ID returned, ring renumbered")
        state["reset_event"].set()
        return

    state["id"] = message["id"]
    state["ring_size"] = message["ring_size"]
    state["dht_peers"] = message["peers"]
    state["year"] = message["year"]
    state["hash_table_size"] = message["hash_table_size"]

    configure_right_neighbor(state)

    log(
        state,
        f"RESET-ID received: new ID {state['id']}, "
        f"ring size {state['ring_size']}, "
        f"right neighbor {state['right_neighbor']['peer_name']}"
    )

    forward = dict(message)
    forward["id"] = state["id"] + 1

    # The last peer reports back to the peer that started it
    if state["id"] == state["ring_size"] - 1:
        send_p2p_message(p_socket, origin, forward)
    else:
        send_p2p_message(p_socket, state["right_neighbor"], forward)


def rebuild_as_leader(p_socket, state, requester):
    status = "FAILURE"

    try:
        records = load_dataset(state["year"])

        state["hash_table_size"] = next_prime(2 * len(records))

        log(state, f"Rebuilding DHT as leader: {len(records)} records, s = {state['hash_table_size']}")

        if fill_dht(p_socket, state, records):
            status = "SUCCESS"

    except (OSError, ValueError) as error:
        log(state, f"Dataset error: {error}")

    log(state, f"Sending REBUILD-COMPLETE ({status}) to {requester['peer_name']}")

    send_p2p_message(
        p_socket,
        requester,
        {
            "command": "rebuild-complete",
            "status": status
        }
    )


def handle_rebuild_dht(message, p_socket, state):
    log(state, f"REBUILD-DHT received from {message['requester']['peer_name']}")

    # The ring keeps handling messages while the leader fills it
    threading.Thread(
        target=rebuild_as_leader,
        args=(p_socket, state, message["requester"]),
        daemon=True
    ).start()


def handle_rebuild_complete(message, state):
    log(state, f"REBUILD-COMPLETE received: {message.get('status')}")

    state["rebuild_status"] = message.get("status")
    state["rebuilt_event"].set()


def lead_join(p_socket, state, joiner):
    log(state, f"JOIN-REQUEST received from {joiner['peer_name']}")

    status = "FAILURE"

    if state["id"] == 0:
        if run_teardown(p_socket, state, False):
            # Joining peer goes at the end of the ring
            new_peers = state["dht_peers"] + [joiner]

            state["dht_peers"] = new_peers
            state["ring_size"] = len(new_peers)
            configure_right_neighbor(state)

            if run_reset_id(p_socket, state, 1, new_peers):
                rebuild_as_leader(p_socket, state, joiner)
                return
    else:
        log(state, "This peer is not the leader, ignoring JOIN-REQUEST")

    send_p2p_message(
        p_socket,
        joiner,
        {
            "command": "rebuild-complete",
            "status": status
        }
    )


def handle_join_request(message, p_socket, state):
    threading.Thread(
        target=lead_join,
        args=(p_socket, state, message["requester"]),
        daemon=True
    ).start()


def listen_for_peer_messages(p_socket, state):
    while True:
        try:
            data, sender_address = p_socket.recvfrom(65535)
        except OSError:
            if p_socket.fileno() == -1:
                break
            continue

        try:
            message = json.loads(data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            print(
                f"\n[PEER {state['peer_name']}] "
                "Received invalid P2P message"
            )
            continue

        command = message.get("command")

        try:
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

            elif command == "find-event":
                handle_find_event(message, p_socket, state)

            elif command == "find-event-response":
                handle_find_response(message, state)

            elif command == "teardown":
                handle_teardown(message, p_socket, state)

            elif command == "reset-id":
                handle_reset_id(message, p_socket, state)

            elif command == "rebuild-dht":
                handle_rebuild_dht(message, p_socket, state)

            elif command == "rebuild-complete":
                handle_rebuild_complete(message, state)

            elif command == "join-request":
                handle_join_request(message, p_socket, state)

            else:
                print(
                    f"\n[PEER {state['peer_name']}] "
                    f"Unknown P2P command: {command}"
                )

        except Exception as error:
            print(
                f"\n[PEER {state['peer_name']}] "
                f"Error handling {command}: {error}"
            )


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


def find_dataset(year):
    folders = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(__file__))
    ]

    patterns = [
        f"StormEvents_details-ftp_v1.0_d{year}_c*.csv",
        f"details-{year}.csv"
    ]

    for folder in folders:
        for pattern in patterns:
            matches = sorted(
                glob.glob(os.path.join(folder, pattern))
            )

            if matches:
                return matches[-1]

    raise FileNotFoundError(
        f"No dataset file found for year {year}"
    )


def load_dataset(year):
    records = []

    with open(
        find_dataset(year),
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

    sent = 0

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

        sent += 1

        # Let the ring catch up so UDP buffers do not overflow
        if sent % BATCH_SIZE == 0:
            if not request_counts(p_socket, state):
                return False

            if state["distributed_total"] != sent:
                print(
                    f"[PEER {state['peer_name']}] "
                    f"ERROR: sent {sent} records but ring "
                    f"holds {state['distributed_total']}"
                )
                return False

    print(
        f"[PEER {state['peer_name']}] "
        "Finished sending storm records"
    )

    return True


def request_counts(p_socket, state):
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

    total = 0

    for peer_id in range(state["ring_size"]):
        total += state["final_counts"][str(peer_id)]["count"]

    state["distributed_total"] = total

    return True


def collect_record_counts(p_socket, state):
    if not request_counts(p_socket, state):
        return False

    print("")
    print("[DHT] Record distribution:")

    for peer_id in range(state["ring_size"]):
        info = state["final_counts"][str(peer_id)]

        print(
            f"[DHT] {info['peer_name']} "
            f"(ID {peer_id}): "
            f"{info['count']} records"
        )

    print(f"[DHT] Total records: {state['distributed_total']}")

    return True


def fill_dht(p_socket, state, records):
    state["local_hash_table"] = {}
    state["record_count"] = 0

    if not distribute_records(p_socket, state, records):
        return False

    if not collect_record_counts(p_socket, state):
        return False

    # Total should match the number of rows read from the file
    if state["distributed_total"] != len(records):
        print(
            f"[PEER {state['peer_name']}] "
            f"ERROR: expected {len(records)} records "
            f"but counted {state['distributed_total']}"
        )
        return False

    print(
        f"[PEER {state['peer_name']}] "
        "All records accounted for"
    )

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

    if not fill_dht(
        p_socket,
        state,
        records
    ):
        return

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


def manager_command(m_socket, manager_address, state, command, **fields):
    message = {
        "command": command,
        "peer_name": state["peer_name"]
    }

    message.update(fields)

    log(state, f"Sending {command.upper()} to manager")

    response = send_manager_request(m_socket, manager_address, message)

    log(state, f"{command.upper()} response: {response.get('status', 'FAILURE')}")
    log(state, response.get("message", ""))

    return response


def query_dht(m_socket, p_socket, manager_address, state, event_id):
    response = manager_command(m_socket, manager_address, state, "query-dht")

    if response.get("status") != "SUCCESS":
        return

    target = response["peer"]

    state["query_event"].clear()

    log(state, f"Sending FIND-EVENT {event_id} to {target['peer_name']}")

    send_p2p_message(
        p_socket,
        target,
        {
            "command": "find-event",
            "event_id": event_id,
            "requester": tuple_of(state),
            "remaining": None,
            "id_seq": []
        }
    )

    if not state["query_event"].wait(timeout=RING_TIMEOUT):
        log(state, "Timed out waiting for the query response")


def leave_dht(m_socket, p_socket, manager_address, state):
    response = manager_command(m_socket, manager_address, state, "leave-dht")

    if response.get("status") != "SUCCESS":
        return

    my_id = state["id"]
    old_peers = state["dht_peers"]
    new_leader = state["right_neighbor"]

    # New ring starts at my right neighbor and skips me
    new_peers = old_peers[my_id + 1:] + old_peers[:my_id]

    # Step 1: teardown
    if not run_teardown(p_socket, state, False):
        return

    # Step 2: renumber the remaining peers
    if not run_reset_id(p_socket, state, 0, new_peers):
        return

    # Step 3: new leader rebuilds the DHT
    state["rebuilt_event"].clear()

    log(state, f"Sending REBUILD-DHT to new leader {new_leader['peer_name']}")

    send_p2p_message(
        p_socket,
        new_leader,
        {
            "command": "rebuild-dht",
            "requester": tuple_of(state)
        }
    )

    if not state["rebuilt_event"].wait(timeout=REBUILD_TIMEOUT) or state["rebuild_status"] != "SUCCESS":
        log(state, "DHT rebuild failed")
        return

    # Step 4: tell the manager
    manager_command(
        m_socket,
        manager_address,
        state,
        "dht-rebuilt",
        new_leader=new_leader["peer_name"]
    )

    clear_ring_state(state)

    log(state, "Left the DHT and is now Free")


def join_dht(m_socket, p_socket, manager_address, state):
    response = manager_command(m_socket, manager_address, state, "join-dht")

    if response.get("status") != "SUCCESS":
        return

    leader = response["leader"]

    state["rebuilt_event"].clear()

    log(state, f"Sending JOIN-REQUEST to leader {leader['peer_name']}")

    send_p2p_message(
        p_socket,
        leader,
        {
            "command": "join-request",
            "requester": tuple_of(state)
        }
    )

    if not state["rebuilt_event"].wait(timeout=REBUILD_TIMEOUT) or state["rebuild_status"] != "SUCCESS":
        log(state, "DHT rebuild failed")
        return

    manager_command(
        m_socket,
        manager_address,
        state,
        "dht-rebuilt",
        new_leader=leader["peer_name"]
    )

    log(state, f"Joined the DHT with ID {state['id']}")


def teardown_dht(m_socket, p_socket, manager_address, state):
    response = manager_command(m_socket, manager_address, state, "teardown-dht")

    if response.get("status") != "SUCCESS":
        return

    if not run_teardown(p_socket, state, True):
        return

    clear_ring_state(state)

    manager_command(m_socket, manager_address, state, "teardown-complete")


def deregister_peer(m_socket, manager_address, state):
    response = manager_command(m_socket, manager_address, state, "deregister")

    return response.get("status") == "SUCCESS"


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
        "count_event": threading.Event(),
        "teardown_event": threading.Event(),
        "reset_event": threading.Event(),
        "rebuilt_event": threading.Event(),
        "query_event": threading.Event(),
        "rebuild_status": None
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

            elif command in (
                "query-dht",
                "leave-dht",
                "join-dht",
                "teardown-dht",
                "deregister"
            ):
                expected = 3 if command == "query-dht" else 2

                if len(parts) != expected:
                    if command == "query-dht":
                        print("Usage: query-dht <peer-name> <event-id>")
                    else:
                        print(f"Usage: {command} <peer-name>")
                    continue

                if not state["registered"]:
                    print("[PEER] This peer is not registered.")
                    continue

                if parts[1] != state["peer_name"]:
                    print(
                        f"[PEER {state['peer_name']}] "
                        "Peer name must match this peer."
                    )
                    continue

                if command == "query-dht":
                    try:
                        event_id = int(parts[2])
                    except ValueError:
                        print("[PEER] event-id must be an integer.")
                        continue

                    query_dht(
                        m_socket,
                        p_socket,
                        manager_address,
                        state,
                        event_id
                    )

                elif command == "leave-dht":
                    leave_dht(m_socket, p_socket, manager_address, state)

                elif command == "join-dht":
                    join_dht(m_socket, p_socket, manager_address, state)

                elif command == "teardown-dht":
                    teardown_dht(m_socket, p_socket, manager_address, state)

                elif deregister_peer(m_socket, manager_address, state):
                    print(f"[PEER {state['peer_name']}] Peer exiting.")
                    break

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