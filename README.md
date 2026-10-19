# CSE 434 Socket Programming Project (Full Project)

Python UDP implementation of the ASU CSE 434 distributed hash table project.

## Files

- `manager.py`: DHT manager process
- `peer.py`: peer process (manager + P2P commands)
- `StormEvents_details-ftp_v1.0_d1950_c20260323.csv`: 1950 dataset
- `StormEvents_details-ftp_v1.0_d1996_c20260323.csv`: 1996 dataset
- `FULL_PROJECT_CHECKLIST.md`: Worker/Checker status and verification

## Port Range (Group 75)

Per project rules, Group 75 (odd) uses:

- `38500-38999`

## Run Manager

```bash
python3 manager.py 38500
```

## Run Peer

```bash
python3 peer.py <manager-ip> 38500
```

Example (localhost):

```bash
python3 peer.py 127.0.0.1 38500
```

## Peer Commands

After starting a peer process:

```text
register <peer-name> <IPv4-address> <m-port> <p-port>
setup-dht <peer-name> <n> <YYYY>
query-dht <peer-name> <event-id>
leave-dht <peer-name>
join-dht <peer-name>
teardown-dht <peer-name>
deregister <peer-name>
exit
```

## Full Local Demo Sequence (Section 4.2)

Use six peers named `Alice Bob Carol Dave Erin Frank`.

1. Start manager on `38500`.
2. Start 6 peer terminals and register:
   - `register Alice 127.0.0.1 38510 38511`
   - `register Bob 127.0.0.1 38512 38513`
   - `register Carol 127.0.0.1 38514 38515`
   - `register Dave 127.0.0.1 38516 38517`
   - `register Erin 127.0.0.1 38518 38519`
   - `register Frank 127.0.0.1 38520 38521`
3. Build DHT (leader example):
   - `setup-dht Alice 5 1996`
4. Query from the one Free peer:
   - `query-dht <free-peer-name> 5536849`
   - `query-dht <free-peer-name> 2402920`
   - `query-dht <free-peer-name> 5539287`
   - `query-dht <free-peer-name> 55770111`
5. Leave from one InDHT peer:
   - `leave-dht <indht-peer-name>`
6. Query from the peer that left:
   - `query-dht <left-peer-name> 5536849`
7. Join from one outside Free peer:
   - `join-dht <free-peer-name>`
8. Query from the remaining outside peer:
   - `query-dht <remaining-free-peer-name> 5539287`
9. Teardown by current leader:
   - `teardown-dht <current-leader-name>`
10. Deregister all peers:
   - `deregister Alice` ... `deregister Frank`

## Tested Local Automation (from `/tmp`)

- `python3 /tmp/dht_test.py small`
- `python3 /tmp/dht_test.py p1`
- `python3 /tmp/dht_test.py full`
- `python3 /tmp/dht_negative_test.py`

These scripts are external test harnesses used for verification and are not required by the project submission.
