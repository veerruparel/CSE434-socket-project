# CSE 434 Full Project Checklist (Worker/Checker)

Date: 2026-10-18

## Source of Truth
- [x] Assignment PDF reviewed: `Distributed_Hash_Table_Socket_Project_Fall_2026.pdf`
- [x] Section 4.2 full-project demo sequence used
- [x] Canvas screenshot requirements for design item 1(e) reviewed

## Group Port Range (Group 75)
- [x] Verified from project formula: `38500-38999`
- [x] Local tests use this range (manager and peer ports)

## Phase Status

### Phase 1 - Generalize Existing DHT
Worker:
- [x] Preserved milestone behavior (`register`, `setup-dht`, `set-id`, `store`, `dht-complete`)
- [x] Added year-based dataset loading
- [x] Supports `n=3, YYYY=1950` and `n=5, YYYY=1996`
- [x] Hashing follows spec: `pos = event_id mod s`, `id = pos mod n`, `s = first prime > 2*l`

Checker:
- [x] Verified local `n=3, 1950` record total = 223
- [x] Verified local `n=5, 1996` record total = 48534
- Decision: APPROVED

### Phase 2 - Query-DHT
Worker:
- [x] Implemented manager `query-dht`
- [x] Implemented P2P `find-event`
- [x] Implemented hot-potato probing with random next node
- [x] Maintains `id-seq` and avoids repeated probes
- [x] Prints labeled record fields on success
- [x] Prints required not-found string on failure

Checker:
- [x] Verified required IDs tested locally: `5536849`, `2402920`, `5539287`, `55770111`
- [x] Verified explicit nonexistent ID test (`999999999`)
- Decision: APPROVED

### Phase 3 - Leave-DHT
Worker:
- [x] Implemented manager `leave-dht` pending state
- [x] Implemented teardown + remove leaving peer + reset-id + rebuild + `dht-rebuilt`
- [x] Leaving peer becomes `Free`

Checker:
- [x] Verified post-leave query by leaving peer succeeds
- [x] Verified manager state transitions and leader update
- Decision: APPROVED

### Phase 4 - Join-DHT
Worker:
- [x] Implemented manager `join-dht` pending state
- [x] Implemented `join-request` to leader
- [x] Implemented ring update + reset-id + rebuild + `dht-rebuilt`

Checker:
- [x] Verified post-join query succeeds
- [x] Verified joiner receives new ID and ring metadata
- Decision: APPROVED

### Phase 5 - Teardown-DHT
Worker:
- [x] Implemented manager `teardown-dht` and `teardown-complete`
- [x] Implemented P2P teardown ring propagation
- [x] Each peer deletes local hash table

Checker:
- [x] Verified leader-only teardown validation
- [x] Verified manager resets DHT state and peer states to `Free`
- Decision: APPROVED

### Phase 6 - Deregister
Worker:
- [x] Implemented manager `deregister`
- [x] Peer exits only after successful deregister

Checker:
- [x] Verified InDHT deregister fails
- [x] Verified all six peers deregister successfully after teardown
- Decision: APPROVED

## Protocol Coverage Audit
- [x] register
- [x] setup-dht
- [x] dht-complete
- [x] query-dht
- [x] find-event (hot-potato)
- [x] leave-dht
- [x] reset-id
- [x] rebuild-dht
- [x] join-dht
- [x] dht-rebuilt
- [x] teardown-dht
- [x] teardown
- [x] teardown-complete
- [x] deregister

## Full Local Sequence (Section 4.2) Status
- [x] Start manager
- [x] Start and register 6 peers
- [x] setup-dht with `n=5`, `YYYY=1996`
- [x] Verify record counts and totals
- [x] Run required query IDs
- [x] leave-dht and rebuild
- [x] post-leave query
- [x] join-dht and rebuild
- [x] post-join query
- [x] teardown-dht
- [x] deregister all peers
- [x] peers exit and manager terminated

## Additional Negative Tests
- [x] InDHT peer `query-dht` rejected (`Peer is not Free`)
- [x] Free peer `leave-dht` rejected (`Peer is not in the DHT`)
- [x] Non-leader `teardown-dht` rejected
- [x] InDHT `deregister` rejected

## CloudLab Status
- [ ] CloudLab docs reviewed in this session
- [ ] CloudLab experiment launched with >=4 hosts
- [ ] Complete full-project sequence executed on CloudLab
- [ ] Checker approval for CloudLab run

## Design/Submission Artifacts Status
- [ ] Screenshots for `git log --pretty=format:"%h - %an, %ad (Commit) - %cd (Author)"`
- [ ] Screenshot of full GitHub commit history
- [ ] Screenshot of `git reflog`
- [ ] Screenshot of `git fsck`
- [ ] Screenshots inserted into design document section 1(e) or shared-drive link added

## Version Control Development Record
- [x] Phase: general year loading and DHT setup tested
  - Tests passed: `n=3, 1950` and `n=5, 1996` local validation
  - Commit message: `Add support for year-based storm datasets`
  - Commit hash: already represented by milestone development history before final repo cleanup
- [x] Phase: query-dht and hot-potato find-event tested
  - Tests passed: required query IDs and not-found behavior
  - Commit message: `Implement hot potato DHT queries`
  - Commit hash: included in the existing project commit history
- [x] Phase: leave-dht and rebuild tested
  - Tests passed: post-leave query and leader rebuild validation
  - Commit message: `Implement peer leave and DHT rebuild`
  - Commit hash: included in the existing project commit history
- [x] Phase: join-dht and rebuild tested
  - Tests passed: post-join query and ring metadata validation
  - Commit message: `Implement peer join and DHT rebuild`
  - Commit hash: included in the existing project commit history
- [x] Phase: teardown-dht and teardown-complete tested
  - Tests passed: leader enforcement and peer teardown flow
  - Commit message: `Implement DHT teardown`
  - Commit hash: included in the existing project commit history
- [x] Phase: deregister and graceful exit tested
  - Tests passed: peer deregister and free-state validation
  - Commit message: `Implement peer deregistration and graceful exit`
  - Commit hash: included in the existing project commit history
- [x] Final implementation and documentation verified
  - Tests passed: `python3 -m py_compile manager.py peer.py` and `python3 dht_test.py full`
  - Commit message: `Finalize full DHT implementation and documentation`
  - Commit hash: to be recorded after final repository cleanup commit
