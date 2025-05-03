# 🗄️ Fault-Tolerant Key/Value Store (Raft-Based)

A fault-tolerant, linearizable key/value store built using a custom implementation of the Raft consensus algorithm. The system ensures strong consistency and availability across a replicated cluster, even in the presence of server crashes and network partitions.
Inspired by materials from MIT's 6.5840 Distributed Systems course (Morris, Kaashoek, Zeldovich) and developed at UBC as part of a distributed systems deep dive.



## 🔧 Language & Core Technologies
- Golang

- Raft (custom implementation)

- RPC — for server-to-server and client-server communication


## ✅ Features
- **Strong Consistency & Linearizability**: Ensures Get/Put/Append operations behave as if executed by a single server.

- **Fault Tolerance via Replication**: Supports crash and partition recovery through Raft-based log replication and leader election.

- **Exactly-Once Semantics**: Client-side deduplication prevents repeated execution of retried operations.

- **Dynamic Leader Tracking**: Clients maintain leader affinity to reduce latency and improve throughput.

- **Partition & Recovery Handling**: Robust handling of partial failures and asynchronous recovery with no data loss.

## 🧪 Testing Highlights
Validated through test-driven development against extensive scenarios including:

- High concurrency (many clients)

- Network unreliability and leader reelections

- Server crashes, restarts, and healing from partitions

- Randomized key operations under stress

## 📎 Example Usage

```
clerk.Put("x", "1")       // Stores value "1" for key "x"

clerk.Append("x", "2")    // Updates "x" -> "12"

value := clerk.Get("x")   // Returns "12"
```

Built by: William Nguyen and Michelle Zhou as part of CPSC 416, Distributed Systems at the University of British Columbia
