---- MODULE versioned_index ----

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Nodes, Clients, MaxVersion, MaxWrites

VARIABLES remoteIndex, remoteVersion, localIndex, localVersion, clientVersion, clientOps

(*
  The `Init` predicate defines the initial state of the system.
  - remoteIndex: Initially, the remote index is set to 0.
  - remoteVersion: Initially, the remote version is set to 0.
  - localIndex: Each node's local index is initially set to 0.
  - localVersion: Each node's local version is initially set to 0.
  - clientVersion: Each client's version is initially set to 0.
  - clientOps: Each client's operation count is initially set to 0.
*)
Init ==
  /\ remoteIndex = 0
  /\ remoteVersion = 0
  /\ localIndex = [n \in Nodes |-> 0]
  /\ localVersion = [n \in Nodes |-> 0]
  /\ clientVersion = [c \in Clients |-> 0]
  /\ clientOps = [c \in Clients |-> 0]

(*
  The `UpdateToLatest` action updates the local index and version to the latest remote version if outdated.
*)
UpdateToLatest(n) ==
  /\ localVersion[n] < remoteVersion
  /\ localIndex' = [localIndex EXCEPT ![n] = remoteIndex]
  /\ localVersion' = [localVersion EXCEPT ![n] = remoteVersion]
  /\ UNCHANGED <<remoteIndex, remoteVersion, clientVersion, clientOps>>

(*
  The `Read` action represents a node reading the remote index.
  - If the local version is outdated, updates the local index and version.
  - If the local version is up-to-date, reads the value from the local index.
  - Sets the client's version to the local version.
  - UNCHANGED <<remoteIndex, remoteVersion>>: These remain unchanged.
*)
Read(n, c) ==
  /\ (localVersion[n] < remoteVersion => UpdateToLatest(n))
  /\ UNCHANGED <<localIndex, localVersion, remoteIndex, remoteVersion, clientOps>>
  /\ clientVersion' = [clientVersion EXCEPT ![c] = localVersion[n]]

(*
  The `Write` action represents a node writing a new index to the remote index.
  - Ensures the local index and version are up-to-date.
  - If the local version is up-to-date, writes the local index, increments the version, and updates the remote index and version.
  - Sets the client's version to the new local version.
  - Increments the operation count for the client.
*)
Write(n, c, newIndex) ==
  /\ clientOps[c] < MaxWrites
  /\ remoteVersion < MaxVersion  (* Ensure the remote version does not exceed the maximum allowed version *)
  /\ (localVersion[n] < remoteVersion => UpdateToLatest(n))  (* Update if the local version is outdated *)
  /\ localIndex' = [localIndex EXCEPT ![n] = newIndex]  (* Update the local index with the new index *)
  /\ localVersion' = [localVersion EXCEPT ![n] = localVersion[n] + 1]  (* Increment the local version *)
  /\ remoteIndex' = newIndex  (* Update the remote index with the new index *)
  /\ remoteVersion' = localVersion[n] + 1  (* Increment the remote version *)
  /\ clientVersion' = [clientVersion EXCEPT ![c] = localVersion[n] + 1]  (* Update the client version *)
  /\ clientOps' = [clientOps EXCEPT ![c] = clientOps[c] + 1]

(*
  The `Client` action simulates multiple clients calling Read and Write and collecting the returned version.
  - Ensures subsequent calls get identical or larger versions.
*)
Client ==
  \E c \in Clients:
    clientOps[c] < MaxWrites /\
    \E n \in Nodes:
      (clientOps[c] < MaxWrites => (Read(n, c) \/ \E newIndex \in 0..MaxVersion: Write(n, c, newIndex)))

(*
  The `Next` relation defines the possible state transitions in the system.
  - Includes the `Client` action.
*)
Next ==
  Client

(*
  The `Invariant` defines a property that must always hold.
  - The local version of any node must be at least as recent as the remote version.
  - The client version must be non-decreasing.
  - Each client's operation count must not exceed the maximum allowed operations.
*)
Invariant ==
  /\ \A n \in Nodes: localVersion[n] >= remoteVersion
  /\ \A c \in Clients: clientVersion[c] <= remoteVersion
  /\ \A n \in Nodes: localIndex[n] <= remoteIndex

====

