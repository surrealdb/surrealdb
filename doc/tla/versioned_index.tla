---- MODULE versioned_index ----

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Nodes, MaxVersion

VARIABLES remoteIndex, remoteVersion, localIndex, localVersion, clientVersion

(*
  The `Init` predicate defines the initial state of the system.
  - remoteIndex: Initially, the remote index is set to 0.
  - remoteVersion: Initially, the remote version is set to 0.
  - localIndex: Each node's local index is initially set to 0.
  - localVersion: Each node's local version is initially set to 0.
  - clientVersion: Initially, the client version is set to 0.
*)
Init ==
  /\ remoteIndex = 0
  /\ remoteVersion = 0
  /\ localIndex = [n \in Nodes |-> 0]
  /\ localVersion = [n \in Nodes |-> 0]
  /\ clientVersion = 0

(*
  The `UpdateToLatest` action updates the local index and version to the latest remote version if outdated.
*)
UpdateToLatest(n) ==
  /\ localVersion[n] < remoteVersion
  /\ localIndex' = [localIndex EXCEPT ![n] = remoteIndex]
  /\ localVersion' = [localVersion EXCEPT ![n] = remoteVersion]
  /\ UNCHANGED <<remoteIndex, remoteVersion, clientVersion>>

(*
  The `Read` action represents a node reading the remote index.
  - If the local version is outdated, updates the local index and version.
  - If the local version is up-to-date, reads the value from the local index.
  - Returns the version of the index after potentially updating it.
  - UNCHANGED <<localIndex, localVersion, remoteIndex, remoteVersion>>: These remain unchanged.
*)
Read(n) ==
  /\ (localVersion[n] < remoteVersion => UpdateToLatest(n))
  /\ UNCHANGED <<localIndex, localVersion, remoteIndex, remoteVersion>>
  /\ clientVersion' = localVersion[n]

(*
  The `Write` action represents a node writing a new index to the remote index.
  - Ensures the local index and version are up-to-date.
  - If the local version is up-to-date, writes the local index, increments the version, and updates the remote index and version.
  - Returns the version of the index after updating it.
*)
Write(n, newIndex) ==
  /\ remoteVersion < MaxVersion  (* Ensure the remote version does not exceed the maximum allowed version *)
  /\ (localVersion[n] < remoteVersion => UpdateToLatest(n))  (* Update if the local version is outdated *)
  /\ localVersion[n] = remoteVersion  (* Ensure the local version is up-to-date *)
  /\ localIndex' = [localIndex EXCEPT ![n] = newIndex]  (* Update the local index with the new index *)
  /\ localVersion' = [localVersion EXCEPT ![n] = localVersion[n] + 1]  (* Increment the local version *)
  /\ remoteIndex' = newIndex  (* Update the remote index with the new index *)
  /\ remoteVersion' = localVersion[n] + 1  (* Increment the remote version *)
  /\ clientVersion' = localVersion[n] + 1  (* Update the client version *)

(*
  The `Client` action simulates a client calling Read and Write and collecting the returned version.
  - The client ensures subsequent calls get identical or larger versions.
*)
Client ==
  \E n \in Nodes:
    \E newIndex \in 0..MaxVersion:
      Read(n) \/ Write(n, newIndex)

(*
  The `Next` relation defines the possible state transitions in the system.
  - Includes the `Client` action.
*)
Next ==
  Client

(*
  The `Invariant` defines a property that must always hold.
  - The local version of any node must be at least as recent as the remote version.
  - The client version sequence must be non-decreasing.
*)
Invariant ==
  \A n \in Nodes:
    /\ localVersion[n] >= remoteVersion
    /\ \A i \in 1..(Len(clientVersion) - 1):
        clientVersion[i] <= clientVersion[i + 1]

====

