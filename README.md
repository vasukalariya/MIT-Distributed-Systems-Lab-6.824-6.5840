# Distributed-Systems-Lab-MIT-6.5840
- [x] Map Reduce

Takeaways:
 - Check the capitalization of struct member names and methods for visibility scopes.
 - Use some sort of exit notification from coordinator when worker pings coordinator and there are no tasks available
 - You should use go routines to track the progress of tasks. Hint: Sleep

- [x] Lab 2A RAFT: Leader Election

Takeaways:
 - Check the locking, never send RPC call while locked (can cause deadlock).
 - Better to request votes in parallel using go routines.
 - It is important to wait for all votes to be returned or till the majority is achieved.
 - Remember to reset timeouts where necessary in append entries and request votes.

