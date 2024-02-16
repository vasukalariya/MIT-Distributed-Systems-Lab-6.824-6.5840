# Description

This project is for MIT CS 6.5840 Spring 2023 labs. But, the same applies for MIT 6.824 labs.
Learned the concepts through MIT CS 6.824 Spring 2020 lectures on YouTube.

- [x] Lab 1: Map Reduce

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
 - Remember to add a random component to election timeout.
 - While granting vote the receiver of RPC should update its current Term to match the leader/candidate.

- [x] Lab 2B RAFT: Log

Takeaways:
 - I would emphasize to create a state diagram for better clarity and design.
 - Work out the condition before you actually implement them.
 - Once you implement and receive errors, try adding print statements in the test to observe the point of failures.
 - Use Dprint in util.go to prevent commenting/uncommenting print statements.
 - Run the test for atleast 500 times to prevent unseen errors, can use the go-many-test.sh. 

- [x] Lab 2C RAFT: Persistence 

Takeaways:
 - If 2A and 2B are done properly, this would not take much time.
 - Follow the papers directions for which parameters to persist.
 - Intiutively, if we save the log, votedFor and currentTerm only, whenever we backup we would be able to join the cluster cause the states would explicitly be defined through either election, append entries or request vote RPCs.


- [x] Lab 2D RAFT: Log Compaction 

Takeaways:
 - Be careful, of sending the apply messages while holding the lock.
 - Remember to discard entries if the last log index are same in InstallSnapshot but terms are different.
 - A good trick is to not remove the index on which snapshot is called, it would make your implementation cleaner as we have started with an empty entry at index 0 as well as no necessary checks needed for empty log entries.


- [x] Lab 3A: Key/Value service without snapshots 

Takeaways:
 - Remember that channels used to pass message between the operations applier and RPC handler should have buffer size of 1. If it is 0 then you could get stuck as all request would need to be ordered sequentially and one delay can timeout all the pending requests.
 - Once you receive the operation from applier to the RPC handler after updating key value store, the operation sent and received needs to be compared because it can be possible that the current server is no longer a leader and message received on the applier is modified due to some other leader.
 - Use the visualize.py script to analyze logs. I have created this to visualize the logs better as you can assign colors to servers or clients and separately focus on that section of the logs.
 - Always check if the received command is applied or not. You can track it by having a lastApplied index and ignoring the messages that have already been applied or the request ids that have already been handled for that client.
 - Be patient with the debugging and format the logs in specific order so that errors are easy to spot.
 - Moreover, you can ignore the requests that have already been applied in the initial check of the RPC handler. Only send those operations to the RAFT if they are not applied yet.


- [x] Lab 3B: Key/value service with snapshots 

Takeaways:
 - Be sure to check for snapshots that are recevied on the applyMsg channel.
 - Make sure to restore the state when KVserver is restarted
 - Do check for raft state in every update received to make sure that raft state does not increase much and you can call the snapshot as soon as minimum threshold i.e. maxraftstate is reached.



