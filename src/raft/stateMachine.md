# TASKS

[x] Receive a Request Vote Request
    
 - term < currentTerm
    - REJECT cause it is an old entry and return

 - term > currentTerm 
    - set the currentTerm to term and become a FOLLOWER by resetting votedFor etc. except lastPing (this should only update if we grant the vote, else it might lead to starvation where one less updated server starts election with higher term and keeps on conducting election cause it does not get majority and this server won't ever be able to start one cause its lastPing is reset on every election due to term increase)
    
    this condition is important cause if can be possible that reconnected node is starting
    election and has higher term, to be in sync with the largest term we can set ours to match it but we cannot vote yet cause the logs might not match
    
    - we check the below condition then

 - term == currentTerm
    - if voteFor is -1 || voteFor == candidateId  && (lastTerm == myLastLogIndex  and lastLogIndex >= myLastLogIndex)
        - it means logs are up to date and we can GRANT vote to this candidate and reset lastPing
    - else
        - REJECT cause either logs are not up to date or we have voted for someone else


[x] Receive an Append Entry Request

 - term < currentTerm
    - REJECT cause it is an old entry and return
    
    reset the lastPing (this request serves as an heartbeat if entries are empty)

 - term > currentTerm
    - set the currentTerm to term and become a FOLLOWER by resetting params like ping, etc.
    this condition is needed cause when a leader which has reconnected and got a higher term request, then it means that a new leader is present which is more recent.

 - if lastLogTerm != prevLogTerm OR lastLogIndex < prevLogIndex
    - current log is not up to date with the leader. Need previous entries so set success to FALSE and return

 - we know that previous logs of leader and our match so we start adding new logs
    - if we get a conflicting entry at an index then overwrite the current log and set the flag to true
    - if we get a entry higher than the log length append the new log

 - if the flag was set
    - find the last index of the entries sent by the leader
    - after this last index delete all the rest of the entries 
    (log should be consistent with the leader, the entries after this are of old term and needs to be deleted)

 - set the commitIndex = min(leaderCommit, lastLogIndex)

 - apply the logs between lastApplied+1 and commitIndex to the stateMachine


[x] Request Vote Reply
 - if state != candidate or current Term != args term (term for which peer started election)
    - return no processing left as someone else has become the leader
 - if voteGranted is true
    - increement voteCount
 - if voteCount >= majority
    - become the leader
    - set the reset the next indices and match indices to len of log and 0 respectively.
    - send append entries
 - we can move back to follower if we won't be able to receive majority.
    
[x] Append Entries Reply
 - if state != leader || args term != current Term || reply term < current term
    - return 
    someone else must have been the leader or the reply is delayed and is of no use anymore for this term

 - if reply term > current term
    - step down to follower state and return
    someone might be latest, if not we need to sync the term, it does not matter if we are the latest and step down as follower because eventually election would be held and the peer with the consistent state is only going to be elected.

 - if reply is success
    - update the match index and next index for that peer
 - else
    - decrease the next index for that peer
    
 - find the majority max commit index using match indices
    - if majority is achieved and that commit index is for the current term
        - update the commit index

 - apply the commits in a different go routine because we should not wait for updates on state machine before processing our role as a leader.
    

