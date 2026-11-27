package meta

import (
	"context"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/hashicorp/raft"
)

type authorityProofState struct {
	term   uint64
	voters []raft.Server
	peers  map[raft.ServerID]*atomic.Bool
}

func (s *Server) freshAuthorityRead() string {
	term := s.raft.CurrentTerm()
	if term == 0 || s.raft.State() != raft.Leader {
		return ErrCodeNotLeader
	}

	voters := s.raft.GetConfiguration().Configuration().Servers
	proof := &s.authorityProof
	if proof.term != term || !slices.Equal(voters, proof.voters) {
		return s.initializeAuthorityProof(term, voters)
	}

	index := s.raft.CommitIndex()
	ctx, cancel := context.WithTimeout(context.Background(), leaderReadWait)
	defer cancel()
	if !s.confirmAuthorityQuorum(ctx, term, voters) {
		return ErrCodeBehind
	}
	if !s.waitForAuthorityApply(ctx, index) {
		return ErrCodeBehind
	}
	if term != s.raft.CurrentTerm() || s.raft.State() != raft.Leader ||
		!slices.Equal(voters, s.raft.GetConfiguration().Configuration().Servers) {
		return ErrCodeNotLeader
	}
	return ""
}

func (s *Server) initializeAuthorityProof(term uint64, voters []raft.Server) string {
	code := s.leaderRead()
	if code != "" || term != s.raft.CurrentTerm() || s.raft.State() != raft.Leader {
		if code == "" {
			return ErrCodeNotLeader
		}
		return code
	}
	if !s.fixedAuthorityVoters(voters) || !slices.Equal(voters, s.raft.GetConfiguration().Configuration().Servers) {
		return code
	}
	proof := &s.authorityProof
	if proof.peers == nil {
		proof.peers = make(map[raft.ServerID]*atomic.Bool, len(voters))
	}
	for _, voter := range voters {
		if proof.peers[voter.ID] == nil {
			proof.peers[voter.ID] = new(atomic.Bool)
		}
	}
	proof.term = term
	proof.voters = slices.Clone(voters)
	return code
}

func (s *Server) fixedAuthorityVoters(voters []raft.Server) bool {
	if len(voters) == 0 || len(voters) != len(s.cfg.Peers) {
		return false
	}
	containsSelf := false
	for _, peer := range s.cfg.Peers {
		containsSelf = containsSelf || peer == s.cfg.NodeID
		want := raft.Server{
			ID:       raft.ServerID(strconv.FormatUint(uint64(peer), 10)),
			Address:  raft.ServerAddress(RaftAddress(peer)),
			Suffrage: raft.Voter,
		}
		if !slices.Contains(voters, want) {
			return false
		}
	}
	return containsSelf
}

func (s *Server) confirmAuthorityQuorum(ctx context.Context, term uint64, voters []raft.Server) bool {
	needed := len(voters) / 2
	if needed == 0 {
		return true
	}

	self := raft.ServerID(strconv.FormatUint(uint64(s.cfg.NodeID), 10))
	addr := s.transport.EncodePeer(self, s.transport.LocalAddr())
	req := raft.AppendEntriesRequest{
		RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax, ID: []byte(self), Addr: addr},
		Term:      term,
	}
	replies := make(chan bool, len(voters)-1)
	pending := 0
	for _, voter := range voters {
		slot := s.authorityProof.peers[voter.ID]
		if voter.ID == self || !slot.CompareAndSwap(false, true) {
			continue
		}
		pending++
		go func() {
			var resp raft.AppendEntriesResponse
			err := s.transport.AppendEntries(voter.ID, voter.Address, &req, &resp)
			slot.Store(false)
			replies <- err == nil && resp.Success && resp.Term == term && string(resp.ID) == string(voter.ID)
		}()
	}
	for pending >= needed {
		select {
		case <-ctx.Done():
			return false
		case ok := <-replies:
			pending--
			if ok {
				needed--
				if needed == 0 {
					return true
				}
			}
		}
	}
	return false
}

func (s *Server) waitForAuthorityApply(ctx context.Context, index uint64) bool {
	applied := func() uint64 {
		return max(s.fsm.applied.Load(), s.authorityAppliedBarrier.Load())
	}
	for applied() < index {
		done := make(chan struct{})
		s.fsm.authorityWaiter.Store(&done)
		if applied() >= index {
			s.fsm.authorityWaiter.CompareAndSwap(&done, nil)
			return true
		}
		select {
		case <-done:
		case <-ctx.Done():
			s.fsm.authorityWaiter.CompareAndSwap(&done, nil)
			return false
		}
	}
	return true
}

func (f *FSM) signalAuthorityApply() {
	if waiter := f.authorityWaiter.Swap(nil); waiter != nil {
		close(*waiter)
	}
}
