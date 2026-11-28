package meta

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strconv"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/hashicorp/raft"
	"github.com/mulgadc/predastore/internal/gate/model"
	"github.com/mulgadc/predastore/internal/rpc"
	"github.com/mulgadc/predastore/internal/transport"
)

var (
	errPlacementChanged      = errors.New("placement changed before retirement")
	errPlacementLive         = errors.New("cannot retire the live placement epoch")
	errPlacementRetired      = errors.New("placement epoch has been retired")
	errInvalidPlacement      = errors.New("invalid placement for retirement")
	errRetirementUnavailable = errors.New("placement retirement requires support from every voter")
)

const placementRetirementPrefix = "\x00raft/placement-retired/"

func placementRetirementKey(key []byte, epoch uint64) []byte {
	row := make([]byte, 0, len(placementRetirementPrefix)+len(key)+8)
	row = append(row, placementRetirementPrefix...)
	row = append(row, key...)
	return binary.BigEndian.AppendUint64(row, epoch)
}

func isPlacementKey(key []byte) bool {
	name, ok := bytes.CutPrefix(key, []byte(model.TableObjects+"/"))
	if !ok || bytes.HasPrefix(name, []byte("arn:aws:s3:::")) ||
		bytes.HasPrefix(name, []byte("deleted:")) || bytes.HasPrefix(name, []byte("deleted-epoch:")) {
		return false
	}
	const partPrefix = "part:"
	const uuidLength = 36
	const partNumberLength = 5
	return len(name) == sha256.Size ||
		(len(name) == len(partPrefix)+uuidLength+1+partNumberLength &&
			bytes.HasPrefix(name, []byte(partPrefix)) && name[len(partPrefix)+uuidLength] == ':')
}

func placementIsRetired(txn *badger.Txn, key []byte, epoch uint64) (bool, error) {
	_, err := txn.Get(placementRetirementKey(key, epoch))
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, badger.ErrKeyNotFound):
		return false, nil
	default:
		return false, &storeError{err}
	}
}

func refuseRetiredPlacement(txn *badger.Txn, key, value []byte) error {
	if !isPlacementKey(key) {
		return nil
	}
	epoch, ok := model.PlacementEpoch(value)
	if !ok || epoch == 0 {
		return nil
	}
	retired, err := placementIsRetired(txn, key, epoch)
	if retired {
		return errPlacementRetired
	}
	return err
}

func (f *FSM) retirePlacement(txn *badger.Txn, cmd Command) error {
	if cmd.Epoch == 0 || !isPlacementKey(cmd.Key) {
		return errInvalidPlacement
	}
	current, found, err := f.readValue(txn, cmd.Key)
	if err != nil {
		return err
	}
	if found {
		epoch, ok := model.PlacementEpoch(current)
		if !ok {
			return errInvalidPlacement
		}
		if epoch == cmd.Epoch {
			return errPlacementLive
		}
	}
	retired, err := placementIsRetired(txn, cmd.Key, cmd.Epoch)
	if err != nil || retired {
		return err
	}
	if found != (len(cmd.Value) != 0) || !bytes.Equal(current, cmd.Value) {
		return errPlacementChanged
	}
	return txn.Set(placementRetirementKey(cmd.Key, cmd.Epoch), nil)
}

func (s *Server) handleRetirePlacement(ctx context.Context, req MetaRequest, stream transport.Stream) error {
	if req.Epoch == 0 || !isPlacementKey(req.Key) {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}
	observed, err := io.ReadAll(io.LimitReader(stream, MaxTxnBodyBytes+1))
	if err != nil {
		return err
	}
	if len(observed) > MaxTxnBodyBytes {
		return respond(stream, &MetaResponse{Err: ErrCodeInvalid})
	}
	if err := s.confirmRetirementSupport(ctx); err != nil {
		return respond(stream, s.writeResult(err))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return respond(stream, s.writeResult(s.apply(Command{
		Type: CommandRetirePlacement, Key: req.Key, Epoch: req.Epoch, Value: observed,
	})))
}

func (s *Server) confirmRetirementSupport(ctx context.Context) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	term := s.raft.CurrentTerm()
	configured := s.raft.GetConfiguration()
	if err := configured.Error(); err != nil {
		return err
	}
	servers := configured.Configuration().Servers
	if term == 0 || len(servers) == 0 {
		return errRetirementUnavailable
	}
	client, err := NewClient(ClientConfig{Client: rpc.NewClient(s.pool), Replicas: s.cfg.Peers})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	leader := RaftAddress(s.cfg.NodeID)
	for _, server := range servers {
		if server.Suffrage != raft.Voter || string(server.Address) == leader {
			continue
		}
		id, err := ParseRaftAddress(string(server.Address))
		if err != nil {
			return errRetirementUnavailable
		}
		status, err := client.Status(ctx, id)
		if err != nil || !status.PlacementRetirement || status.NodeID != string(server.ID) ||
			status.Term != strconv.FormatUint(term, 10) || status.Leader != leader {
			return errRetirementUnavailable
		}
	}
	if s.raft.State() != raft.Leader || s.raft.CurrentTerm() != term {
		return ErrNotLeader
	}
	after := s.raft.GetConfiguration()
	if err := after.Error(); err != nil {
		return err
	}
	if after.Index() != configured.Index() || !slices.Equal(servers, after.Configuration().Servers) {
		return errRetirementUnavailable
	}
	return nil
}
