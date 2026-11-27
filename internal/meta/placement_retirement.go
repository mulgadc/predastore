package meta

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/dgraph-io/badger/v4"
	"github.com/mulgadc/predastore/internal/gate/model"
)

var (
	errPlacementChanged = errors.New("placement changed before retirement")
	errPlacementLive    = errors.New("cannot retire the live placement epoch")
	errPlacementRetired = errors.New("placement epoch has been retired")
	errInvalidPlacement = errors.New("invalid placement for retirement")
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
