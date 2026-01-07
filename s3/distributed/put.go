package distributed

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/davecgh/go-spew/spew"
	"github.com/dgraph-io/badger/v4"
	"github.com/gofiber/fiber/v2"
	s3db "github.com/mulgadc/predastore/s3db"
)

func (backend Backend) PutObjectPart(bucket string, object string, partNumber int, uploadId string, c *fiber.Ctx) {

}

func (backend Backend) PutObject(bucket string, object string, c *fiber.Ctx) (err error) {

	fmt.Println(bucket, object)

	objectHash := s3db.GenObjectHash(bucket, object)

	objectToShardNodes := ObjectToShardNodes{}

	// Check if existing
	data, err := backend.DB.Get(objectHash[:])

	if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return err
	} else if errors.Is(err, badger.ErrKeyNotFound) {

		// Set the defaults
		objectToShardNodes = ObjectToShardNodes{
			Object:           objectHash,
			DataShardNodes:   make([]uint32, backend.RsDataShard),
			ParityShardNodes: make([]uint32, backend.RsParityShard),
		}
	} else {

		r := bytes.NewReader(data)
		dec := gob.NewDecoder(r)

		if err := dec.Decode(&objectToShardNodes); err != nil {
			return err
		}

		//spew.Dump(data)
		//spew.Dump(objectToShardNodes)

	}

	//objectSha256 := hex.EncodeToString(hashSha256[:])

	d, p, size, err := backend.putObjectToWAL(bucket, object, objectHash)

	spew.Dump(d)
	spew.Dump(p)

	if err != nil {
		return err
	}

	objectToShardNodes.Size = size

	_, file := filepath.Split(object)
	key := []byte(fmt.Sprintf("%s/%s", bucket, file))
	hashRingShards, _ := backend.HashRing.GetClosestN(key, backend.RsDataShard+backend.RsParityShard)

	// Print the WAL location results for now (Badger KV later).
	for i := 0; i < backend.RsDataShard; i++ {

		//fmt.Printf("put_object wal_write data_shard=%d node=%s write_result=%#v\n",
		//	i, hashRingShards[i].String(), dataRes[i])

		objectToShardNodes.DataShardNodes[i], err = NodeToUint32(hashRingShards[i].String())

		if err != nil {
			return err
		}

	}
	for i := 0; i < backend.RsParityShard; i++ {

		//fmt.Printf("put_object wal_write parity_shard=%d node=%s write_result=%#v\n",
		//	i, hashRingShards[backend.RsDataShard+i].String(), parityRes[i])

		objectToShardNodes.ParityShardNodes[i], err = NodeToUint32(hashRingShards[backend.RsDataShard+i].String())

		if err != nil {
			return err
		}

	}

	err = backend.DB.Badger.Update(func(txn *badger.Txn) error {

		// Marshal objectToShardNodes to []byte
		/*
			objectToShardNodesBytes, err := json.Marshal(objectToShardNodes)
			if err != nil {
				return err
			}
		*/

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		if err := enc.Encode(objectToShardNodes); err != nil {
			return err
		}

		//err = txn.Set(objectHash[:], objectToShardNodesBytes)
		e := badger.NewEntry(objectHash[:], buf.Bytes())
		err = txn.SetEntry(e)

		return err
	})

	return err

}
