package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	Engine *engine_util.Engines
	Config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	log.Infof("call NewStandAloneStorage(), conf.DBPath = %v", conf.DBPath)
	//create subPath "kv" and "raft" for new Badger DB on disk
	kvEngine := engine_util.CreateDB("kv", conf)
	raftEngine := engine_util.CreateDB("raft", conf)
	//subPaths
	kvPath := conf.DBPath + "/kv"
	raftPath := conf.DBPath + "/raft"
	//return *StandAloneStorage which has NewEngines and Config
	return &StandAloneStorage{
		Engine: engine_util.NewEngines(kvEngine, raftEngine, kvPath, raftPath),
		Config: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("call s.Start()")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	log.Info("call s.Stop()")
	err := s.Engine.Close()
	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	log.Infof("call s.Reader(), ctx = %v", ctx)
	// ctx is nil in project1 test
	return &StandAloneStorageReader{
		// use read-only transaction
		KvTxn:   s.Engine.Kv.NewTransaction(false),
		RaftTxn: s.Engine.Raft.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	log.Infof("call s.Write(), ctx = %v", ctx)
	// a new transaction
	var txn *badger.Txn
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			//solve put transaction
			put := m.Data.(storage.Put)
			// use read-write transaction
			switch put.Cf {
			case "raft":
				txn = s.Engine.Raft.NewTransaction(true)
			default:
				txn = s.Engine.Kv.NewTransaction(true)
			}
			// set key and value. actual key is ${cf}+${key}
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
			// commit this transaction
			if err := txn.Commit(); err != nil {
				return err
			}
		case storage.Delete:
			//solve delete transaction
			delete := m.Data.(storage.Delete)
			// use read-write transaction
			switch delete.Cf {
			case "raft":
				txn = s.Engine.Raft.NewTransaction(true)
			default:
				txn = s.Engine.Kv.NewTransaction(true)
			}
			//delete by key. actual key is ${cf}+${key}
			if err := txn.Delete(engine_util.KeyWithCF(delete.Cf, delete.Key)); err != nil {
				return err
			}
			// commit this transaction
			if err := txn.Commit(); err != nil {
				return err
			}
		}
	}
	return nil
}

type StandAloneStorageReader struct {
	// Your Data Here (1).
	KvTxn   *badger.Txn
	RaftTxn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	log.Infof("call sr.GetCF(), cf = %v, key = %v", cf, key)
	var (
		val []byte
		err error
	)
	//Txn.Get() returns ErrKeyNotFound if the value is not found.
	switch cf {
	case "raft":
		val, err = engine_util.GetCFFromTxn(sr.RaftTxn, cf, key)
	default:
		val, err = engine_util.GetCFFromTxn(sr.KvTxn, cf, key)
	}
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	log.Infof("call sr.IterCF(), cf = %v", cf)
	//NewCFIterator returns *BadgerIterator, which implement the engine_util.DBIterator interface.
	var dbIter engine_util.DBIterator
	//log.Info(cf)
	switch cf {
	case "raft":
		dbIter = engine_util.NewCFIterator(cf, sr.RaftTxn)
	default:
		dbIter = engine_util.NewCFIterator(cf, sr.KvTxn)
	}
	return dbIter
}

func (sr *StandAloneStorageReader) Close() {
	log.Info("call sr.Close()")
	sr.KvTxn.Discard()
	sr.RaftTxn.Discard()
}
