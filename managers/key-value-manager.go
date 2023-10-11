package managers

import (
	"encoding/json"
	"io"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

type KeyValueSnapshot struct {
	Values map[string]string `json:"values"`
}

func (s *KeyValueSnapshot) Persist(sink raft.SnapshotSink) error {
	bytes, err := json.Marshal(s.Values)

	if err != nil {
		sink.Cancel()
		return err
	}

	_, err = sink.Write(bytes)

	if err != nil {
		sink.Cancel()
		return err
	}

	if err := sink.Close(); err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

func (s *KeyValueSnapshot) Release() {}

type KeyValueManager struct {
	raft   *raft.Raft
	values map[string]string
}

type Operation = string

const (
	OpSet    Operation = "set"
	OpDelete Operation = "delete"
)

type command struct {
	Op Operation `json:"op"`
}

type SetCommand struct {
	Op    Operation `json:"op"`
	Key   string    `json:"key"`
	Value string    `json:"value"`
}

type DeleteCommand struct {
	Op  Operation `json:"op"`
	Key string    `json:"key"`
}

func (manager *KeyValueManager) GetAll() map[string]string {
	return manager.values
}

func (manager *KeyValueManager) Get(key string) (string, bool) {
	value, ok := manager.values[key]

	return value, ok
}

func (manager *KeyValueManager) Set(key, value string) error {
	cmd := SetCommand{
		Op:    OpSet,
		Key:   key,
		Value: value,
	}

	data, _ := json.Marshal(cmd)

	return manager.raft.Apply(data, 10*time.Second).Error()
}

func (manager *KeyValueManager) Delete(key string) error {
	cmd := DeleteCommand{
		Op:  OpDelete,
		Key: key,
	}

	data, _ := json.Marshal(cmd)

	return manager.raft.Apply(data, 10*time.Second).Error()
}

func (manager *KeyValueManager) Apply(l *raft.Log) interface{} {
	command := command{}

	err := json.Unmarshal(l.Data, &command)

	if err != nil {
		log.Fatalf("Unable to unmarshal command: %v", err)
	}

	switch command.Op {
	case OpSet:
		setCommand := SetCommand{}

		err := json.Unmarshal(l.Data, &setCommand)

		if err != nil {
			log.Fatalf("Unable to unmarshal command: %v", err)
		}

		manager.values[setCommand.Key] = setCommand.Value

		return nil
	case OpDelete:
		deleteCommand := DeleteCommand{}

		err := json.Unmarshal(l.Data, &deleteCommand)

		if err != nil {
			log.Fatalf("Unable to unmarshal command: %v", err)
		}

		delete(manager.values, deleteCommand.Key)

		return nil
	default:
		log.Fatalf("Unknown command: %v", command.Op)
		return nil
	}
}

func (manager *KeyValueManager) Snapshot() (raft.FSMSnapshot, error) {
	return &KeyValueSnapshot{
		Values: manager.values,
	}, nil
}

func (manager *KeyValueManager) Restore(rc io.ReadCloser) error {
	bytes, err := io.ReadAll(rc)

	if err != nil {
		return err
	}

	if err := json.Unmarshal(bytes, &manager.values); err != nil {
		return err
	}

	return nil
}

func (manager *KeyValueManager) SetRaft(raft *raft.Raft) {
	manager.raft = raft
}

func (manager *KeyValueManager) GetRaft() *raft.Raft {
	return manager.raft
}

func NewKeyValueManager() *KeyValueManager {
	return &KeyValueManager{
		raft:   nil,
		values: make(map[string]string),
	}
}
