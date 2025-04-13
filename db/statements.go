package db

import (
	_ "embed"
)

type StatementSet interface {
	Init() []Mutation
	Get(key string) Query
	GetPrefix(prefix string, offset, limit int) Query
	GetRange(from, to string, offset, limit int) Query
	List(offset, limit int) Query
	Put(key string, version int64, value any) Mutation
	PutPatches(operations ...PutPatchInput) Mutation
	DeleteKeys(keys ...string) Mutation
	Delete(key string) Mutation
	DeletePrefix(prefix string, offset, limit int) Mutation
	DeleteRange(from, to string, offset, limit int) Mutation
	Count() Query
	CountPrefix(prefix string) Query
	CountRange(from, to string) Query
	Patch(key string, version int64, patch any) Mutation
}

type PutPatchInput struct {
	Key       string    `json:"key"`
	Version   int64     `json:"version"`
	Value     any       `json:"value"`
	Operation Operation `json:"operation"`
}

type Operation string

var OperationPut Operation = "put"
var OperationPatch Operation = "patch"

func PutInput(key string, version int64, value any) PutPatchInput {
	return PutPatchInput{
		Key:       key,
		Version:   version,
		Value:     value,
		Operation: OperationPut,
	}
}

func PatchInput(key string, version int64, patch any) PutPatchInput {
	return PutPatchInput{
		Key:       key,
		Version:   version,
		Value:     patch,
		Operation: OperationPatch,
	}
}
