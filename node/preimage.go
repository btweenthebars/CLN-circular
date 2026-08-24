package node

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

type PreimageHashPair struct {
	Preimage string `json:"preimage"`
	Hash     string `json:"hash"`
}

func NewPreimageHashPair() (PreimageHashPair, error) {
	preimage := make([]byte, 32)
	if _, err := rand.Read(preimage); err != nil {
		return PreimageHashPair{}, err
	}
	hash := sha256.Sum256(preimage)

	return PreimageHashPair{
		Preimage: hex.EncodeToString(preimage),
		Hash:     hex.EncodeToString(hash[:]),
	}, nil
}

func (n *Node) GeneratePreimageHashPair() (string, error) {
	pair, err := NewPreimageHashPair()
	if err != nil {
		return "", err
	}
	err = n.DB.Set(pair.Hash, []byte(pair.Preimage))
	if err != nil {
		return "", err
	}
	return pair.Hash, nil
}
