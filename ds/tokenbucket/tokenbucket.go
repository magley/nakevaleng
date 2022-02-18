package tokenbucket

import (
	"encoding/binary"
	"fmt"
	"time"
)

// TODO: Make MaxTokens and ResetInterval configurable
type TokenBucket struct {
	MaxTokens     int
	Tokens        int
	Timestamp     int64
	ResetInterval int64 // type is int64 to avoid casting in HasEnoughTokens
}

// New creates a pointer to a TokenBucket object.
//  maxTokens        Maximum amomunt of tokens to be stored in the TokenBucket
//  resetInterval    Length of time (in seconds) for which the current timestamp is valid
//  returns          Pointer to a TokenBucket object
// Throws if the maxTokens and/or resetInterval parameters are not positive numbers.
func New(maxTokens int, resetInterval int64) (*TokenBucket, error) {
	err := ValidateParams(maxTokens, int(resetInterval))
	if err != nil {
		return nil, err
	}

	return &TokenBucket{
		MaxTokens:     maxTokens,
		Tokens:        maxTokens,
		Timestamp:     time.Now().Unix(),
		ResetInterval: resetInterval,
	}, nil
}

func ValidateParams(maxTokens, resetInterval int) error {
	if maxTokens <= 0 {
		err := fmt.Errorf("maxTokens must be a positive number, but %d was given", maxTokens)
		return err
	}
	if resetInterval <= 0 {
		err := fmt.Errorf("resetInterval must be a positive number, but %d was given", resetInterval)
		return err
	}

	return nil
}

// HasEnoughTokens checks if the TokenBucket has enough tokens for the user's request.
// If enough time has passed from the original timestamp, a new one will be created and the token count updated.
//  returns    boolean indicating if there are enough tokens in the TokenBucket
func (tb *TokenBucket) HasEnoughTokens() bool {
	if currentTimestamp := time.Now().Unix(); currentTimestamp-tb.Timestamp > tb.ResetInterval {
		tb.Timestamp = currentTimestamp
		tb.Tokens = tb.MaxTokens - 1 // we immediately subtract the requested token
		return true
	}

	if tb.Tokens > 0 {
		tb.Tokens--
		return true
	}

	return false
}

// ToBytes is temporary function todo
func (tb *TokenBucket) ToBytes() []byte {
	ret := make([]byte, 32)
	binary.LittleEndian.PutUint64(ret[0:8], uint64(tb.MaxTokens))
	binary.LittleEndian.PutUint64(ret[8:16], uint64(tb.Tokens))
	binary.LittleEndian.PutUint64(ret[16:24], uint64(tb.Timestamp))
	binary.LittleEndian.PutUint64(ret[24:32], uint64(tb.ResetInterval))
	return ret
}

func FromBytes(bytes []byte) TokenBucket {
	return TokenBucket{
		int(binary.LittleEndian.Uint64(bytes[0:8])),
		int(binary.LittleEndian.Uint64(bytes[8:16])),
		int64(binary.LittleEndian.Uint64(bytes[16:24])),
		int64(binary.LittleEndian.Uint64(bytes[24:32])),
	}
}
