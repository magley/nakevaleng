package tokenbucket

import (
	"fmt"
	"time"
)

// TODO: Make Timestamp and ResetInterval configurable
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
func New(maxTokens int, resetInterval int64) *TokenBucket {
	if maxTokens <= 0 {
		fmt.Println("ERROR: maxTokens must be a positive number, but ", maxTokens, " was given.")
		panic(nil)
	}
	if resetInterval <= 0 {
		fmt.Println("ERROR: resetInterval must be a positive number, but ", resetInterval, " was given.")
		panic(nil)
	}

	return &TokenBucket{
		MaxTokens:     maxTokens,
		Tokens:        maxTokens,
		Timestamp:     time.Now().Unix(),
		ResetInterval: resetInterval,
	}
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
