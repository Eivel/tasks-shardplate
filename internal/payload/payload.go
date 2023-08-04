package payload

import "github.com/google/uuid"

// Identifiable represents a universal resource that returns its type with the Payload() method and
// its identifier with ID() method. The identifiers are universally unique.
type Identifiable interface {
	Payload() any
	ID() *uuid.UUID
}

// Payload represents the payload passed to the application via HTTP requests.
type Payload struct {
	Identifier *uuid.UUID `json:"-"`
	Timestamp  string     `json:"timestamp"`
	Value      *string    `json:"value"`
}

func (p Payload) ID() *uuid.UUID {
	return p.Identifier
}

func (p Payload) Payload() any {
	return p
}
