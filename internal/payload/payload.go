package payload

import "github.com/google/uuid"

type Identifiable interface {
	Payload() any
	ID() *uuid.UUID
}

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
