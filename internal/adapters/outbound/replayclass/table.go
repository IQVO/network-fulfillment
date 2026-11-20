// Package replayclass is the vocabulary an outbound adapter uses to read a
// fleet callee's refusals (ADR 0037).
//
// A callee on an idempotent route declares a replay class for every problem
// type it answers (the fleet rule of 2026-11-18). A RETRYABLE refusal committed
// nothing, is NOT stored and releases the Idempotency-Key: the same request
// under the SAME key executes again and may pass. A FINAL or OUTCOME refusal is
// stored and replayed, so asking again only repeats it. The class is the
// callee's knowledge: each adapter keeps ONE visible Table of its own callee,
// derived from the callee's class table, and the use cases above only see
// "released or not".
//
// A slug that is not in a table is terminal: an unknown refusal must never
// loop.
package replayclass

import (
	"encoding/json"
	"sort"
	"strings"
)

// Class is how this service treats one problem slug of a callee.
type Class uint8

const (
	// Unset is the zero value: the slug is not in the table, which reads as
	// terminal.
	Unset Class = iota
	// Terminal is a FINAL or OUTCOME refusal of the callee, or one this caller
	// can never get past: it is a verdict.
	Terminal
	// Retryable is a RETRYABLE refusal of the callee: the step stays pending.
	Retryable
)

// Table maps a problem slug of one callee to its class here.
type Table map[string]Class

// NewTable builds a table from the slugs the callee releases and the slugs it
// stores (or that this caller reads as a verdict). It panics when a slug is
// listed twice or is empty: a table that disagrees with itself is a
// programming error, caught when the package loads.
func NewTable(retryable, terminal []string) Table {
	t := Table{}
	add := func(slugs []string, c Class) {
		for _, s := range slugs {
			if s == "" {
				panic("replayclass: the empty slug cannot be classified")
			}
			if _, dup := t[s]; dup {
				panic("replayclass: " + s + " is classified twice")
			}
			t[s] = c
		}
	}
	add(retryable, Retryable)
	add(terminal, Terminal)
	return t
}

// Class returns the class of a slug, Unset when it is not in the table.
func (t Table) Class(slug string) Class { return t[slug] }

// Retryable reports whether the callee releases the slug under the key. Only a
// listed slug is: unknown reads as terminal.
func (t Table) Retryable(slug string) bool { return t[slug] == Retryable }

// Slugs lists the slugs of one class, sorted.
func (t Table) Slugs(c Class) []string {
	var out []string
	for s, got := range t {
		if got == c {
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

// ProblemOf reads an application/problem+json body: the slug is the last path
// element of `type`, the detail is `detail`. A body that is not a problem
// document yields empty strings.
func ProblemOf(body []byte) (slug, detail string) {
	var p struct {
		Type   string `json:"type"`
		Detail string `json:"detail"`
	}
	_ = json.Unmarshal(body, &p)
	if i := strings.LastIndexByte(p.Type, '/'); i >= 0 {
		return p.Type[i+1:], p.Detail
	}
	return p.Type, p.Detail
}
