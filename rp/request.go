// Copyright © 2017 Genome Research Limited
// Author: Sendu Bala <sb10@sanger.ac.uk>.
//
//  This file is part of wr.
//
//  wr is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  wr is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public License
//  along with wr. If not, see <http://www.gnu.org/licenses/>.

package rp

// This file contains the implementation of request and Receipt.

import (
	"sync"
	"time"
)

// Receipt is the unique id of a request
type Receipt string

// request struct describes a request for tokens tied to a particular resource
// protector.
type request struct {
	id          Receipt
	owner       string
	numTokens   int
	grantedCh   chan bool
	releaseCh   chan bool
	touchCh     chan bool
	autoRelease time.Duration
	active      bool
	done        bool
	mu          sync.Mutex
}

// waitUntilGranted blocks until the Protector that created us sends on our
// grantedCh. Returns false if already granted or finished().
func (r *request) waitUntilGranted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active || r.done {
		return false
	}
	r.active = true
	<-r.grantedCh
	return true
}

// touch sends on our touchCh, which will be read by the Protector that granted
// our tokens to stop it timing out and auto-releasing.
func (r *request) touch() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active || r.done {
		return
	}
	r.touchCh <- true
}

// release sends on our releaseCh, which will be read by the Protector that
// granted our tokens. Finally does the equivalent of finished().
func (r *request) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.active || r.done {
		return
	}
	r.done = true
	r.releaseCh <- true
}

// finished stops the other methods from doing anything.
func (r *request) finished() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done = true
}
