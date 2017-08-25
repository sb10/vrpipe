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

// This file contains the implementation of the main struct in the rp package,
// the Protector.

import (
	"github.com/satori/go.uuid"
	"sync"
	"time"
)

// Protector struct is used to Protect a particular resource by granting tokens
// tokens when the resource has capacity.
type Protector struct {
	Name           string // Name of the resource being protected.
	maxTokens      int
	usedTokens     int
	delayBetween   time.Duration
	releaseTimeout time.Duration
	requests       map[Receipt]*request
	pending        []*request
	lastProcess    time.Time
	reprocessing   bool
	availabilityCb AvailabilityCallback
	mu             sync.RWMutex
}

// New creates a new Protector. The name is for your benefit, describing the
// resource you're protecting.
//
// delayBetween defines the minimum delay between the granting of the tokens for
// each Request(). This would be used to avoid spamming your resource with too
// high a frequency of accesses.
//
// maxSimultaneous defines the maximum number of tokens that can be in use
// concurrently. This would be used to avoid overloading your resource with too
// much usage.
//
// releaseTimeout is the time after which granted tokens are automatically
// released for use by other requests if the receiver fails to Touch() or
// Release() before then (so that a client that starts using tokens but then
// dies unexpectedly doesn't hold on to those tokens forever).
func New(name string, delayBetween time.Duration, maxSimultaneous int, releaseTimeout time.Duration) *Protector {
	return &Protector{
		Name:           name,
		maxTokens:      maxSimultaneous,
		delayBetween:   delayBetween,
		releaseTimeout: releaseTimeout,
		requests:       make(map[Receipt]*request),
	}
}

// AvailabilityCallback is used as a callback to know how many tokens are
// currently available for use.
type AvailabilityCallback func() (numTokens int)

// SetAvailabilityCallback sets a callback that will be called whenever the
// Protector checks to see if Request()s can be fulfilled.
//
// The callback should do some kind of check on the resource to see how busy it
// is, and return a number between 0 (block any additional usage of the
// resource) and the maxSimultaneous value provided to New() (higher values will
// be of no benefit).
//
// The callback will only be called at most every delayBetween value supplied to
// New(), so if checking the resource is an expensive operation, be sure to set
// that value appropriately high. (Or do appropriate caching of the busyness on
// your end.)
//
// NB: You'd only set this callback if you have unprotected access to your
// resource that you need the Protector to take in to account.
func (p *Protector) SetAvailabilityCallback(callback AvailabilityCallback) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.availabilityCb = callback
}

// Request lets you request that a desired number of tokens be granted to you
// for use.
//
// You immediately get back a Receipt, which you should supply to
// WaitUntilGranted(), then to Touch() periodically until you're no longer using
// the resource, then finally to Release().
//
// An optional autoRelease value can be supplied, which effectively means
// Release() will be called for you after that amount of time has passed. (You'd
// still need to Touch() periodically if this value was less than the
// releaseTimeout value given to New().)
func (p *Protector) Request(numTokens int, autoRelease ...time.Duration) (Receipt, error) {
	if numTokens > p.maxTokens {
		return Receipt(""), Error{p.Name, "Request", Receipt(""), ErrOverMaximumTokens}
	}

	// create a request object
	r := &request{
		id:        Receipt(uuid.NewV4().String()),
		grantedCh: make(chan bool, 1),
		releaseCh: make(chan bool, 1),
		touchCh:   make(chan bool, 1),
		numTokens: numTokens,
	}
	if len(autoRelease) == 1 {
		r.autoRelease = autoRelease[0]
	} else {
		// default to a year
		r.autoRelease = 8760 * time.Hour
	}

	// queue the request and return its id as a receipt for future use by the
	// user
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = append(p.pending, r)
	p.requests[r.id] = r
	if p.lastProcess.IsZero() && len(p.pending) == 1 {
		go p.process()
	} else {
		go p.reprocess()
	}
	return r.id, nil
}

// WaitUntilGranted will block until the request corresponding to the given
// Receipt has been granted its tokens, whereupon you can start using the
// protected resource.
//
// If you call this after more time than the releaseTimeout supplied to New()
// has passed since you made the Request(), the request will already have been
// released, and this function will return false: do not use the resources in
// that case! (You will also get false if you supply an invalid Receipt.)
func (p *Protector) WaitUntilGranted(receipt Receipt) bool {
	p.mu.RLock()
	r, found := p.requests[receipt]
	p.mu.RUnlock()
	if found {
		return r.waitUntilGranted()
	}
	return false
}

// Touch for a request (identified by the given receipt) prevents it timing out
// and releasing the granted tokens. You should call this periodically after
// WaitUntilGranted() for the same receipt.
func (p *Protector) Touch(receipt Receipt) {
	p.mu.RLock()
	r, found := p.requests[receipt]
	p.mu.RUnlock()
	if found {
		r.touch()
	}
}

// Release will release the tokens of a granted Request(), for use by any other
// requests. You should always call this when you're done using a resource
// (unless your request had an autoRelease specified).
func (p *Protector) Release(receipt Receipt) {
	p.mu.RLock()
	r, found := p.requests[receipt]
	p.mu.RUnlock()
	if found {
		r.release()
	}
}

// process takes the oldest queued Request(), and if it can be fulfilled, tells
// the request that it has been granted the tokens it wanted. Calls reprocess()
// which in turn calls process() again in the future, as necessary.
func (p *Protector) process() {
	p.mu.Lock()
	defer p.mu.Unlock()
	pendingLen := len(p.pending)
	if p.usedTokens == p.maxTokens || pendingLen == 0 {
		return
	}
	availableTokens, checked := p.availableTokens()
	r := p.pending[0]
	if checked && availableTokens < r.numTokens {
		// more resources could turn up later, outside of our control and
		// knowledge, so call this again after the standard delay
		p.lastProcess = time.Now() // act as if we processed successfully so that reprocess() will wait
		go p.reprocess()
		return
	}
	if r.numTokens > 1 && p.maxTokens-p.usedTokens < r.numTokens {
		// we're tracking that we've used these tokens, and when we release them
		// we'll call process() again at that time
		return
	}

	// claim these resource "tokens" and let the requester know we've granted
	// the request
	p.pending = p.pending[1:]
	p.usedTokens += r.numTokens
	p.lastProcess = time.Now()
	r.grantedCh <- true

	// manage the deliberate or automatic release of these resource "tokens" in
	// a goroutine. (not sure if having 1 goroutine per active request will be
	// an issue...)
	go func() {
		auto := time.After(r.autoRelease)
		for {
			limit := time.After(p.releaseTimeout)
			select {
			case <-r.releaseCh:
				// released on request
			case <-limit:
				// released after releaseTimeout
				r.finished()
			case <-auto:
				// release after the requested auto release time
				r.finished()
			case <-r.touchCh:
				// Touch() was called, loop to reset the timeout
				continue
			}

			// return the used tokens to the pool for future use
			p.mu.Lock()
			p.usedTokens -= r.numTokens
			delete(p.requests, r.id)
			if len(p.pending) > 0 {
				// now that we've released tokens, call process() again, making
				// sure we obey delayBetween
				p.mu.Unlock()
				p.reprocess()
			} else {
				p.mu.Unlock()
			}
			break
		}
	}()

	if pendingLen > 1 {
		// arrange for the next request to be taken care of after the desired
		// delay
		go p.reprocess()
	}
}

// reprocess calls process() after at least the desired delay, throwing away
// additional requests during that time.
func (p *Protector) reprocess() {
	p.mu.Lock()
	if p.reprocessing {
		p.mu.Unlock()
		return
	}
	p.reprocessing = true
	since := time.Since(p.lastProcess)

	if since < p.delayBetween {
		remaining := p.delayBetween - since
		p.mu.Unlock()
		<-time.After(remaining)
		p.mu.Lock()
	}

	p.reprocessing = false
	p.mu.Unlock()
	p.process()
}

// availableTokens runs any set callback to find the available tokens we can
// grant right now. Also returns a bool indicating if the callback was even
// set. Never returns a value higher than maxTokens.
func (p *Protector) availableTokens() (int, bool) {
	if p.availabilityCb != nil {
		availableTokens := p.availabilityCb()
		if availableTokens > p.maxTokens {
			availableTokens = p.maxTokens
		}
		return availableTokens, true
	}
	return 0, false
}
