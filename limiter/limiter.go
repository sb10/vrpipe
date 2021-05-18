// Copyright © 2019, 2021 Genome Research Limited
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

package limiter

// This file contains the implementation of the main struct in the limiter
// package, the Limiter.

import (
	"context"
	"time"

	sync "github.com/sasha-s/go-deadlock"
)

// SetLimitCallback is provided to New(). Your function should take the name of
// a group and return the current limit for that group. If the group doesn't
// exist or has no limit, return -1. The idea is that you retrieve the limit for
// a group from some on-disk database, so you don't have to have all group
// limits in memory. (Limiter itself will clear out unused groups from its own
// memory.)
type SetLimitCallback func(context.Context, string) int

// Limiter struct is used to limit usage of groups.
type Limiter struct {
	cb     SetLimitCallback
	groups map[string]*group
	mu     sync.Mutex
}

// New creates a new Limiter.
func New(cb SetLimitCallback) *Limiter {
	return &Limiter{
		cb:     cb,
		groups: make(map[string]*group),
	}
}

// SetLimit creates or updates a group with the given limit.
func (l *Limiter) SetLimit(name string, limit uint) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if g, set := l.groups[name]; set {
		g.setLimit(limit)
	} else {
		l.groups[name] = newGroup(name, limit)
	}
}

// GetLimit tells you the limit currently set for the given group. If the group
// doesn't exist, returns -1.
func (l *Limiter) GetLimit(ctx context.Context, name string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	group := l.vivifyGroup(ctx, name)
	if group == nil {
		return -1
	}
	return int(group.limit)
}

// GetLimits tells you the current limit of all currently set groups.
func (l *Limiter) GetLimits() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()

	limits := make(map[string]int, len(l.groups))

	for name, group := range l.groups {
		limits[name] = int(group.limit)
	}

	return limits
}

// RemoveLimit removes the given group from memory. If your callback also begins
// returning -1 for this group, the group effectively becomes unlimited.
func (l *Limiter) RemoveLimit(name string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.groups, name)
}

// Increment sees if it would be possible to increment the count of every
// supplied group, without making any of them go over their limit.
//
// If this is the first time we're seeing a group name, or a Decrement() call
// has made us forget about that group, the callback provided to New() will be
// called with the name, and the returned value will be used to create a new
// group with that limit and initial count of 0 (which will become 1 if this
// returns true). Groups with a limit of 0 will not be able to be Increment()ed.
//
// If possible, the group counts are actually incremented and this returns
// true. If not possible, no group counts are altered and this returns false.
//
// If an optional wait duration is supplied, will wait for up to the given wait
// period for an increment of every group to be possible.
func (l *Limiter) Increment(ctx context.Context, groups []string, wait ...time.Duration) bool {
	l.mu.Lock()
	if l.checkGroups(ctx, groups) {
		l.incrementGroups(ctx, groups)
		l.mu.Unlock()
		return true
	}

	if len(wait) != 1 {
		l.mu.Unlock()
		return false
	}

	ch := make(chan bool, len(groups))
	l.registerGroupNotifications(ctx, groups, ch)
	l.mu.Unlock()

	limit := time.After(wait[0])
	for {
		select {
		case <-ch:
			l.mu.Lock()
			if l.checkGroups(ctx, groups) {
				l.incrementGroups(ctx, groups)
				l.mu.Unlock()
				return true
			}
			ch = make(chan bool, len(groups))
			l.registerGroupNotifications(ctx, groups, ch)
			l.mu.Unlock()
			continue
		case <-limit:
			return false
		}
	}
}

// checkGroups checks all the groups to see if they can be incremented. You must
// hold the mu.lock before calling this, and until after calling
// incrementGroups() if this returns true.
func (l *Limiter) checkGroups(ctx context.Context, groups []string) bool {
	for _, name := range groups {
		group := l.vivifyGroup(ctx, name)
		if group != nil {
			if !group.canIncrement() {
				return false
			}
		}
	}
	return true
}

// incrementGroups increments all the groups without checking them. You must
// hold the mu.lock before calling this (and check first).
func (l *Limiter) incrementGroups(ctx context.Context, groups []string) {
	for _, name := range groups {
		group := l.vivifyGroup(ctx, name)
		if group != nil {
			group.increment()
		}
	}
}

// vivifyGroup either returns a stored group or creates a new one based on the
// results of calling the SetLimitCallback. You must have the mu.Lock() before
// calling this. Can return nil if the callback doesn't know about this group
// and returns a -1 limit.
func (l *Limiter) vivifyGroup(ctx context.Context, name string) *group {
	group, exists := l.groups[name]
	if !exists {
		if limit := l.cb(ctx, name); limit >= 0 {
			group = newGroup(name, uint(limit))
			l.groups[name] = group
		}
	}
	return group
}

// registerGroupNotifications passes the channel to each group to be notified of
// decrement() calls on them.
func (l *Limiter) registerGroupNotifications(ctx context.Context, groups []string, ch chan bool) {
	for _, name := range groups {
		group := l.vivifyGroup(ctx, name)
		if group != nil {
			group.notifyDecrement(ch)
		}
	}
}

// Decrement decrements the count of every supplied group.
//
// To save memory, if a group reaches a count of 0, it is forgotten.
//
// If a group isn't known about (because it was never previously Increment()ed,
// or was previously Decrement()ed to 0 and forgotten about), it is silently
// ignored.
func (l *Limiter) Decrement(groups []string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, name := range groups {
		if group, exists := l.groups[name]; exists {
			if group.decrement() {
				delete(l.groups, group.name)
			}
		}
	}
}

// GetLowestLimit tells you the lowest limit currently set amongst the given
// groups. If none have a limit set, returns -1.
func (l *Limiter) GetLowestLimit(ctx context.Context, groups []string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	lowest := -1
	for _, name := range groups {
		group := l.vivifyGroup(ctx, name)
		if group != nil && (lowest == -1 || int(group.limit) < lowest) {
			lowest = int(group.limit)
		}
	}
	return lowest
}

// GetRemainingCapacity tells you how many times you could Increment() the given
// groups. If none have a limit set, returns -1.
func (l *Limiter) GetRemainingCapacity(ctx context.Context, groups []string) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	lowest := -1
	for _, name := range groups {
		group := l.vivifyGroup(ctx, name)
		if group != nil {
			capacity := group.capacity()
			if lowest == -1 || capacity < lowest {
				lowest = capacity
			}
		}
	}
	return lowest
}
