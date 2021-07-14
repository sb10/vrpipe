// Copyright © 2016-2019, 2021 Genome Research Limited
// Author: Sendu Bala <sb10@sanger.ac.uk>.
// This file was based on: Diego Bernardes de Sousa Pinto's
// https://github.com/diegobernardes/ttlcache
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

/*
Package queue provides an in-memory queue structure suitable for the safe and
low latency implementation of a real job queue.

It's like beanstalkd, but faster, with the ability to query the queue for
desired items, reject duplicates, and wait on dependencies.

Like beanstalkd, when you add items to the queue, they move between different
sub-queues:

Items start in the delay queue. After the item's delay time, they automatically
move to the ready queue. From there you can Reserve() an item to get the highest
priority (or for those with equal priority, the oldest - fifo) one which
switches it from the ready queue to the run queue. Items can also have
dependencies, in which case they start in the dependency queue and only move to
the ready queue (bypassing the delay queue) once all its dependencies have been
Remove()d from the queue. Items can also belong to a reservation group, in which
case you can Reserve() an item in a desired group.

In the run queue the item starts a time-to-release (ttr) countdown; when that
runs out the item is placed back on the ready queue. This is to handle a
process Reserving an item but then crashing before it deals with the item;
with it back on the ready queue, some other process can pick it up.

To stop it going back to the ready queue you either Remove() the item (you dealt
with the item successfully), Touch() it to give yourself more time to handle the
item, or you Bury() the item (the item can't be dealt with until the user takes
some action). When you know you have a transient problem preventing you from
handling the item right now, you can manually Release() the item back to the
delay queue.

    import "github.com/VertebrateResequencing/wr/queue"
    q = queue.New("myQueue")
    q.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
        for _, item := range allitemdata {
            // cast item to the original type, then arrange to do something now
            // you know that the item is ready to be processed
        }
    })

    // add an item to the queue
    ttr := 30 * time.Second
    item, err := q.Add("uuid1", "", "item data1", 0, 0 * time.Second, ttr)
    item, err := q.Add("uuid2", "group", "item data2", 0, 0 * time.Second, ttr)

    // get it back out
    item, err = queue.Get("uuid1")

    // reserve the next item with no group
    item, err = queue.Reserve("", 0)

    // or reserve the next item in a particular group
	item, err = queue.Reserve("group", 0)

	// or reserve even if there are no items in the queue right now, waiting
	// until something gets added or otherwise becomes ready
	item, err = queue.Reserve("group", 1 * time.Second)

    // queue.Touch() every < ttr seconds if you might take longer than ttr to
    // process the item

    // say you successfully handled the item
    item.Remove()
*/
package queue

import (
	"context"
	"errors"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"
)

// SubQueue is how we name the sub-queues of a Queue.
type SubQueue string

// SubQueue* constants represent all the possible sub-queues. For use in
// changedCallback(), there are also the fake sub-queues representing items
// new to the queue and items removed from the queue.
const (
	SubQueueNew       SubQueue = "new"
	SubQueueDelay     SubQueue = "delay"
	SubQueueReady     SubQueue = "ready"
	SubQueueRun       SubQueue = "run"
	SubQueueBury      SubQueue = "bury"
	SubQueueDependent SubQueue = "dependent"
	SubQueueRemoved   SubQueue = "removed"
)

// recallBreak is how long we wait before recalling readyAdded.
const recallBreak = 500 * time.Millisecond

// queue has some typical errors
var (
	ErrQueueClosed   = errors.New("queue closed")
	ErrNothingReady  = errors.New("ready queue is empty")
	ErrAlreadyExists = errors.New("already exists")
	ErrNotFound      = errors.New("not found")
	ErrNotReady      = errors.New("not ready")
	ErrNotRunning    = errors.New("not running")
	ErrNotBuried     = errors.New("not buried")
)

// Error records an error and the operation, item and queue that caused it.
type Error struct {
	Queue string // the queue's Name
	Op    string // name of the method
	Item  string // the item's key
	Err   error  // one of our Err vars
}

func (e Error) Error() string {
	return "queue(" + e.Queue + ") " + e.Op + "(" + e.Item + "): " + e.Err.Error()
}

// ReadyAddedCallback is used as a callback to know when new items have been
// added to the ready sub-queue, getting /all/ items in the ready sub-queue.
type ReadyAddedCallback func(queuename string, allitemdata []interface{})

// ChangedCallback is used as a callback to know when items change sub-queues,
// telling you what item.Data() moved from which sub-queue to which other sub-
// queue. For new items in the queue, `from` will be SubQueueNew, and for items
// leaving the queue, `to` will be SubQueueRemoved.
type ChangedCallback func(from, to SubQueue, data []interface{})

// TTRCallback is used as a callback to decide which sub-queue an item should
// move to when a an item in the run sub-queue hits its TTR, based on that
// item's data. Valid return values are SubQueueDelay, SubQueueReady and
// SubQueueBury. SubQueueRun can be used to avoid changing subqueue. Other
// values will be treated as SubQueueReady).
type TTRCallback func(data interface{}) SubQueue

// defaultTTRCallback is used if the the user never calls SetTTRCallback() and
// always moves the items to the ready sub-queue.
var defaultTTRCallback = func(data interface{}) SubQueue {
	return SubQueueReady
}

// Queue is a synchronized map of items that can shift to different sub-queues,
// automatically depending on their delay or ttr expiring, or manually by
// calling certain methods.
type Queue struct {
	delayTime              time.Time
	ttrTime                time.Time
	Name                   string
	items                  map[string]*Item
	dependants             map[string]map[string]*Item
	delayQueue             *subQueue
	readyQueue             *subQueue
	runQueue               *subQueue
	buryQueue              *buryQueue
	depQueue               *depQueue
	delayNotification      chan bool
	startedDelayProcessing chan bool
	delayClose             chan bool
	ttrNotification        chan bool
	startedTTRProcessing   chan bool
	ttrClose               chan bool
	readyAddedCb           ReadyAddedCallback
	changedCb              ChangedCallback
	ttrCb                  TTRCallback
	mutex                  sync.RWMutex
	readyAddedCbMutex      sync.Mutex
	closed                 bool
	readyAddedCbRunning    bool
	readyAddedCbRecall     bool
}

// Stats holds information about the Queue's state.
type Stats struct {
	Items     int
	Delayed   int
	Ready     int
	Running   int
	Buried    int
	Dependant int
}

// ItemDef makes it possible to supply a slice of Add() args to AddMany().
type ItemDef struct {
	Key          string
	ReserveGroup string
	Data         interface{}
	Priority     uint8 // highest priority is 255
	Delay        time.Duration
	TTR          time.Duration
	StartQueue   SubQueue // blank, or one of SubQueueRun or SubQueueBury
	Dependencies []string
}

// New is a helper to create instance of the Queue struct.
func New(ctx context.Context, name string) *Queue {
	queue := &Queue{
		Name:                   name,
		items:                  make(map[string]*Item),
		dependants:             make(map[string]map[string]*Item),
		delayQueue:             newSubQueue(0),
		readyQueue:             newSubQueue(1),
		runQueue:               newSubQueue(2),
		buryQueue:              newBuryQueue(),
		depQueue:               newDependencyQueue(),
		ttrNotification:        make(chan bool, 1),
		startedTTRProcessing:   make(chan bool),
		ttrClose:               make(chan bool, 1),
		ttrTime:                time.Now(),
		delayNotification:      make(chan bool, 1),
		startedDelayProcessing: make(chan bool),
		delayClose:             make(chan bool, 1),
		delayTime:              time.Now(),
		ttrCb:                  defaultTTRCallback,
	}
	go queue.startDelayProcessing(ctx)
	<-queue.startedDelayProcessing
	go queue.startTTRProcessing(ctx)
	<-queue.startedTTRProcessing
	return queue
}

// SetReadyAddedCallback sets a callback that will be called when new items have
// been added to the ready sub-queue. The callback will receive the name of the
// queue, and a slice of the Data properties of every item currently in the
// ready sub-queue. The callback will be initiated in a go routine.
//
// Note that we will wait for the callback to finish running before calling it
// again. If new items enter the ready sub-queue while your callback is still
// running, you will only know about them when your callback is called again,
// immediately after the previous call completes.
func (queue *Queue) SetReadyAddedCallback(callback ReadyAddedCallback) {
	queue.readyAddedCb = callback
}

// TriggerReadyAddedCallback allows you to manually trigger your
// readyAddedCallback at times when no new items have been added to the ready
// queue. It will receive the current set of ready item data.
func (queue *Queue) TriggerReadyAddedCallback(ctx context.Context) {
	queue.readyAdded(ctx, "triggered")
}

// readyAdded checks if a readyAddedCallback has been set, and if so calls it
// in a go routine. It never runs the callback concurrently though: if it is
// still running from a previous call, we only schedule that the callback be
// called (once) after the current call completes.
func (queue *Queue) readyAdded(ctx context.Context, source string) {
	if queue.readyAddedCb != nil {
		queue.readyAddedCbMutex.Lock()
		if queue.readyAddedCbRunning {
			queue.readyAddedCbRecall = true
			queue.readyAddedCbMutex.Unlock()
			return
		}
		queue.readyAddedCbRunning = true
		queue.readyAddedCbMutex.Unlock()

		go func() {
			queue.mutex.RLock()
			var data []interface{}
			for _, il := range queue.readyQueue.groupedItems {
				for _, item := range il {
					data = append(data, item.Data())
				}
			}
			queue.mutex.RUnlock()
			clog.Debug(ctx, "ready items available, triggering callback", "source", source, "items", len(data))
			queue.readyAddedCb(queue.Name, data)
			clog.Debug(ctx, "finished triggering callback for ready items", "source", source, "items", len(data))

			queue.readyAddedCbMutex.Lock()
			recall := false
			if queue.readyAddedCbRecall {
				recall = true
			} else {
				queue.readyAddedCbRunning = false
			}
			queue.readyAddedCbMutex.Unlock()

			if recall {
				// wait before the recall to stop us being constantly
				// locked getting ready items
				<-time.After(recallBreak)
				queue.readyAddedCbMutex.Lock()
				queue.readyAddedCbRunning = false
				queue.readyAddedCbRecall = false
				defer queue.readyAdded(ctx, "recall")
				queue.readyAddedCbMutex.Unlock()
			}
		}()
	}
}

// SetChangedCallback sets a callback that will be called when items move from
// one sub-queue to another. The callback receives the name of the moved-from
// sub-queue ('new' in the case of entering the queue for the first time), the
// name of the moved-to sub-queue ('removed' in the case of the item being
// removed from the queue), and a slice of item.Data() of everything that moved
// in this way. The callback will be initiated in a go routine.
func (queue *Queue) SetChangedCallback(callback ChangedCallback) {
	queue.changedCb = callback
}

// changed checks if a changedCallback has been set, and if so calls it in a go
// routine.
func (queue *Queue) changed(from, to SubQueue, items []*Item) {
	if queue.changedCb != nil {
		var data []interface{}
		for _, item := range items {
			data = append(data, item.Data())
		}
		go queue.changedCb(from, to, data)
	}
}

// SetTTRCallback sets a callback that will be called when an item in the run
// sub-queue hits its TTR. The callback receives an item's data and should
// return the sub-queue the item should be moved to. If you don't set this, the
// default will be to move all items to the ready sub-queue.
func (queue *Queue) SetTTRCallback(callback TTRCallback) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()
	queue.ttrCb = callback
}

// Destroy shuts down a queue, destroying any contents. You can't do anything
// useful with it after that.
func (queue *Queue) Destroy() error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return Error{queue.Name, "Destroy", "", ErrQueueClosed}
	}

	queue.ttrClose <- true
	queue.delayClose <- true
	queue.items = nil
	queue.delayQueue.empty()
	queue.readyQueue.empty()
	queue.runQueue.empty()
	queue.buryQueue.empty()
	queue.depQueue.empty()
	queue.closed = true
	return nil
}

// Stats returns information about the number of items in the queue and each
// sub-queue.
func (queue *Queue) Stats() *Stats {
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()

	return &Stats{
		Items:     len(queue.items),
		Delayed:   queue.delayQueue.len(),
		Ready:     queue.readyQueue.len(),
		Running:   queue.runQueue.len(),
		Buried:    queue.buryQueue.len(),
		Dependant: queue.depQueue.len(),
	}
}

// Add is a thread-safe way to add new items to the queue.
//
// After delay they will switch to the ready sub-queue from where they can be
// Reserve()d. Once reserved, they have ttr to Remove() the item, otherwise it
// gets released back to the ready sub-queue.
//
// The priority determines which item will be next to be Reserve()d, with
// priority 255 (the max) items coming before lower priority ones (with 0 being
// the lowest). Items with the same priority number are Reserve()d on a fifo
// basis.
//
// reserveGroup can be left as an empty string, but specifying it then lets you
// provide the same to Reserve() to get the next item with the given
// reserveGroup.
//
// startQueue should normally be supplied as an empty string, meaning the item
// will start in the delay or ready sub-queue as described above. For the
// purpose of recovering a queue following a crash, however, you can supply
// either SubQueueRun or SubQueueBury to start the item in one of those
// sub-queues. If the item has unmet dependencies, startQueue is ignored.
//
// The final argument to Add() is an optional slice of item ids on which this
// item depends: this item will first enter the dependency sub-queue and only
// transfer to the ready sub-queue when items with these ids get Remove()d from
// the queue.
//
// Add() returns an item, which may have already existed (in which case, nothing
// was actually added or changed).
func (queue *Queue) Add(ctx context.Context, key string, reserveGroup string, data interface{}, priority uint8, delay time.Duration, ttr time.Duration, startQueue SubQueue, deps ...[]string) (*Item, error) {
	queue.mutex.Lock()
	item, err := queue.newItemForAdd(key, reserveGroup, data, priority, 0, delay, ttr)
	if err != nil {
		queue.mutex.Unlock()
		return item, err
	}
	queue.handleItemForAdd(ctx, item, startQueue, delay, deps...)
	return item, nil
}

// newItemForAdd prepares a new item for Add() and AddWithSize() methods. You
// must hold the mutex lock before calling this.
func (queue *Queue) newItemForAdd(key string, reserveGroup string, data interface{}, priority uint8, size uint8, delay time.Duration, ttr time.Duration) (*Item, error) {
	if queue.closed {
		return nil, Error{queue.Name, "Add", key, ErrQueueClosed}
	}

	item, existed := queue.items[key]
	if existed {
		return item, Error{queue.Name, "Add", key, ErrAlreadyExists}
	}

	item = newItem(key, reserveGroup, data, priority, delay, ttr)
	item.size = size
	queue.items[key] = item
	return item, nil
}

// handleItemForAdd checks dependencies and then pushes the item to the desired
// subqueue. You must hold the mutex lock before calling this. It will unlock.
func (queue *Queue) handleItemForAdd(ctx context.Context, item *Item, startQueue SubQueue, delay time.Duration, deps ...[]string) {
	// check dependencies
	if len(deps) == 1 && len(deps[0]) > 0 {
		queue.setItemDependencies(item, deps[0])
		queue.mutex.Unlock()
		queue.changed(SubQueueNew, SubQueueDependent, []*Item{item})
		return
	}

	switch startQueue {
	case SubQueueRun:
		item.switchDelayReady()
		item.touch()
		queue.runQueue.push(item)
		item.switchReadyRun()
		queue.mutex.Unlock()
		queue.ttrNotificationTrigger(item)
		queue.changed(SubQueueNew, SubQueueRun, []*Item{item})
	case SubQueueBury:
		item.switchDelayReady()
		queue.buryQueue.push(item)
		item.switchRunBury()
		queue.mutex.Unlock()
		queue.changed(SubQueueNew, SubQueueBury, []*Item{item})
	default:
		if delay.Nanoseconds() == 0 {
			// put it directly on the ready queue
			item.switchDelayReady()
			queue.readyQueue.push(item)
			queue.mutex.Unlock()
			queue.changed(SubQueueNew, SubQueueReady, []*Item{item})
			queue.readyAdded(ctx, "new")
		} else {
			queue.delayQueue.push(item)
			queue.mutex.Unlock()
			queue.changed(SubQueueNew, SubQueueDelay, []*Item{item})
			queue.delayNotificationTrigger(item)
		}
	}
}

// AddWithSize is like Add(), but the item also gets a "size" property.
// Size alters the way priority is handled. For items with the same priority,
// the next to be Reserve()d will be the item with the highest size. If they
// also have the same size, then they will be Reserve()d in fifo order.
func (queue *Queue) AddWithSize(ctx context.Context, key string, reserveGroup string, data interface{}, priority uint8, size uint8, delay time.Duration, ttr time.Duration, startQueue SubQueue, deps ...[]string) (*Item, error) {
	queue.mutex.Lock()
	item, err := queue.newItemForAdd(key, reserveGroup, data, priority, size, delay, ttr)
	if err != nil {
		queue.mutex.Unlock()
		return item, err
	}
	queue.handleItemForAdd(ctx, item, startQueue, delay, deps...)
	return item, nil
}

// setItemDependencies sets the given item keys as the dependencies of the given
// item, and places the item in the dependency queue. Note that you can be
// dependent on items that do not exist in the queue; the item will remain in
// dependent queue until you add items with the given deps keys and then
// Remove() them.
func (queue *Queue) setItemDependencies(item *Item, deps []string) {
	item.setDependencies(deps)
	queue.setQueueDeps(item)
	item.switchDelayDependent()
	queue.depQueue.push(item)
}

// setQueueDeps updates the queue's lookup of parent items to their dependent
// children when you give it a child item (that has had some dependencies set on
// it).
func (queue *Queue) setQueueDeps(item *Item) {
	for _, dep := range item.Dependencies() {
		if _, exists := queue.dependants[dep]; !exists {
			queue.dependants[dep] = make(map[string]*Item)
		}
		queue.dependants[dep][item.Key] = item
	}
}

// itemHasDeps returns true if the item has unresolved dependencies according
// to the queue's lookup of parent items to their dependent children.
func (queue *Queue) itemHasDeps(item *Item) bool {
	for _, dep := range item.Dependencies() {
		if _, exists := queue.items[dep]; exists {
			return true
		}
	}
	return false
}

// AddMany is like Add(), except that you supply a slice of *ItemDef, and it
// returns the number that were actually added and the number of items that were
// not added because they were duplicates of items already in the queue. If an
// error occurs, nothing will have been added.
func (queue *Queue) AddMany(ctx context.Context, items []*ItemDef) (added, dups int, err error) {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return 0, 0, Error{queue.Name, "AddMany", "", ErrQueueClosed}
	}

	deferredDelayTrigger := false
	deferredTTRTrigger := false
	var addedReadyItems []*Item
	var addedDelayItems []*Item
	var addedDepItems []*Item
	var addedRunItems []*Item
	var addedBuryItems []*Item
	for _, def := range items {
		_, existed := queue.items[def.Key]
		if existed {
			dups++
			continue
		}

		item := newItem(def.Key, def.ReserveGroup, def.Data, def.Priority, def.Delay, def.TTR)
		queue.items[def.Key] = item

		if len(def.Dependencies) > 0 {
			queue.setItemDependencies(item, def.Dependencies)
			addedDepItems = append(addedDepItems, item)
		} else {
			switch def.StartQueue {
			case SubQueueRun:
				item.switchDelayReady()
				item.touch()
				queue.runQueue.push(item)
				item.switchReadyRun()
				addedRunItems = append(addedRunItems, item)
				if !deferredTTRTrigger && queue.ttrTime.After(time.Now().Add(item.ttr)) {
					defer queue.ttrNotificationTrigger(item)
					deferredTTRTrigger = true
				}
			case SubQueueBury:
				item.switchDelayReady()
				queue.buryQueue.push(item)
				item.switchReadyRun()
				item.switchRunBury()
				addedBuryItems = append(addedBuryItems, item)
			default:
				if def.Delay.Nanoseconds() == 0 {
					// put it directly on the ready queue
					item.switchDelayReady()
					queue.readyQueue.push(item)
					addedReadyItems = append(addedReadyItems, item)
				} else {
					queue.delayQueue.push(item)
					addedDelayItems = append(addedDelayItems, item)
					if !deferredDelayTrigger && queue.delayTime.After(time.Now().Add(item.delay)) {
						defer queue.delayNotificationTrigger(item)
						deferredDelayTrigger = true
					}
				}
			}
		}

		added++
	}

	queue.mutex.Unlock()
	if len(addedReadyItems) > 0 {
		queue.changed(SubQueueNew, SubQueueReady, addedReadyItems)
		queue.readyAdded(ctx, "new")
	}
	if len(addedDelayItems) > 0 {
		queue.changed(SubQueueNew, SubQueueDelay, addedDelayItems)
	}
	if len(addedDepItems) > 0 {
		queue.changed(SubQueueNew, SubQueueDependent, addedDepItems)
	}
	if len(addedRunItems) > 0 {
		queue.changed(SubQueueNew, SubQueueRun, addedRunItems)
	}
	if len(addedBuryItems) > 0 {
		queue.changed(SubQueueNew, SubQueueBury, addedBuryItems)
	}

	return added, dups, err
}

// Get is a thread-safe way to get an item by the key you used to Add() it.
func (queue *Queue) Get(key string) (*Item, error) {
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()

	if queue.closed {
		return nil, Error{queue.Name, "Get", key, ErrQueueClosed}
	}

	item, exists := queue.items[key]
	if !exists {
		return nil, Error{queue.Name, "Get", key, ErrNotFound}
	}
	return item, nil
}

// GetRunningData gets all the item.Data() of items currently in the run sub-
// queue.
func (queue *Queue) GetRunningData() []interface{} {
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()
	data := make([]interface{}, 0, len(queue.runQueue.items))
	for _, item := range queue.runQueue.items {
		data = append(data, item.Data())
	}
	return data
}

// AllItems returns the items in the queue. NB: You should NOT do anything
// to these items - use for read-only purposes.
func (queue *Queue) AllItems() []*Item {
	queue.mutex.RLock()
	defer queue.mutex.RUnlock()
	items := make([]*Item, 0, len(queue.items))
	for _, item := range queue.items {
		items = append(items, item)
	}
	return items
}

// Update is a thread-safe way to change the data, ReserveGroup, priority, delay,
// ttr or dependencies of an item. You must supply all of these as per Add() -
// just supply the old values of those you are not changing (except for
// dependencies, which remain optional). The old values can be found by getting
// the item with Get() (giving you item.Key, item.ReserveGroup, item.Data() and
// item.UnresolvedDependencies()), and then calling item.Stats() to get
// stats.Priority, stats.Delay and stats.TTR.
func (queue *Queue) Update(ctx context.Context, key string, reserveGroup string, data interface{}, priority uint8, delay time.Duration, ttr time.Duration, deps ...[]string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Update", key, ErrQueueClosed}
	}

	item, exists := queue.items[key]
	if !exists {
		queue.mutex.Unlock()
		return Error{queue.Name, "Update", key, ErrNotFound}
	}

	var changedFrom SubQueue
	var addedReady bool
	item.SetData(data)
	if len(deps) == 1 {
		// check if dependencies actually changed
		oldDeps := make(map[string]bool)
		for _, dep := range item.UnresolvedDependencies() {
			oldDeps[dep] = true
		}
		newDeps := 0
		for _, dep := range deps[0] {
			if !oldDeps[dep] {
				newDeps++
			}
			delete(oldDeps, dep)
		}
		var toRemove []string
		for dep := range oldDeps {
			toRemove = append(toRemove, dep)
		}

		if len(toRemove) > 0 || newDeps > 0 {
			// remove any invalid dependencies from our lookup
			for _, dep := range toRemove {
				if _, exists := queue.items[dep]; exists {
					delete(queue.dependants[dep], key)
					if len(queue.dependants[dep]) == 0 {
						delete(queue.dependants, dep)
					}
				}
			}

			// set the new dependencies and update our lookup
			item.setDependencies(deps[0])
			queue.setQueueDeps(item)

			// if we now have unresolved dependencies and we're not in dependent
			// state, switch to dependent queue
			item.mutex.RLock()
			iState := item.state
			item.mutex.RUnlock()
			if len(deps[0]) > 0 && iState != ItemStateDependent {
				pushToDep := true
				switch iState {
				case ItemStateDelay:
					queue.delayQueue.remove(item)
					item.switchDelayDependent()
					changedFrom = SubQueueDelay
				case ItemStateReady:
					queue.readyQueue.remove(item)
					item.switchReadyDependent()
					changedFrom = SubQueueReady
				case ItemStateRun:
					queue.runQueue.remove(item)
					item.switchRunDependent()
					changedFrom = SubQueueRun
				case ItemStateBury:
					// leave buried things buried; Kick() will put it on the
					// dependent queue if they are still unresolved by then
					pushToDep = false
				}
				if pushToDep {
					queue.depQueue.push(item)
				}
			} else if len(deps[0]) == 0 {
				// switch to ready queue
				queue.depQueue.remove(item)
				item.switchDependentReady()
				queue.readyQueue.push(item)
				addedReady = true
			}
		}
	}

	item.mutex.Lock()
	switch {
	case item.delay != delay:
		item.delay = delay
		if item.state == ItemStateDelay {
			item.mutex.Unlock()
			item.restart()
			queue.delayQueue.update(item)
		} else {
			item.mutex.Unlock()
		}
	case item.priority != priority || item.ReserveGroup != reserveGroup || addedReady:
		item.priority = priority
		oldGroup := item.ReserveGroup
		item.ReserveGroup = reserveGroup
		if item.state == ItemStateReady {
			item.mutex.Unlock()
			queue.readyQueue.update(item, oldGroup)
		} else {
			item.mutex.Unlock()
		}
	case item.ttr != ttr:
		item.ttr = ttr
		if item.state == ItemStateRun {
			item.mutex.Unlock()
			item.touch()
			queue.runQueue.update(item)
		} else {
			item.mutex.Unlock()
		}
	default:
		item.mutex.Unlock()
	}

	if addedReady {
		queue.mutex.Unlock()
		queue.readyAdded(ctx, "updated")
		queue.changed(SubQueueDependent, SubQueueReady, []*Item{item})
	} else {
		queue.mutex.Unlock()
	}

	if changedFrom != "" {
		queue.changed(changedFrom, SubQueueDependent, []*Item{item})
	}

	return nil
}

// ChangeKey is a thread-safe way to change the key an item can be found with
// using Get() (and also ensures any dependencies involving the old key will
// continue to work). If an item already exists in the queue with the new key,
// this will fail.
func (queue *Queue) ChangeKey(old, new string) error {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return Error{queue.Name, "ChangeKey", old, ErrQueueClosed}
	}

	if _, exists := queue.items[new]; exists {
		return Error{queue.Name, "ChangeKey", new, ErrAlreadyExists}
	}

	item, exists := queue.items[old]
	if !exists {
		return Error{queue.Name, "ChangeKey", old, ErrNotFound}
	}

	delete(queue.items, old)
	queue.items[new] = item

	if val, exists := queue.dependants[old]; exists {
		delete(queue.dependants, old)
		queue.dependants[new] = val
	}

	for _, items := range queue.dependants {
		if val, exists := items[old]; exists {
			delete(items, old)
			items[new] = val
		}
	}

	for _, item := range queue.items {
		item.ChangedKey(old, new)
	}

	return nil
}

// SetDelay is a thread-safe way to change the delay of an item.
func (queue *Queue) SetDelay(key string, delay time.Duration) error {
	queue.mutex.Lock()
	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "SetDelay", key, ErrQueueClosed}
	}

	item, exists := queue.items[key]
	if !exists {
		queue.mutex.Unlock()
		return Error{queue.Name, "SetDelay", key, ErrNotFound}
	}

	item.mutex.Lock()
	if item.delay != delay {
		item.delay = delay
		if item.state == ItemStateDelay {
			item.mutex.Unlock()
			item.restart()
			queue.delayQueue.update(item)
			queue.mutex.Unlock()
			queue.delayNotificationTrigger(item)
			return nil
		}
	}
	item.mutex.Unlock()
	queue.mutex.Unlock()
	return nil
}

// SetReserveGroup is a thread-safe way to change the ReserveGroup of an item.
func (queue *Queue) SetReserveGroup(key string, newGroup string) error {
	queue.mutex.Lock()
	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "SetReserveGroup", key, ErrQueueClosed}
	}

	item, exists := queue.items[key]
	if !exists {
		queue.mutex.Unlock()
		return Error{queue.Name, "SetReserveGroup", key, ErrNotFound}
	}

	item.mutex.Lock()
	oldGroup := item.ReserveGroup
	if oldGroup != newGroup {
		item.ReserveGroup = newGroup
		if item.state == ItemStateReady {
			item.mutex.Unlock()
			queue.readyQueue.update(item, oldGroup)
		} else {
			item.mutex.Unlock()
		}
	} else {
		item.mutex.Unlock()
	}
	queue.mutex.Unlock()
	return nil
}

// Reserve is a thread-safe way to get the highest priority (or for those with
// equal priority, the oldest (by time since the item was first Add()ed) item in
// the queue, switching it from the ready sub-queue to the run sub-queue, and in
// so doing starting its ttr countdown.
//
// If reserveGroup is not blank, you will get the next item that was added with
// the given ReserveGroup (conversely, if your items were added with
// ReserveGroups but you don't supply one here, you will not get an item).
//
// If wait is greater than 0, we will wait for up to that much time for an item
// to appear in the ready sub-queue, if at least 1 isn't already there. If after
// this time there is still nothing in the ready sub-queue, no item and a
// ErrNothingReady error is returned.
//
// You need to Remove() the item when you're done with it. If you're still doing
// something and ttr is approaching, Touch() it, otherwise it will be assumed
// you died and the item will be released back to the ready sub-queue
// automatically, to be handled by someone else that gets it from a Reserve()
// call. If you know you can't handle it right now, but someone else might be
// able to later, you can manually call Release(), which moves it to the delay
// sub-queue.
func (queue *Queue) Reserve(reserveGroup string, wait time.Duration) (*Item, error) {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return nil, Error{queue.Name, "Reserve", "", ErrQueueClosed}
	}

	// pop an item from the ready queue and add it to the run queue
	item := queue.readyQueue.pop(reserveGroup)
	if item == nil {
		if wait > 0 {
			ch := make(chan bool, 1)
			queue.readyQueue.notifyPush(reserveGroup, ch, wait)
			queue.mutex.Unlock()

			// wait until something is pushed to the ready queue or we hit the
			// timeout
			tryAgain := <-ch
			if tryAgain {
				queue.mutex.Lock()
				item = queue.readyQueue.pop(reserveGroup)
				if item == nil {
					queue.mutex.Unlock()
				}
			}
		} else {
			queue.mutex.Unlock()
		}

		if item == nil {
			return item, Error{queue.Name, "Reserve", "", ErrNothingReady}
		}
	}

	item.touch()
	queue.runQueue.push(item)
	item.switchReadyRun()

	queue.mutex.Unlock()
	queue.ttrNotificationTrigger(item)
	queue.changed(SubQueueReady, SubQueueRun, []*Item{item})

	return item, nil
}

// Touch is a thread-safe way to extend the amount of time a Reserve()d item
// is allowed to run.
func (queue *Queue) Touch(key string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Touch", key, ErrQueueClosed}
	}

	// check it's actually still in the queue first
	item, ok := queue.items[key]
	if !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Touch", key, ErrNotFound}
	}

	// and it must be in the run queue
	if ok = item.state == ItemStateRun; !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Touch", key, ErrNotRunning}
	}

	// touch and update the heap
	item.touch()
	queue.runQueue.update(item)

	queue.mutex.Unlock()
	queue.ttrNotificationTrigger(item)

	return nil
}

// Release is a thread-safe way to switch an item in the run sub-queue to the
// delay sub-queue, for when the item should be dealt with later, not now.
func (queue *Queue) Release(ctx context.Context, key string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Release", key, ErrQueueClosed}
	}

	// check it's actually still in the queue first
	item, ok := queue.items[key]
	if !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Release", key, ErrNotFound}
	}

	// and it must be in the run queue
	if ok = item.state == ItemStateRun; !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Release", key, ErrNotRunning}
	}

	// switch from run to delay queue (unless there is no delay, in which case
	// straight to ready)
	queue.runQueue.remove(item)
	if item.delay.Nanoseconds() == 0 {
		item.switchRunReady()
		queue.readyQueue.push(item)
		queue.mutex.Unlock()
		queue.changed(SubQueueRun, SubQueueReady, []*Item{item})
		queue.readyAdded(ctx, "released")
	} else {
		item.restart()
		queue.delayQueue.push(item)
		item.switchRunDelay()
		queue.mutex.Unlock()
		queue.delayNotificationTrigger(item)
		queue.changed(SubQueueRun, SubQueueDelay, []*Item{item})
	}

	return nil
}

// Bury is a thread-safe way to switch an item in the run sub-queue to the
// bury sub-queue, for when the item can't be dealt with ever, at least until
// the user takes some action and changes something.
func (queue *Queue) Bury(key string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Bury", key, ErrQueueClosed}
	}

	// check it's actually still in the queue first
	item, ok := queue.items[key]
	if !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Bury", key, ErrNotFound}
	}

	// and it must be in the run queue
	if ok = item.state == ItemStateRun; !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Bury", key, ErrNotRunning}
	}

	// switch from run to bury queue
	queue.runQueue.remove(item)
	queue.buryQueue.push(item)
	item.switchRunBury()
	queue.mutex.Unlock()
	queue.changed(SubQueueRun, SubQueueBury, []*Item{item})

	return nil
}

// Kick is a thread-safe way to switch an item in the bury sub-queue to the
// ready sub-queue, for when a previously buried item can now be handled.
func (queue *Queue) Kick(ctx context.Context, key string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Kick", key, ErrQueueClosed}
	}

	// check it's actually still in the queue first
	item, ok := queue.items[key]
	if !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Kick", key, ErrNotFound}
	}

	// and it must be in the bury queue
	if ok = item.state == ItemStateBury; !ok {
		queue.mutex.Unlock()
		return Error{queue.Name, "Kick", key, ErrNotBuried}
	}

	// switch from bury to ready or dependent queue
	queue.buryQueue.remove(item)
	if queue.itemHasDeps(item) {
		queue.depQueue.push(item)
		item.switchBuryDependent()
		queue.mutex.Unlock()
		queue.changed(SubQueueBury, SubQueueDependent, []*Item{item})
	} else {
		queue.readyQueue.push(item)
		item.switchBuryReady()
		queue.mutex.Unlock()
		queue.changed(SubQueueBury, SubQueueReady, []*Item{item})
		queue.readyAdded(ctx, "kicked")
	}
	return nil
}

// Remove is a thread-safe way to remove an item from the queue.
func (queue *Queue) Remove(ctx context.Context, key string) error {
	queue.mutex.Lock()

	if queue.closed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Remove", key, ErrQueueClosed}
	}

	// check it's actually still in the queue first
	item, existed := queue.items[key]
	if !existed {
		queue.mutex.Unlock()
		return Error{queue.Name, "Remove", key, ErrNotFound}
	}

	// transfer any dependants to the ready queue
	addedReady := false
	var addedReadyItems []*Item
	if deps, exists := queue.dependants[key]; exists {
		for _, dep := range deps {
			done := dep.resolveDependency(key)
			if done && dep.state == ItemStateDependent {
				queue.depQueue.remove(dep)

				// put it straight on the ready queue, regardless of delay value
				dep.switchDependentReady()
				queue.readyQueue.push(dep)
				addedReadyItems = append(addedReadyItems, dep)
				addedReady = true
			}
		}
		delete(queue.dependants, key)
	}

	// if this item is dependent on other items, update those items that this is
	// no longer dependent upon them
	for _, parent := range item.dependencies {
		if deps, exists := queue.dependants[parent]; exists {
			delete(deps, key)
			if len(deps) == 0 {
				delete(queue.dependants, parent)
			}
		}
	}

	// remove from the queue
	delete(queue.items, key)

	// remove from the current sub-queue
	switch item.state {
	case ItemStateDelay:
		queue.delayQueue.remove(item)
		queue.changed(SubQueueDelay, SubQueueRemoved, []*Item{item})
	case ItemStateReady:
		queue.readyQueue.remove(item)
		queue.changed(SubQueueReady, SubQueueRemoved, []*Item{item})
	case ItemStateRun:
		queue.runQueue.remove(item)
		queue.changed(SubQueueRun, SubQueueRemoved, []*Item{item})
	case ItemStateBury:
		queue.buryQueue.remove(item)
		queue.changed(SubQueueBury, SubQueueRemoved, []*Item{item})
	case ItemStateDependent:
		queue.depQueue.remove(item)
		queue.changed(SubQueueDependent, SubQueueRemoved, []*Item{item})
	}
	item.removalCleanup()

	queue.mutex.Unlock()
	if addedReady {
		queue.changed(SubQueueDependent, SubQueueReady, addedReadyItems)
		queue.readyAdded(ctx, "dependent")
	}

	return nil
}

// HasDependents tells you if the item with the given key has any other items
// depending upon it. You'd want to check this before Remove()ing this item if
// you're removing it because it was undesired as opposed to complete, as
// Remove() always triggers dependent items to become ready.
func (queue *Queue) HasDependents(ctx context.Context, key string) (bool, error) {
	queue.mutex.Lock()
	defer queue.mutex.Unlock()

	if queue.closed {
		return false, Error{queue.Name, "Remove", key, ErrQueueClosed}
	}

	_, has := queue.dependants[key]
	return has, nil
}

func (queue *Queue) startDelayProcessing(ctx context.Context) {
	sendStarted := true
	for {
		queue.mutex.Lock()
		var sleepTime time.Duration
		if queue.delayQueue.len() > 0 {
			sleepTime = time.Until(queue.delayQueue.firstItem().ReadyAt())
		} else {
			sleepTime = 1 * time.Hour
		}

		queue.delayTime = time.Now().Add(sleepTime)
		queue.mutex.Unlock()
		if sendStarted {
			queue.startedDelayProcessing <- true
		}

		select {
		case <-time.After(time.Until(queue.delayTime)):
			queue.mutex.Lock()
			len := queue.delayQueue.len()
			addedReady := false
			var items []*Item
			for i := 0; i < len; i++ {
				item := queue.delayQueue.firstItem()

				if !item.isready() {
					break
				}

				// remove it from the delay sub-queue and add it to the ready
				// sub-queue
				queue.delayQueue.remove(item)
				queue.readyQueue.push(item)
				item.switchDelayReady()
				items = append(items, item)
				addedReady = true
			}
			queue.mutex.Unlock()
			if addedReady {
				queue.changed(SubQueueDelay, SubQueueReady, items)
				queue.readyAdded(ctx, "delayed")
			}
			sendStarted = false
		case <-queue.delayNotification:
			sendStarted = true
			continue
		case <-queue.delayClose:
			return
		}
	}
}

func (queue *Queue) delayNotificationTrigger(item *Item) {
	queue.mutex.RLock()
	if queue.delayTime.After(time.Now().Add(item.delay)) {
		queue.mutex.RUnlock()
		queue.delayNotification <- true
		<-queue.startedDelayProcessing
	} else {
		queue.mutex.RUnlock()
	}
}

func (queue *Queue) startTTRProcessing(ctx context.Context) {
	sendStarted := true
	for {
		var sleepTime time.Duration
		queue.mutex.Lock()
		if queue.runQueue.len() > 0 {
			sleepTime = time.Until(queue.runQueue.firstItem().ReleaseAt())
		} else {
			sleepTime = 1 * time.Hour
		}

		queue.ttrTime = time.Now().Add(sleepTime)
		queue.mutex.Unlock()
		if sendStarted {
			queue.startedTTRProcessing <- true
		}

		select {
		case <-time.After(time.Until(queue.ttrTime)):
			queue.mutex.Lock()
			length := queue.runQueue.len()
			var delayedItems, buriedItems, readyItems []*Item
			for i := 0; i < length; i++ {
				item := queue.runQueue.firstItem()

				if !item.releasable() {
					break
				}

				// obey the ttr callback
				moveTo := queue.ttrCb(item.Data())
				if moveTo == SubQueueRun {
					// increase this item's time to release to a year from now,
					// but keep it in the run queue
					item.tempDisableTTR()
					queue.runQueue.update(item)
				} else {
					// remove it from the ttr sub-queue and move to another
					queue.runQueue.remove(item)
					switch moveTo {
					case SubQueueDelay:
						item.restart()
						queue.delayQueue.push(item)
						item.switchRunDelay(true)
						delayedItems = append(delayedItems, item)
					case SubQueueBury:
						queue.buryQueue.push(item)
						item.switchRunBury(true)
						buriedItems = append(buriedItems, item)
					default:
						queue.readyQueue.push(item)
						item.switchRunReady()
						readyItems = append(readyItems, item)
					}
				}
			}

			queue.mutex.Unlock()
			if len(delayedItems) > 0 {
				for _, item := range delayedItems {
					queue.delayNotificationTrigger(item)
				}
				queue.changed(SubQueueRun, SubQueueDelay, delayedItems)
			}
			if len(buriedItems) > 0 {
				queue.changed(SubQueueRun, SubQueueBury, buriedItems)
			}
			if len(readyItems) > 0 {
				queue.changed(SubQueueRun, SubQueueReady, readyItems)
				queue.readyAdded(ctx, "ttr")
			}
			sendStarted = false
		case <-queue.ttrNotification:
			sendStarted = true
			continue
		case <-queue.ttrClose:
			return
		}
	}
}

func (queue *Queue) ttrNotificationTrigger(item *Item) {
	queue.mutex.RLock()
	if queue.ttrTime.After(time.Now().Add(item.ttr)) {
		queue.mutex.RUnlock()
		queue.ttrNotification <- true
		<-queue.startedTTRProcessing
	} else {
		queue.mutex.RUnlock()
	}
}
