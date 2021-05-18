// Copyright © 2016, 2019, 2021 Genome Research Limited
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

package queue

// subqueue implements a heap structure for items in the various sub-queues, so
// that they can efficiently get the next item in desired order.

import (
	"container/heap"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	logext "github.com/inconshreveable/log15/ext"
)

type subQueue struct {
	mutex                    sync.RWMutex
	items                    []*Item
	groupedItems             map[string][]*Item
	sqIndex                  int
	reserveGroup             string
	pushNotificationChannels map[string]map[string]chan bool
}

// create a new subQueue that can hold *Items in "priority" order. sqIndex is
// one of 0 (priority is based on the item's delay), 1 (priority is based on the
// item's priority or creation) or 2 (priority is based on the item's ttr).
func newSubQueue(sqIndex int) *subQueue {
	queue := &subQueue{
		sqIndex:                  sqIndex,
		pushNotificationChannels: make(map[string]map[string]chan bool),
	}
	if sqIndex == 1 {
		queue.groupedItems = make(map[string][]*Item)
	}
	heap.Init(queue)
	return queue
}

// notifyPush lets you supply a channel that will then receive true whenever the
// next item with the given ReserveGroup (of if reserveGroup is blank, no
// ReserveGroup) is push()ed to this subQueue. It will also receive true if an
// item that has already been pushed this subQueue has its ReserveGroup updated
// to the given reserverGroup.
//
// After 1 matching item has been pushed or updated, the supplied ch will not be
// used again.
//
// If timeout duration passes before a matching item is pushed, the ch will
// receive false and not be used again.
func (q *subQueue) notifyPush(reserveGroup string, ch chan bool, timeout time.Duration) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	var chans map[string]chan bool
	if val, ok := q.pushNotificationChannels[reserveGroup]; ok {
		chans = val
	} else {
		chans = make(map[string]chan bool)
	}
	id := logext.RandId(8)
	chans[id] = ch
	q.pushNotificationChannels[reserveGroup] = chans

	go func() {
		<-time.After(timeout)
		q.mutex.Lock()
		defer q.mutex.Unlock()
		if chans, ok := q.pushNotificationChannels[reserveGroup]; ok {
			if ch, ok := chans[id]; ok {
				ch <- false
				delete(chans, id)
				if len(q.pushNotificationChannels[reserveGroup]) == 0 {
					delete(q.pushNotificationChannels, reserveGroup)
				}
			}
		}
	}()
}

// triggerNotify is used to check if we should notify about the given
// reserverGroup and send true on the registered channels if so. You
// must hold the mutext lock before calling this.
func (q *subQueue) triggerNotify(reserveGroup string) {
	if chans, ok := q.pushNotificationChannels[reserveGroup]; ok {
		for _, ch := range chans {
			ch <- true
		}
		delete(q.pushNotificationChannels, reserveGroup)
	}
}

// push adds an item to the queue
func (q *subQueue) push(item *Item) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.sqIndex == 1 {
		q.reserveGroup = item.ReserveGroup
	}
	defer q.triggerNotify(q.reserveGroup)
	heap.Push(q, item)
}

// pop removes the next item from the queue according to its "priority"
func (q *subQueue) pop(reserveGroup ...string) *Item {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	var itemList []*Item
	if q.sqIndex == 1 {
		var group string
		if len(reserveGroup) == 1 {
			group = reserveGroup[0]
		}
		var existed bool
		if itemList, existed = q.groupedItems[group]; !existed {
			return nil
		}
		q.reserveGroup = group
	} else {
		itemList = q.items
	}
	if len(itemList) == 0 {
		return nil
	}
	return heap.Pop(q).(*Item)
}

// remove removes a given item from the queue
func (q *subQueue) remove(item *Item) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.sqIndex == 1 {
		q.reserveGroup = item.ReserveGroup
	}
	heap.Remove(q, item.queueIndexes[q.sqIndex])
}

// len tells you how many items are in the queue
func (q *subQueue) len(reserveGroup ...string) int {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	var itemList []*Item
	if q.sqIndex == 1 {
		if len(reserveGroup) == 1 {
			group := reserveGroup[0]
			var existed bool
			if itemList, existed = q.groupedItems[group]; !existed {
				return 0
			}
		} else {
			num := 0
			for _, il := range q.groupedItems {
				num += len(il)
			}
			return num
		}
	} else {
		itemList = q.items
	}
	return len(itemList)
}

// firstItem is useful in testing to get the first item in the queue in a
// thread-safe way.
func (q *subQueue) firstItem() *Item {
	q.mutex.RLock()
	defer q.mutex.RUnlock()
	return q.items[0]
}

// update ensures that if an item's "priority" characteristic(s) change, that
// its order in the queue is corrected. Optional oldGroup is the previous
// ReserveGroup that this item had, supplied if the group changed.
func (q *subQueue) update(item *Item, oldGroup ...string) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.sqIndex == 1 && len(oldGroup) == 1 && oldGroup[0] != item.ReserveGroup {
		q.reserveGroup = oldGroup[0]
		heap.Remove(q, item.queueIndexes[q.sqIndex])
		q.reserveGroup = item.ReserveGroup
		defer q.triggerNotify(q.reserveGroup)
		heap.Push(q, item)
		return
	}
	heap.Fix(q, item.queueIndexes[q.sqIndex])
}

// empty clears out a queue, setting it back to its new state
func (q *subQueue) empty() {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.sqIndex == 1 {
		q.groupedItems = make(map[string][]*Item)
	} else {
		q.items = nil
	}
}

// the following functions are required for the heap implementation, and though
// they are exported they are not supposed to be used directly - use the above
// methods instead

func (q *subQueue) Len() int {
	var itemList []*Item
	if q.sqIndex == 1 {
		var existed bool
		if itemList, existed = q.groupedItems[q.reserveGroup]; !existed {
			return 0
		}
	} else {
		itemList = q.items
	}
	return len(itemList)
}

func (q *subQueue) Less(i, j int) bool {
	switch q.sqIndex {
	case 0:
		return q.items[i].readyAt.Before(q.items[j].readyAt)
	case 1:
		if itemList, existed := q.groupedItems[q.reserveGroup]; existed {
			if itemList[i].priority == itemList[j].priority {
				if itemList[i].size == itemList[j].size {
					return itemList[i].creation.Before(itemList[j].creation)
				}
				return itemList[i].size > itemList[j].size
			}
			return itemList[i].priority > itemList[j].priority
		}
		return false
	}
	// case 2, outside the switch, because we need to return
	return q.items[i].releaseAt.Before(q.items[j].releaseAt)
}

func (q *subQueue) Swap(i, j int) {
	var itemList []*Item
	if q.sqIndex == 1 {
		var existed bool
		if itemList, existed = q.groupedItems[q.reserveGroup]; !existed {
			return
		}
	} else {
		itemList = q.items
	}

	itemList[i], itemList[j] = itemList[j], itemList[i]
	lockFirst := i
	lockSecond := j
	if itemList[i].iid > itemList[j].iid {
		lockFirst = j
		lockSecond = i
	}
	itemList[lockFirst].mutex.Lock()
	defer itemList[lockFirst].mutex.Unlock()
	if i != j {
		itemList[lockSecond].mutex.Lock()
		defer itemList[lockSecond].mutex.Unlock()
	}

	itemList[i].queueIndexes[q.sqIndex] = i
	itemList[j].queueIndexes[q.sqIndex] = j
}

func (q *subQueue) Push(x interface{}) {
	item := x.(*Item)
	var itemList []*Item
	if q.sqIndex == 1 {
		var existed bool
		if itemList, existed = q.groupedItems[q.reserveGroup]; !existed {
			q.groupedItems[q.reserveGroup] = itemList
		}
	} else {
		itemList = q.items
	}
	item.mutex.Lock()
	item.queueIndexes[q.sqIndex] = len(itemList)
	item.mutex.Unlock()
	itemList = append(itemList, item)
	if q.sqIndex == 1 {
		q.groupedItems[q.reserveGroup] = itemList
	} else {
		q.items = itemList
	}
}

func (q *subQueue) Pop() interface{} {
	var itemList []*Item
	if q.sqIndex == 1 {
		var existed bool
		if itemList, existed = q.groupedItems[q.reserveGroup]; !existed {
			return nil
		}
	} else {
		itemList = q.items
	}
	lasti := len(itemList) - 1
	item := itemList[lasti]
	item.mutex.Lock()
	item.queueIndexes[q.sqIndex] = -1
	item.mutex.Unlock()
	itemList = itemList[:lasti]
	if q.sqIndex == 1 {
		q.groupedItems[q.reserveGroup] = itemList
	} else {
		q.items = itemList
	}
	return item
}
