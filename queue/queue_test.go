// Copyright © 2016, 2018, 2019, 2021 Genome Research Limited
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

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	. "github.com/smartystreets/goconvey/convey"
)

type changedStruct struct {
	from  SubQueue
	to    SubQueue
	count int
}

func TestQueue(t *testing.T) {
	ctx := context.Background()

	Convey("Adding multiple items with a delay to fresh queues always works", t, func() {
		done := make(chan bool, 1)
		go func() {
			// this test proves we fixed a deadlock bug, hence testing against
			// a timeout, but with go-deadlock and race detection, it can take
			// longer than 5s
			<-time.After(10 * time.Second)
			done <- false
		}()
		go func() {
			for l := 0; l < 1000; l++ {
				queue := New(ctx, "myqueue")
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key_%d", i)
					t := time.Duration((i+1)*100) * time.Millisecond
					_, erra := queue.Add(ctx, key, "", "data", 0, t, 0*time.Millisecond, "")
					if erra != nil {
						done <- false
					}
				}
				qdestroy(queue)
			}
			done <- true
		}()
		So(<-done, ShouldBeTrue)
	})

	Convey("Reserving multiple items with a ttr always works", t, func() {
		done := make(chan bool, 1)
		go func() {
			<-time.After(45 * time.Second) // with go-deadlock plus race detection, the following tests are just fundamentally slow; only ~1s needed without these
			done <- false
		}()
		go func() {
			for l := 0; l < 1000; l++ {
				queue := New(ctx, "myqueue")
				for i := 0; i < 10; i++ {
					key := fmt.Sprintf("key_%d", i)
					t := time.Duration((i+1)*100) * time.Millisecond
					_, erra := queue.Add(ctx, key, "", "data", 0, 0*time.Millisecond, t, "")
					if erra != nil {
						fmt.Printf("\nqueue.Add() failed: %s\n", erra)
						done <- false
					}
				}
				for i := 0; i < 10; i++ {
					_, errr := queue.Reserve("", 0)
					if errr != nil {
						fmt.Printf("\nqueue.Reserve() failed: %s\n", errr)
					}
				}
				qdestroy(queue)
			}
			done <- true
		}()
		So(<-done, ShouldBeTrue)
	})

	Convey("Once 10 items of differing delay and ttr have been added to the queue", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		var callBackLock sync.Mutex
		var numReadyAdded int
		var enableWaitForReadyAdded bool
		waitForReadyAdded := make(chan bool)
		queue.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
			callBackLock.Lock()
			defer callBackLock.Unlock()
			numReadyAdded = len(allitemdata)
			if enableWaitForReadyAdded {
				waitForReadyAdded <- true
			}
		})

		var callBackLock2 sync.Mutex
		var changed *changedStruct
		var changes []*changedStruct
		var enableWaitForChanged bool
		var enableChangedCollection bool
		waitForChanged := make(chan bool)
		queue.SetChangedCallback(func(from, to SubQueue, data []interface{}) {
			callBackLock2.Lock()
			defer callBackLock2.Unlock()
			changed = &changedStruct{from, to, len(data)}
			if enableChangedCollection {
				changes = append(changes, &changedStruct{from, to, len(data)})
			}
			if enableWaitForChanged {
				enableWaitForChanged = false
				waitForChanged <- true
			}
		})

		prepareToCheckChanged := func() {
			callBackLock2.Lock()
			enableChangedCollection = true
			callBackLock2.Unlock()
		}

		searchChanged := func(c []*changedStruct, from, to SubQueue, count int) bool {
			for _, cs := range c {
				if cs.from != from || cs.to != to || cs.count != count {
					continue
				}
				return true
			}
			return false
		}

		checkChanged := func(from, to SubQueue, count int) bool {
			callBackLock2.Lock()
			defer callBackLock2.Unlock()
			if enableChangedCollection {
				if len(changes) == 0 {
					enableWaitForChanged = true
					callBackLock2.Unlock()
					<-waitForChanged
					callBackLock2.Lock()
				}

				// because the change we have might be an old undesired one that
				// came through after prepareToCheckChanged() was called but
				// before our desired change happened, we check for the desired
				// value now and wait some more if not present
				if searchChanged(changes, from, to, count) {
					return true
				}
				for {
					enableWaitForChanged = true
					callBackLock2.Unlock()
					changedLimit := time.After(100 * time.Millisecond)
					select {
					case <-waitForChanged:
						callBackLock2.Lock()
						if searchChanged(changes, from, to, count) {
							changes = nil
							enableChangedCollection = false
							return true
						}
						continue
					case <-changedLimit:
						callBackLock2.Lock()
						changes = nil
						enableWaitForChanged = false
						enableChangedCollection = false
						return false
					}
				}
			}
			return searchChanged([]*changedStruct{changed}, from, to, count)
		}

		items := make(map[string]*Item)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			t := time.Duration((i+1)*100) * time.Millisecond
			item, err := queue.Add(ctx, key, "", "data", 0, t, t, "")
			So(err, ShouldBeNil)
			items[key] = item
		}

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 10)
		So(stats.Delayed, ShouldEqual, 10)
		So(stats.Ready, ShouldEqual, 0)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 0)

		Convey("You can get an item back out", func() {
			item, err := queue.Get("key_0")
			So(err, ShouldBeNil)
			So(item.Data(), ShouldEqual, "data")
			So(item.creation, ShouldHappenOnOrBefore, items["key_0"].creation)
		})

		Convey("You can't get an non-existent item", func() {
			item, err := queue.Get("key_fake")
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNotFound)
		})

		Convey("You can get all items back out", func() {
			items := queue.AllItems()
			So(len(items), ShouldEqual, 10)
		})

		Convey("When nothing is running, GetRunningData returns nothing", func() {
			data := queue.GetRunningData()
			So(len(data), ShouldEqual, 0)
		})

		Convey("You can't add the same item again", func() {
			item, err := queue.Add(ctx, "key_0", "", "data new", 0, 100*time.Millisecond, 100*time.Millisecond, "")
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrAlreadyExists)
			So(err.Error(), ShouldEqual, "queue(myqueue) Add(key_0): already exists")
			So(item.Data(), ShouldEqual, "data")
			So(item.creation, ShouldHappenOnOrBefore, items["key_0"].creation)
		})

		Convey("They should start delayed and gradually become ready", func() {
			<-time.After(110 * time.Millisecond)
			stats = queue.Stats()
			So(stats.Delayed, ShouldEqual, 9)
			So(stats.Ready, ShouldEqual, 1)
			<-time.After(110 * time.Millisecond)
			stats = queue.Stats()
			So(stats.Delayed, ShouldEqual, 8)
			So(stats.Ready, ShouldEqual, 2)
			<-time.After(110 * time.Millisecond)
			stats = queue.Stats()
			So(stats.Delayed, ShouldEqual, 7)
			So(stats.Ready, ShouldEqual, 3)
			callBackLock.Lock()
			So(numReadyAdded, ShouldEqual, 3)
			numReadyAdded = 0
			callBackLock.Unlock()
			So(checkChanged(SubQueueDelay, SubQueueReady, 1), ShouldBeTrue)

			callBackLock.Lock()
			enableWaitForReadyAdded = true
			callBackLock.Unlock()
			queue.TriggerReadyAddedCallback(ctx)
			<-waitForReadyAdded
			callBackLock.Lock()
			So(numReadyAdded, ShouldEqual, 3)
			callBackLock.Unlock()

			Convey("Once ready you should be able to reserve them in the expected order", func() {
				item1, err := queue.Reserve("", 0)
				So(err, ShouldBeNil)
				So(item1, ShouldNotBeNil)
				So(item1.Key, ShouldEqual, "key_0")
				So(item1.reserves, ShouldEqual, 1)
				item2, err := queue.Reserve("", 0)
				So(err, ShouldBeNil)
				So(item2, ShouldNotBeNil)
				So(item2.Key, ShouldEqual, "key_1")
				prepareToCheckChanged()
				item3, err := queue.Reserve("", 0)
				So(err, ShouldBeNil)
				So(checkChanged(SubQueueReady, SubQueueRun, 1), ShouldBeTrue)
				So(item3, ShouldNotBeNil)
				So(item3.Key, ShouldEqual, "key_2")
				item4, err := queue.Reserve("", 0)
				So(err, ShouldNotBeNil)
				So(item4, ShouldBeNil)
				qerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNothingReady)

				stats = queue.Stats()
				So(stats.Items, ShouldEqual, 10)
				So(stats.Delayed, ShouldEqual, 7)
				So(stats.Ready, ShouldEqual, 0)
				So(stats.Running, ShouldEqual, 3)

				Convey("Once reserved, you can get the data with GetRunningData", func() {
					data := queue.GetRunningData()
					So(len(data), ShouldEqual, 3)
				})

				Convey("Once reserved you can release them", func() {
					So(item1.State(), ShouldEqual, ItemStateRun)
					So(item1.releases, ShouldEqual, 0)
					prepareToCheckChanged()
					err := queue.Release(ctx, item1.Key)
					So(err, ShouldBeNil)
					So(checkChanged(SubQueueRun, SubQueueDelay, 1), ShouldBeTrue)
					So(item1.State(), ShouldEqual, ItemStateDelay)
					So(item1.releases, ShouldEqual, 1)

					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 10)
					So(stats.Delayed, ShouldEqual, 8)
					So(stats.Ready, ShouldEqual, 0)
					So(stats.Running, ShouldEqual, 2)

					Convey("You can get read-only item stats at any point", func() {
						itemstats := item1.Stats()
						So(itemstats.State, ShouldEqual, ItemStateDelay)
						So(itemstats.Releases, ShouldEqual, 1)
					})

					Convey("Once released, items become ready after their delay", func() {
						<-time.After(50 * time.Millisecond)
						So(item1.State(), ShouldEqual, ItemStateDelay)
						<-time.After(60 * time.Millisecond)
						So(item1.State(), ShouldEqual, ItemStateReady)
						stats = queue.Stats()
						So(stats.Delayed, ShouldEqual, 6)
						So(stats.Ready, ShouldEqual, 2)
						So(checkChanged(SubQueueDelay, SubQueueReady, 1), ShouldBeTrue)

						Convey("Once reserved, the delay can be altered and this affects the next release", func() {
							item1, err = queue.Reserve("", 0)
							So(err, ShouldBeNil)
							So(item1, ShouldNotBeNil)
							So(item1.Key, ShouldEqual, "key_0")

							err = queue.SetDelay(item1.Key, 20*time.Millisecond)
							So(err, ShouldBeNil)
							err = queue.Release(ctx, item1.Key)
							So(err, ShouldBeNil)
							So(item1.State(), ShouldEqual, ItemStateDelay)
							So(item1.releases, ShouldEqual, 2)

							<-time.After(10 * time.Millisecond)
							So(item1.State(), ShouldEqual, ItemStateDelay)
							<-time.After(20 * time.Millisecond)
							So(item1.State(), ShouldEqual, ItemStateReady)
						})

						Convey("Once reserved and released, the delay can be altered", func() {
							item1, err = queue.Reserve("", 0)
							So(err, ShouldBeNil)
							So(item1, ShouldNotBeNil)
							So(item1.Key, ShouldEqual, "key_0")
							err = queue.Release(ctx, item1.Key)
							So(err, ShouldBeNil)
							So(item1.State(), ShouldEqual, ItemStateDelay)
							So(item1.releases, ShouldEqual, 2)

							err = queue.SetDelay(item1.Key, 30*time.Millisecond)
							So(err, ShouldBeNil)

							<-time.After(20 * time.Millisecond)
							So(item1.State(), ShouldEqual, ItemStateDelay)
							<-time.After(20 * time.Millisecond)
							So(item1.State(), ShouldEqual, ItemStateReady)
						})
					})
				})

				Convey("Or remove them", func() {
					So(item2.State(), ShouldEqual, ItemStateRun)
					prepareToCheckChanged()
					err := queue.Remove(ctx, item2.Key)
					So(err, ShouldBeNil)
					So(checkChanged(SubQueueRun, SubQueueRemoved, 1), ShouldBeTrue)
					So(item2.State(), ShouldEqual, ItemStateRemoved)

					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 9)
					So(stats.Delayed, ShouldEqual, 7)
					So(stats.Ready, ShouldEqual, 0)
					So(stats.Running, ShouldEqual, 2)

					Convey("Releasing, touching, burying, kicking and updating fail after removal", func() {
						err := queue.Release(ctx, item2.Key)
						So(err, ShouldNotBeNil)
						qerr, ok := err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
						err = queue.Touch(item2.Key)
						So(err, ShouldNotBeNil)
						qerr, ok = err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
						err = queue.Bury(item2.Key)
						So(err, ShouldNotBeNil)
						qerr, ok = err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
						err = queue.Kick(ctx, item2.Key)
						So(err, ShouldNotBeNil)
						qerr, ok = err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
						err = queue.Update(ctx, item2.Key, "", item2.Data(), 0, 0*time.Second, 0*time.Second)
						So(err, ShouldNotBeNil)
						qerr, ok = err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
						err = queue.SetDelay(item2.Key, 0*time.Second)
						So(err, ShouldNotBeNil)
						qerr, ok = err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
					})
				})

				Convey("Or bury them", func() {
					So(item3.State(), ShouldEqual, ItemStateRun)
					So(item3.buries, ShouldEqual, 0)
					prepareToCheckChanged()
					err := queue.Bury(item3.Key)
					So(err, ShouldBeNil)
					So(checkChanged(SubQueueRun, SubQueueBury, 1), ShouldBeTrue)
					So(item3.State(), ShouldEqual, ItemStateBury)
					So(item3.buries, ShouldEqual, 1)

					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 10)
					So(stats.Delayed, ShouldEqual, 7)
					So(stats.Ready, ShouldEqual, 0)
					So(stats.Running, ShouldEqual, 2)
					So(stats.Buried, ShouldEqual, 1)

					Convey("Once buried you can kick them", func() {
						So(item3.kicks, ShouldEqual, 0)
						prepareToCheckChanged()
						err := queue.Kick(ctx, item3.Key)
						So(err, ShouldBeNil)
						So(checkChanged(SubQueueBury, SubQueueReady, 1), ShouldBeTrue)
						So(item3.State(), ShouldEqual, ItemStateReady)
						So(item3.kicks, ShouldEqual, 1)

						stats = queue.Stats()
						So(stats.Items, ShouldEqual, 10)
						So(stats.Delayed, ShouldEqual, 7)
						So(stats.Ready, ShouldEqual, 1)
						So(stats.Running, ShouldEqual, 2)
						So(stats.Buried, ShouldEqual, 0)
					})

					Convey("You can also remove them whilst buried", func() {
						prepareToCheckChanged()
						err := queue.Remove(ctx, "key_2")
						So(err, ShouldBeNil)
						So(checkChanged(SubQueueBury, SubQueueRemoved, 1), ShouldBeTrue)

						stats = queue.Stats()
						So(stats.Items, ShouldEqual, 9)
						So(stats.Delayed, ShouldEqual, 7)
						So(stats.Ready, ShouldEqual, 0)
						So(stats.Running, ShouldEqual, 2)
						So(stats.Buried, ShouldEqual, 0)

						err = queue.Remove(ctx, "key_2")
						So(err, ShouldNotBeNil)
						qerr, ok := err.(Error)
						So(ok, ShouldBeTrue)
						So(qerr.Err, ShouldEqual, ErrNotFound)
					})
				})

				Convey("If not buried you can't kick them", func() {
					err := queue.Kick(ctx, item3.Key)
					So(err, ShouldNotBeNil)
					qerr, ok := err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrNotBuried)
					So(item3.State(), ShouldEqual, ItemStateRun)
				})

				Convey("If you do nothing they get auto-released to the ready queue", func() {
					<-time.After(50 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateRun)
					<-time.After(60 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateReady)
					So(item1.Stats().Timeouts, ShouldEqual, 1)
					stats = queue.Stats()
					So(stats.Ready, ShouldEqual, 2)
					So(stats.Running, ShouldEqual, 2)
					So(checkChanged(SubQueueRun, SubQueueReady, 1), ShouldBeTrue)
					<-time.After(110 * time.Millisecond)
					So(item2.State(), ShouldEqual, ItemStateReady)
					stats = queue.Stats()
					So(stats.Ready, ShouldEqual, 4)
					So(stats.Running, ShouldEqual, 1)
					<-time.After(110 * time.Millisecond)
					So(item3.State(), ShouldEqual, ItemStateReady)
					stats = queue.Stats()
					So(stats.Ready, ShouldEqual, 6)
					So(stats.Running, ShouldEqual, 0)

					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 10)
					So(stats.Delayed, ShouldEqual, 4)
				})

				Convey("When they hit their ttr you can choose to delay them", func() {
					queue.SetTTRCallback(func(data interface{}) SubQueue {
						return SubQueueDelay
					})
					defer queue.SetTTRCallback(defaultTTRCallback)

					<-time.After(50 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateRun)
					stats = queue.Stats()
					So(stats.Delayed, ShouldBeBetweenOrEqual, 6, 7) // with go-deadlock and race detection, the answer varies from normal, but this is not the thing we're really interested in testing
					So(stats.Ready, ShouldBeBetweenOrEqual, 0, 1)
					So(stats.Running, ShouldEqual, 3)
					<-time.After(60 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateDelay)
					So(item1.Stats().Timeouts, ShouldEqual, 1)
					stats = queue.Stats()
					So(stats.Delayed, ShouldBeBetweenOrEqual, 6, 7)
					So(stats.Ready, ShouldBeBetweenOrEqual, 1, 2)
					So(stats.Running, ShouldEqual, 2)
					So(checkChanged(SubQueueRun, SubQueueDelay, 1), ShouldBeTrue)
				})

				Convey("When they hit their ttr you can choose to bury them", func() {
					queue.SetTTRCallback(func(data interface{}) SubQueue {
						return SubQueueBury
					})
					defer queue.SetTTRCallback(defaultTTRCallback)

					<-time.After(50 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateRun)
					stats = queue.Stats()
					So(stats.Buried, ShouldEqual, 0)
					So(stats.Delayed, ShouldBeBetweenOrEqual, 6, 7)
					So(stats.Ready, ShouldBeBetweenOrEqual, 0, 1)
					So(stats.Running, ShouldEqual, 3)
					<-time.After(60 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateBury)
					So(item1.Stats().Timeouts, ShouldEqual, 1)
					stats = queue.Stats()
					So(stats.Buried, ShouldEqual, 1)
					So(stats.Delayed, ShouldBeBetweenOrEqual, 5, 6)
					So(stats.Ready, ShouldBeBetweenOrEqual, 1, 2)
					So(stats.Running, ShouldEqual, 2)
					So(checkChanged(SubQueueRun, SubQueueBury, 1), ShouldBeTrue)
				})

				Convey("Though you can prevent auto-release by touching them", func() {
					<-time.After(50 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateRun)
					err := queue.Touch(item1.Key)
					So(err, ShouldBeNil)
					<-time.After(60 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateRun)
					So(item1.Stats().Timeouts, ShouldEqual, 0)
					stats = queue.Stats()
					So(stats.Ready, ShouldEqual, 1)
					So(stats.Running, ShouldEqual, 3)
					<-time.After(45 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateReady)
					So(item1.Stats().Timeouts, ShouldEqual, 1)
					stats = queue.Stats()
					// if the total elapsed time since the items were added to the queue goes over 500ms, we can get an extra 'Ready' item
					So(stats.Ready, ShouldBeBetweenOrEqual, 2, 3)
					So(stats.Running, ShouldEqual, 2)
					<-time.After(60 * time.Millisecond)
					So(item2.State(), ShouldEqual, ItemStateReady)
					stats = queue.Stats()
					So(stats.Ready, ShouldEqual, 4)
					So(stats.Running, ShouldEqual, 1)

					err = queue.Touch("item_fake")
					So(err, ShouldNotBeNil)
					qerr, ok := err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrNotFound)
				})

				Convey("Touching doesn't mess with the correct queue order", func() {
					queue = New(ctx, "new queue")
					defer qdestroy(queue)
					_, erra := queue.Add(ctx, "item1", "", "data", 0, 0*time.Millisecond, 50*time.Millisecond, "")
					So(erra, ShouldBeNil)
					_, erra = queue.Add(ctx, "item2", "", "data", 0, 0*time.Millisecond, 52*time.Millisecond, "")
					So(erra, ShouldBeNil)
					<-time.After(1 * time.Millisecond)
					item1, erra := queue.Reserve("", 0)
					So(erra, ShouldBeNil)
					So(item1.Key, ShouldEqual, "item1")
					So(item1.State(), ShouldEqual, ItemStateRun)
					item2, erra := queue.Reserve("", 0)
					So(erra, ShouldBeNil)
					So(item2.Key, ShouldEqual, "item2")
					So(item2.State(), ShouldEqual, ItemStateRun)

					<-time.After(25 * time.Millisecond)

					So(queue.runQueue.firstItem().Key, ShouldEqual, "item1")
					erra = queue.Touch(item1.Key)
					So(erra, ShouldBeNil)
					So(queue.runQueue.firstItem().Key, ShouldEqual, "item2")

					<-time.After(30 * time.Millisecond)

					So(item1.State(), ShouldEqual, ItemStateRun)
					So(item2.State(), ShouldEqual, ItemStateReady)

					<-time.After(25 * time.Millisecond)
					So(item1.State(), ShouldEqual, ItemStateReady)
				})

				Convey("Finally, you can destroy the queue, which doesn't let you do much else after", func() {
					err := queue.Destroy()
					So(err, ShouldBeNil)
					stats := queue.Stats()
					So(stats.Items, ShouldEqual, 0)
					So(stats.Delayed, ShouldEqual, 0)
					So(stats.Ready, ShouldEqual, 0)
					So(stats.Running, ShouldEqual, 0)
					So(stats.Buried, ShouldEqual, 0)

					_, err = queue.Add(ctx, "fake", "", "data", 0, 0*time.Second, 0*time.Second, "")
					So(err, ShouldNotBeNil)
					qerr, ok := err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					_, err = queue.Get("fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Update(ctx, "fake", "", "data", 0, 0*time.Second, 0*time.Second)
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					_, err = queue.Reserve("", 0)
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					_, err = queue.Reserve("", 0)
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Touch("fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Release(ctx, "fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Bury("fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Kick(ctx, "fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.Remove(ctx, "fake")
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
					err = queue.SetDelay("fake", 0*time.Second)
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)

					err = queue.Destroy()
					So(err, ShouldNotBeNil)
					qerr, ok = err.(Error)
					So(ok, ShouldBeTrue)
					So(qerr.Err, ShouldEqual, ErrQueueClosed)
				})
			})

			Convey("If not reserved you can't release, touch, bury or kick them", func() {
				err := queue.Release(ctx, "key_0")
				So(err, ShouldNotBeNil)
				qerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotRunning)
				err = queue.Touch("key_0")
				So(err, ShouldNotBeNil)
				qerr, ok = err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotRunning)
				err = queue.Bury("key_0")
				So(err, ShouldNotBeNil)
				qerr, ok = err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotRunning)
				err = queue.Kick(ctx, "key_0")
				So(err, ShouldNotBeNil)
				qerr, ok = err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotBuried)
			})

			Convey("But you can remove them when ready", func() {
				err := queue.Remove(ctx, "key_0")
				So(err, ShouldBeNil)

				stats := queue.Stats()
				So(stats.Items, ShouldEqual, 9)
				So(stats.Delayed, ShouldEqual, 7)
				So(stats.Ready, ShouldEqual, 2)
				So(stats.Running, ShouldEqual, 0)
				So(stats.Buried, ShouldEqual, 0)

				err = queue.Remove(ctx, "key_0")
				So(err, ShouldNotBeNil)
				qerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotFound)
			})
		})

		Convey("If not ready you can't reserve them", func() {
			item, err := queue.Reserve("", 0)
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNothingReady)
		})

		Convey("But you can remove them when not ready", func() {
			err := queue.Remove(ctx, "key_0")
			So(err, ShouldBeNil)

			stats := queue.Stats()
			So(stats.Items, ShouldEqual, 9)
			So(stats.Delayed, ShouldEqual, 9)
			So(stats.Ready, ShouldEqual, 0)
			So(stats.Running, ShouldEqual, 0)
			So(stats.Buried, ShouldEqual, 0)

			err = queue.Remove(ctx, "key_0")
			So(err, ShouldNotBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNotFound)
		})
	})

	Convey("Once an item been added to the queue", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)
		item, err := queue.Add(ctx, "item1", "", "data", 0, 50*time.Millisecond, 50*time.Millisecond, "")
		So(err, ShouldBeNil)

		Convey("It can be removed from the queue immediately prior to it getting switched to the ready queue", func() {
			<-time.After(49 * time.Millisecond)
			err = queue.Remove(ctx, "item1")
			So(err, ShouldBeNil)
			<-time.After(6 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateRemoved)

			stats := queue.Stats()
			So(stats.Items, ShouldEqual, 0)
			So(stats.Delayed, ShouldEqual, 0)
			So(stats.Ready, ShouldEqual, 0)

			Convey("Once removed it can't be updated", func() {
				err = queue.Update(ctx, "item1", "", "data", 0, 75*time.Millisecond, 50*time.Millisecond)
				So(err, ShouldNotBeNil)
				qerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(qerr.Err, ShouldEqual, ErrNotFound)
			})
		})

		Convey("The queue won't fall over if we manage to change the item's readyAt without updating the queue", func() {
			So(item.State(), ShouldEqual, ItemStateDelay)
			<-time.After(45 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateDelay)
			item.mutex.Lock()
			item.readyAt = time.Now().Add(25 * time.Millisecond)
			item.mutex.Unlock()
			<-time.After(10 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateDelay)
			<-time.After(25 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateReady)
		})

		Convey("The delay can be updated even in the delay queue", func() {
			So(item.State(), ShouldEqual, ItemStateDelay)
			<-time.After(25 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateDelay)
			err = queue.Update(ctx, "item1", "", "data", 0, 75*time.Millisecond, 50*time.Millisecond)
			So(err, ShouldBeNil)
			<-time.After(30 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateDelay)
			<-time.After(50 * time.Millisecond)
			So(item.State(), ShouldEqual, ItemStateReady)

			Convey("When ready the priority can be updated", func() {
				err = queue.Update(ctx, "item1", "", "data", 1, 75*time.Millisecond, 50*time.Millisecond)
				So(err, ShouldBeNil)
				So(item.priority, ShouldEqual, 1)
			})

			Convey("When ready the ReserveGroup can be changed with Update()", func() {
				err = queue.Update(ctx, "item1", "newGroup", "data", 0, 75*time.Millisecond, 50*time.Millisecond)
				So(err, ShouldBeNil)
				So(item.ReserveGroup, ShouldEqual, "newGroup")
			})

			Convey("When ready the ReserveGroup can be changed with SetReserveGroup()", func() {
				gotItem, errr := queue.Reserve("newGroup", 0)
				So(errr, ShouldNotBeNil)
				So(gotItem, ShouldBeNil)

				err = queue.SetReserveGroup("item1", "newGroup")
				So(err, ShouldBeNil)
				So(item.ReserveGroup, ShouldEqual, "newGroup")

				gotItem, err = queue.Reserve("newGroup", 0)
				So(err, ShouldBeNil)
				// So(gotItem, ShouldNotBeNil) *** this causes a data race since goconvey must be trying to access members of gotItem
				if gotItem == nil {
					So(false, ShouldBeTrue)
				} else {
					So(gotItem.Key, ShouldEqual, "item1")
				}
			})
		})

		Convey("Once reserved", func() {
			<-time.After(55 * time.Millisecond)
			_, err = queue.Reserve("", 0)
			So(err, ShouldBeNil)

			Convey("It can be removed from the queue immediately prior to it getting switched to the ready queue", func() {
				<-time.After(49 * time.Millisecond)
				err = queue.Remove(ctx, "item1")
				So(err, ShouldBeNil)
				<-time.After(6 * time.Millisecond)
				So(item.State(), ShouldEqual, ItemStateRemoved)

				stats := queue.Stats()
				So(stats.Items, ShouldEqual, 0)
				So(stats.Delayed, ShouldEqual, 0)
				So(stats.Ready, ShouldEqual, 0)
			})

			Convey("The queue won't fall over if we manage to change the item's releaseAt without updating the queue", func() {
				So(item.State(), ShouldEqual, ItemStateRun)
				<-time.After(49 * time.Millisecond)
				item.mutex.Lock()
				if item.state == ItemStateRun {
					item.releaseAt = time.Now().Add(25 * time.Millisecond)
					item.mutex.Unlock()
					<-time.After(6 * time.Millisecond)
					So(item.State(), ShouldEqual, ItemStateRun)
					<-time.After(25 * time.Millisecond)
					So(item.State(), ShouldEqual, ItemStateReady)
				} else {
					// due to timing vagueries, the state might not be run, so
					// just wait until it's definitely ready
					item.mutex.Unlock()
					<-time.After(35 * time.Millisecond)
					So(item.State(), ShouldEqual, ItemStateReady)
				}
			})

			Convey("When running the ttr can be updated", func() {
				So(item.State(), ShouldEqual, ItemStateRun)
				<-time.After(25 * time.Millisecond)
				So(item.State(), ShouldEqual, ItemStateRun)
				err := queue.Update(ctx, "item1", "", "data", 0, 50*time.Millisecond, 75*time.Millisecond)
				So(err, ShouldBeNil)
				<-time.After(30 * time.Millisecond)
				So(item.State(), ShouldEqual, ItemStateRun)
				<-time.After(50 * time.Millisecond)
				So(item.State(), ShouldEqual, ItemStateReady)
			})
		})
	})

	Convey("You can add items to the queue that start in the run or bury sub-queues", t, func() {
		queue := New(ctx, "run/bury queue")
		defer func() {
			errd := queue.Destroy()
			So(errd, ShouldBeNil)
		}()

		_, err := queue.Add(ctx, "key_run", "", "data", 0, 100*time.Millisecond, 100*time.Millisecond, SubQueueRun)
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_bury", "", "data", 0, 100*time.Millisecond, 100*time.Millisecond, SubQueueBury)
		So(err, ShouldBeNil)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 2)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 0)
		So(stats.Running, ShouldEqual, 1)
		So(stats.Buried, ShouldEqual, 1)

		<-time.After(110 * time.Millisecond)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 2)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 1)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 1)
	})

	Convey("You can add items to the queue that have sizes", t, func() {
		queue := New(ctx, "size queue")
		defer func() {
			errd := queue.Destroy()
			So(errd, ShouldBeNil)
		}()

		_, err := queue.Add(ctx, "key_normal", "", "data", 0, 0*time.Millisecond, 100*time.Millisecond, "")
		So(err, ShouldBeNil)
		_, err = queue.AddWithSize(ctx, "key_large", "", "data", 0, 1, 0*time.Millisecond, 100*time.Millisecond, "")
		So(err, ShouldBeNil)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 2)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 2)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 0)

		item, err := queue.Reserve("", 0)
		So(err, ShouldBeNil)
		So(item, ShouldNotBeNil)
		So(item.Key, ShouldEqual, "key_large")
	})

	Convey("Once a thousand items with no delay have been added to the queue", t, func() {
		queue := New(ctx, "1000 queue")
		defer qdestroy(queue)
		type testdata struct {
			ID int
		}
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			dataid := rand.Intn(999)
			_, err := queue.Add(ctx, key, "", &testdata{ID: dataid}, 0, 0*time.Second, 30*time.Second, "")
			So(err, ShouldBeNil)
		}

		Convey("They are immediately all ready", func() {
			stats := queue.Stats()
			So(stats.Items, ShouldEqual, 1000)
			So(stats.Delayed, ShouldEqual, 0)
			So(stats.Ready, ShouldEqual, 1000)
			So(stats.Running, ShouldEqual, 0)
			So(stats.Buried, ShouldEqual, 0)

			Convey("And can all be reserved", func() {
				for i := 0; i < 1000; i++ {
					item, err := queue.Reserve("", 0)
					So(err, ShouldBeNil)
					So(item, ShouldNotBeNil)
					So(item.Key, ShouldEqual, fmt.Sprintf("key_%d", i))
				}

				Convey("And when released are immediately ready", func() {
					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 1000)
					So(stats.Delayed, ShouldEqual, 0)
					So(stats.Ready, ShouldEqual, 0)
					So(stats.Running, ShouldEqual, 1000)
					err := queue.Release(ctx, "key_0")
					So(err, ShouldBeNil)
					stats = queue.Stats()
					So(stats.Items, ShouldEqual, 1000)
					So(stats.Delayed, ShouldEqual, 0)
					So(stats.Ready, ShouldEqual, 1)
					So(stats.Running, ShouldEqual, 999)
				})
			})
		})

		Convey("You can change the group of items with SetReserveGroup()", func() {
			item, err := queue.Reserve("1001", 0)
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)

			err = queue.SetReserveGroup("key_1", "1001")
			So(err, ShouldBeNil)

			err = queue.SetReserveGroup("key_2", "1001")
			So(err, ShouldBeNil)

			item, err = queue.Reserve("1001", 0)
			So(err, ShouldBeNil)
			So(item, ShouldNotBeNil)
			So(item.Key, ShouldEqual, "key_1")
			So(item.ReserveGroup, ShouldEqual, "1001")

			item, err = queue.Reserve("1001", 0)
			So(err, ShouldBeNil)
			So(item, ShouldNotBeNil)
			So(item.Key, ShouldEqual, "key_2")
			So(item.ReserveGroup, ShouldEqual, "1001")

			item, err = queue.Reserve("1001", 0)
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)
		})
	})

	Convey("Once a thousand items with no delay and differing ReserveGroups have been added to the queue", t, func() {
		queue := New(ctx, "1000 queue")
		defer qdestroy(queue)
		type testdata struct {
			ID int
		}
		var dataids []int
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			dataid := rand.Intn(999)
			dataids = append(dataids, dataid)
			_, err := queue.Add(ctx, key, fmt.Sprintf("%d", dataid), &testdata{ID: dataid}, 0, 0*time.Second, 30*time.Second, "")
			So(err, ShouldBeNil)
		}

		Convey("They are immediately all ready", func() {
			stats := queue.Stats()
			So(stats.Items, ShouldEqual, 1000)
			So(stats.Delayed, ShouldEqual, 0)
			So(stats.Ready, ShouldEqual, 1000)
			So(stats.Running, ShouldEqual, 0)
			So(stats.Buried, ShouldEqual, 0)
		})

		Convey("They can be reserved by specifying a group", func() {
			item, err := queue.Reserve("1001", 0)
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNothingReady)

			sort.Ints(dataids)
			for _, dataid := range dataids {
				item, err := queue.Reserve(fmt.Sprintf("%d", dataid), 0)
				So(err, ShouldBeNil)
				So(item, ShouldNotBeNil)
				So(item.Data().(*testdata).ID, ShouldEqual, dataid)
			}
		})

		Convey("You can change a group with SetReserveGroup()", func() {
			item, err := queue.Reserve("1001", 0)
			So(err, ShouldNotBeNil)
			So(item, ShouldBeNil)

			err = queue.SetReserveGroup("key_1", "1001")
			So(err, ShouldBeNil)

			item, err = queue.Reserve("1001", 0)
			So(err, ShouldBeNil)
			So(item, ShouldNotBeNil)
			So(item.Key, ShouldEqual, "key_1")
			So(item.ReserveGroup, ShouldEqual, "1001")
		})
	})

	Convey("Once a thousand items with a small delay have been added to the queue", t, func() {
		queue := New(ctx, "1000 queue")
		defer qdestroy(queue)
		t := time.Now()
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := queue.Add(ctx, key, "", "data", 0, 100*time.Millisecond, 30*time.Second, "")
			So(err, ShouldBeNil)
		}
		e := time.Since(t)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 1000)
		if e < 100*time.Millisecond {
			So(stats.Delayed, ShouldEqual, 1000)
			So(stats.Ready, ShouldEqual, 0)
		}
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 0)

		<-time.After(110 * time.Millisecond)

		Convey("They are all ready after that delay", func() {
			stats := queue.Stats()
			So(stats.Items, ShouldEqual, 1000)
			So(stats.Delayed, ShouldEqual, 0)
			So(stats.Ready, ShouldEqual, 1000)
			So(stats.Running, ShouldEqual, 0)
			So(stats.Buried, ShouldEqual, 0)

			Convey("And can all be reserved", func() {
				for i := 0; i < 1000; i++ {
					item, err := queue.Reserve("", 0)
					So(err, ShouldBeNil)
					So(item, ShouldNotBeNil)
					So(item.Key, ShouldEqual, fmt.Sprintf("key_%d", i))
				}
			})
		})
	})

	Convey("You can add many items to the queue in one go", t, func() {
		q := New(ctx, "myqueue")
		defer qdestroy(q)

		item, err := q.Reserve("", 0)
		So(err, ShouldNotBeNil)
		So(item, ShouldBeNil)
		qerr, ok := err.(Error)
		So(ok, ShouldBeTrue)
		So(qerr.Err, ShouldEqual, ErrNothingReady)

		var itemdefs []*ItemDef
		for i := 0; i < 10; i++ {
			itemdefs = append(itemdefs, &ItemDef{
				Key:      fmt.Sprintf("key_%d", i),
				Data:     "data",
				Priority: 0,
				Delay:    100 * time.Millisecond,
				TTR:      1 * time.Minute,
			})
		}

		added, dups, err := q.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 10)
		So(dups, ShouldEqual, 0)

		added, dups, err = q.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 0)
		So(dups, ShouldEqual, 10)

		for i := 10; i < 20; i++ {
			itemdefs = append(itemdefs, &ItemDef{
				Key:      fmt.Sprintf("key_%d", i),
				Data:     "data",
				Priority: 0,
				Delay:    50 * time.Millisecond,
				TTR:      1 * time.Minute,
			})
		}

		added, dups, err = q.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 10)
		So(dups, ShouldEqual, 10)

		itemdefs = append(itemdefs, &ItemDef{
			Key:      fmt.Sprintf("key_%d", 99),
			Data:     "data",
			Priority: 0,
			Delay:    0 * time.Millisecond,
			TTR:      1 * time.Minute,
		})
		added, dups, err = q.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 1)
		So(dups, ShouldEqual, 20)

		Convey("It doesn't work if the queue is closed", func() {
			err = q.Destroy()
			So(err, ShouldBeNil)
			_, _, err = q.AddMany(ctx, itemdefs)
			So(err, ShouldNotBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrQueueClosed)
		})
	})

	Convey("You can add many items to the queue that start in the run or bury sub-queues, all in one go", t, func() {
		queue := New(ctx, "run/bury queue")
		defer func() {
			errd := queue.Destroy()
			So(errd, ShouldBeNil)
		}()

		queues := []SubQueue{SubQueueRun, SubQueueBury}
		var itemdefs []*ItemDef
		for i := 0; i < 10; i++ {
			for _, subQueue := range queues {
				itemdefs = append(itemdefs, &ItemDef{
					Key:        fmt.Sprintf("key_%d_%s", i, subQueue),
					Data:       "data",
					Priority:   0,
					Delay:      100 * time.Millisecond,
					TTR:        100 * time.Millisecond,
					StartQueue: subQueue,
				})
			}
		}

		added, dups, err := queue.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 20)
		So(dups, ShouldEqual, 0)

		added, dups, err = queue.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 0)
		So(dups, ShouldEqual, 20)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 20)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 0)
		So(stats.Running, ShouldEqual, 10)
		So(stats.Buried, ShouldEqual, 10)

		<-time.After(110 * time.Millisecond)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 20)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 10)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 10)
	})

	Convey("Once some items with dependencies have been added to the queue", t, func() {
		// https://i-msdn.sec.s-msft.com/dynimg/IC332764.gif
		queue := New(ctx, "dep queue")
		defer qdestroy(queue)
		_, err := queue.Add(ctx, "key_1", "", "1", 0, 0*time.Second, 30*time.Second, "")
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_2", "", "2", 0, 0*time.Second, 30*time.Second, "")
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_3", "", "3", 0, 0*time.Second, 30*time.Second, "")
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_4", "", "4", 0, 0*time.Second, 30*time.Second, "", []string{"key_1"})
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_5", "five", "5", 0, 0*time.Second, 30*time.Second, "", []string{"key_1", "key_2", "key_3"})
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_6", "", "6", 0, 0*time.Second, 30*time.Second, "", []string{"key_3", "key_4"})
		So(err, ShouldBeNil)
		fivesixdep, err := queue.Add(ctx, "key_7", "", "7", 0, 0*time.Second, 30*time.Second, "", []string{"key_5", "key_6"})
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key_8", "", "8", 0, 0*time.Second, 30*time.Second, "", []string{"key_5"})
		So(err, ShouldBeNil)

		So(fivesixdep.Dependencies(), ShouldResemble, []string{"key_5", "key_6"})

		Convey("Only the non-dependent items are immediately ready", func() {
			depTestFunc(ctx, queue, false)
		})

		Convey("HasDependents works", func() {
			hasDeps, err := queue.HasDependents(ctx, "key_8")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeFalse)
			hasDeps, err = queue.HasDependents(ctx, "key_2")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeTrue)

			err = queue.Remove(ctx, "key_5")
			So(err, ShouldBeNil)

			hasDeps, err = queue.HasDependents(ctx, "key_2")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeFalse)
		})

		Convey("You can update dependencies", func() {
			four, err := queue.Get("key_4")
			So(err, ShouldBeNil)
			fourStats := four.Stats()
			So(four.Dependencies(), ShouldResemble, []string{"key_1"})
			So(fourStats.State, ShouldEqual, ItemStateDependent)
			hasDeps, err := queue.HasDependents(ctx, "key_1")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeTrue)

			err = queue.Update(ctx, "key_4", "", four.Data(), fourStats.Priority, fourStats.Delay, fourStats.TTR, []string{})
			So(err, ShouldBeNil)

			So(four.Dependencies(), ShouldResemble, []string{})
			So(four.Stats().State, ShouldEqual, ItemStateReady)
			hasDeps, err = queue.HasDependents(ctx, "key_1")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeTrue)

			five, err := queue.Get("key_5")
			So(err, ShouldBeNil)
			fiveStats := five.Stats()
			So(five.Dependencies(), ShouldResemble, []string{"key_1", "key_2", "key_3"})
			So(fiveStats.State, ShouldEqual, ItemStateDependent)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_2", "key_3"})
			So(err, ShouldBeNil)

			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateDependent)

			So(five.Dependencies(), ShouldResemble, []string{"key_2", "key_3"})
			hasDeps, err = queue.HasDependents(ctx, "key_1")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeFalse)

			err = queue.Remove(ctx, "key_2")
			So(err, ShouldBeNil)
			err = queue.Remove(ctx, "key_3")
			So(err, ShouldBeNil)
			<-time.After(6 * time.Millisecond)

			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateReady)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_2", "key_1", "key_3"})
			So(err, ShouldBeNil)

			So(five.Dependencies(), ShouldResemble, []string{"key_2", "key_1", "key_3"})
			hasDeps, err = queue.HasDependents(ctx, "key_1")
			So(err, ShouldBeNil)
			So(hasDeps, ShouldBeTrue)
			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateDependent)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_2", "key_3"})
			So(err, ShouldBeNil)

			// (you can be dependent on items that do not exist in the queue)
			So(five.Stats().State, ShouldEqual, ItemStateDependent)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{})
			So(err, ShouldBeNil)
			So(five.Stats().State, ShouldEqual, ItemStateReady)

			five, err = queue.Reserve("five", 0)
			So(err, ShouldBeNil)
			So(five, ShouldNotBeNil)
			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateRun)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_2", "key_1", "key_3"})
			So(err, ShouldBeNil)

			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateDependent)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{})
			So(err, ShouldBeNil)

			So(five.Stats().State, ShouldEqual, ItemStateReady)

			five, err = queue.Reserve("five", 0)
			So(err, ShouldBeNil)
			So(five, ShouldNotBeNil)
			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateRun)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, 1*time.Second, fiveStats.TTR, []string{})
			So(err, ShouldBeNil)

			err = queue.Release(ctx, five.Key)
			So(err, ShouldBeNil)
			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateDelay)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_2", "key_1", "key_3"})
			So(err, ShouldBeNil)

			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateDependent)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{})
			So(err, ShouldBeNil)

			So(five.Stats().State, ShouldEqual, ItemStateReady)

			five, err = queue.Reserve("five", 0)
			So(err, ShouldBeNil)
			So(five, ShouldNotBeNil)
			So(five.Stats().State, ShouldEqual, ItemStateRun)

			err = queue.Bury(five.Key)
			So(err, ShouldBeNil)
			fiveStats = five.Stats()
			So(fiveStats.State, ShouldEqual, ItemStateBury)

			err = queue.Update(ctx, "key_5", "five", five.Data(), fiveStats.Priority, fiveStats.Delay, fiveStats.TTR, []string{"key_1"})
			So(err, ShouldBeNil)

			So(five.Stats().State, ShouldEqual, ItemStateBury)

			err = queue.Kick(ctx, five.Key)
			So(err, ShouldBeNil)
			So(five.Stats().State, ShouldEqual, ItemStateDependent)

			err = queue.Remove(ctx, "key_1")
			So(err, ShouldBeNil)
			<-time.After(6 * time.Millisecond)

			So(five.Stats().State, ShouldEqual, ItemStateReady)
		})

		Convey("You can change item keys without breaking dependencies", func() {
			err := queue.ChangeKey("key_3", "changed_3")
			So(err, ShouldBeNil)
			err = queue.ChangeKey("key_6", "changed_6")
			So(err, ShouldBeNil)
			err = queue.ChangeKey("key_7", "changed_7")
			So(err, ShouldBeNil)

			err = queue.ChangeKey("foo", "bar")
			So(err, ShouldNotBeNil)
			qerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNotFound)

			err = queue.ChangeKey("key_1", "changed_3")
			So(err, ShouldNotBeNil)
			qerr, ok = err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrAlreadyExists)

			_, err = queue.Get("key_3")
			So(err, ShouldNotBeNil)
			qerr, ok = err.(Error)
			So(ok, ShouldBeTrue)
			So(qerr.Err, ShouldEqual, ErrNotFound)

			item, err := queue.Get("changed_3")
			So(err, ShouldBeNil)
			So(item.Key, ShouldEqual, "changed_3")

			depTestFunc(ctx, queue, true)
		})

		Convey("You can add dependencies on non-exist items and resolve them later", func() {
			ten, err := queue.Add(ctx, "key_10", "", "10", 0, 0*time.Second, 30*time.Second, "", []string{"key_9"})
			So(err, ShouldBeNil)
			So(ten.Stats().State, ShouldEqual, ItemStateDependent)

			_, err = queue.Add(ctx, "key_9", "", "9", 0, 0*time.Second, 30*time.Second, "", []string{})
			So(err, ShouldBeNil)
			So(ten.Stats().State, ShouldEqual, ItemStateDependent)

			err = queue.Remove(ctx, "key_9")
			So(err, ShouldBeNil)
			<-time.After(6 * time.Millisecond)

			So(ten.Stats().State, ShouldEqual, ItemStateReady)
		})
	})

	Convey("Once some items with dependencies have been added to the queue en-masse", t, func() {
		// same setup as in previous test
		queue := New(ctx, "dep many queue")
		defer qdestroy(queue)

		var itemdefs []*ItemDef
		itemdefs = append(itemdefs, &ItemDef{
			Key:  "key_1",
			Data: "1",
			TTR:  30 * time.Second,
		})
		itemdefs = append(itemdefs, &ItemDef{
			Key:  "key_2",
			Data: "2",
			TTR:  30 * time.Second,
		})
		itemdefs = append(itemdefs, &ItemDef{"key_3", "", "3", 0, 0 * time.Second, 30 * time.Second, "", []string{}})
		itemdefs = append(itemdefs, &ItemDef{"key_4", "", "4", 0, 0 * time.Second, 30 * time.Second, "", []string{"key_1"}})
		itemdefs = append(itemdefs, &ItemDef{"key_5", "", "5", 0, 0 * time.Second, 30 * time.Second, "", []string{"key_2", "key_3"}})
		itemdefs = append(itemdefs, &ItemDef{"key_6", "", "6", 0, 0 * time.Second, 30 * time.Second, "", []string{"key_3", "key_4"}})
		itemdefs = append(itemdefs, &ItemDef{"key_7", "", "7", 0, 0 * time.Second, 30 * time.Second, "", []string{"key_5", "key_6"}})
		itemdefs = append(itemdefs, &ItemDef{"key_8", "", "8", 0, 0 * time.Second, 30 * time.Second, "", []string{"key_5"}})

		added, dups, err := queue.AddMany(ctx, itemdefs)
		So(err, ShouldBeNil)
		So(added, ShouldEqual, 8)
		So(dups, ShouldEqual, 0)

		item7, err := queue.Get("key_7")
		So(err, ShouldBeNil)
		So(item7.Dependencies(), ShouldResemble, []string{"key_5", "key_6"})

		Convey("Only the non-dependent items are immediately ready", func() {
			depTestFunc(ctx, queue, false)
		})
	})

	Convey("When you add items to the queue over time, slow readyAddedCallbacks only get called once at a time", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		var callBackLock sync.RWMutex
		var added []int
		queue.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
			callBackLock.Lock()
			defer callBackLock.Unlock()
			added = append(added, len(allitemdata))
			time.Sleep(50 * time.Millisecond)
		})

		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			_, err := queue.Add(ctx, key, "", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
			So(err, ShouldBeNil)
			if i == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 10)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 10)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 0)

		callBackLock.RLock()
		So(len(added), ShouldEqual, 1)
		So(added[0], ShouldEqual, 1)
		callBackLock.RUnlock()

		<-time.After(650 * time.Millisecond)

		callBackLock.RLock()
		So(len(added), ShouldEqual, 2)
		So(added[1], ShouldEqual, 10)
		callBackLock.RUnlock()
	})

	Convey("You can reserve with a wait time, reserving before items are even added", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 0)

		item, err := queue.Reserve("foo", 0)
		So(item, ShouldBeNil)
		So(err, ShouldNotBeNil)

		addErrCh := make(chan error)
		go func() {
			<-time.After(5 * time.Millisecond)
			_, err := queue.Add(ctx, "key1", "bar", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
			addErrCh <- err
		}()
		go func() {
			<-time.After(20 * time.Millisecond)
			_, err := queue.Add(ctx, "key2", "foo", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
			addErrCh <- err
		}()

		rCh0 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("bar", 1*time.Millisecond)
			rCh0 <- item == nil && err != nil && time.Since(t) < 5*time.Millisecond
		}()

		rCh1 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("foo", 10*time.Millisecond)
			rCh1 <- item == nil && err != nil && time.Since(t) < 15*time.Millisecond
		}()

		rCh2 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("foo", 30*time.Millisecond)
			rCh2 <- item != nil && err == nil && time.Since(t) < 25*time.Millisecond && item.Key == "key2"
		}()

		So(<-rCh0, ShouldBeTrue)
		So(<-addErrCh, ShouldBeNil)
		So(<-rCh1, ShouldBeTrue)
		So(<-addErrCh, ShouldBeNil)
		So(<-rCh2, ShouldBeTrue)
	})

	Convey("Multiple clients can reserve with a wait time before items are even added", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 0)

		// with race detection and using go-deadlock, just adding 2 items to the
		// queue takes ~15ms instead of ~0.1ms, so the times here allow for that
		// kind of leeway

		willReserve := make(map[int]chan bool)
		willReserve[1] = make(chan bool)
		willReserve[2] = make(chan bool)
		addTimes := make(map[int]chan time.Time)
		addTimes[1] = make(chan time.Time)
		addTimes[2] = make(chan time.Time)
		rCh := make(chan bool)
		for i := 1; i <= 2; i++ {
			go func(i int) {
				willReserve[i] <- true
				t := time.Now()
				item, err := queue.Reserve("foo", 3000*time.Millisecond)
				addT := <-addTimes[i]
				ok := item != nil && err == nil && time.Since(t) < 2500*time.Millisecond && time.Since(t) > 2000*time.Millisecond && time.Since(addT) < 10*time.Millisecond
				if !ok {
					fmt.Printf("\nitem: %v, err: [%s], time passed: %s, since add: %s\n", item != nil, err, time.Since(t), time.Since(addT))
				}
				rCh <- ok
			}(i)
		}

		addErrCh := make(chan error)
		for i := 1; i <= 2; i++ {
			go func(i int) {
				<-willReserve[i]
				<-time.After(2000 * time.Millisecond)
				_, err := queue.Add(ctx, fmt.Sprintf("key%d", i), "foo", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
				addTimes[i] <- time.Now()
				addErrCh <- err
			}(i)
		}

		So(<-addErrCh, ShouldBeNil)
		So(<-addErrCh, ShouldBeNil)
		So(<-rCh, ShouldBeTrue)
		So(<-rCh, ShouldBeTrue)
	})

	Convey("You can reserve with a wait time, reserving before a dependent item becomes ready", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		_, err := queue.Add(ctx, "key1", "one", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key2", "two", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "", []string{"key1"})
		So(err, ShouldBeNil)

		rmErrCh := make(chan error)
		go func() {
			<-time.After(20 * time.Millisecond)
			err := queue.Remove(ctx, "key1")
			rmErrCh <- err
		}()

		rCh1 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("two", 10*time.Millisecond)
			rCh1 <- item == nil && err != nil && time.Since(t) < 15*time.Millisecond
		}()

		rCh2 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("two", 30*time.Millisecond)
			rCh2 <- item != nil && err == nil && time.Since(t) < 25*time.Millisecond && item.Key == "key2"
		}()

		So(<-rCh1, ShouldBeTrue)
		So(<-rmErrCh, ShouldBeNil)
		So(<-rCh2, ShouldBeTrue)
	})

	Convey("You can reserve with a wait time, reserving before changing an item's reserve group", t, func() {
		queue := New(ctx, "myqueue")
		defer qdestroy(queue)

		_, err := queue.Add(ctx, "key1", "one", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "")
		So(err, ShouldBeNil)
		_, err = queue.Add(ctx, "key2", "two", "data", 0, 0*time.Millisecond, 10*time.Millisecond, "", []string{"key1"})
		So(err, ShouldBeNil)

		rmErrCh := make(chan error)
		go func() {
			<-time.After(20 * time.Millisecond)
			err := queue.Remove(ctx, "key1")
			rmErrCh <- err
		}()

		go func() {
			<-time.After(40 * time.Millisecond)
			err := queue.SetReserveGroup("key2", "three")
			rmErrCh <- err
		}()

		rCh1 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("three", 10*time.Millisecond)
			rCh1 <- item == nil && err != nil && time.Since(t) < 15*time.Millisecond
		}()

		rCh2 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("three", 30*time.Millisecond)
			rCh2 <- item == nil && err != nil && time.Since(t) < 35*time.Millisecond
		}()

		rCh3 := make(chan bool)
		go func() {
			t := time.Now()
			item, err := queue.Reserve("three", 50*time.Millisecond)
			rCh3 <- item != nil && err == nil && time.Since(t) < 45*time.Millisecond && item.Key == "key2"
		}()

		So(<-rCh1, ShouldBeTrue)
		So(<-rmErrCh, ShouldBeNil)
		So(<-rCh2, ShouldBeTrue)
		So(<-rmErrCh, ShouldBeNil)
		So(<-rCh3, ShouldBeTrue)
	})
}

func depTestFunc(ctx context.Context, queue *Queue, changed bool) {
	key3 := "key_3"
	key6 := "key_6"
	key7 := "key_7"
	if changed {
		key3 = "changed_3"
		key6 = "changed_6"
		key7 = "changed_7"
	}

	stats := queue.Stats()
	So(stats.Items, ShouldEqual, 8)
	So(stats.Delayed, ShouldEqual, 0)
	So(stats.Ready, ShouldEqual, 3)
	So(stats.Running, ShouldEqual, 0)
	So(stats.Buried, ShouldEqual, 0)
	So(stats.Dependant, ShouldEqual, 5)

	Convey("Once parent items are removed, dependent items become ready", func() {
		item, err := queue.Get("key_4")
		So(err, ShouldBeNil)
		So(item.Stats().State, ShouldEqual, ItemStateDependent)

		err = queue.Remove(ctx, "key_1")
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		So(item.Stats().State, ShouldEqual, ItemStateReady)

		stats := queue.Stats()
		So(stats.Items, ShouldEqual, 7)
		So(stats.Delayed, ShouldEqual, 0)
		So(stats.Ready, ShouldEqual, 3)
		So(stats.Running, ShouldEqual, 0)
		So(stats.Buried, ShouldEqual, 0)
		So(stats.Dependant, ShouldEqual, 4)

		item, err = queue.Get(key6)
		So(err, ShouldBeNil)
		So(item.Stats().State, ShouldEqual, ItemStateDependent)

		err = queue.Remove(ctx, key3)
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		So(item.Stats().State, ShouldEqual, ItemStateDependent)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 6)
		So(stats.Ready, ShouldEqual, 2)
		So(stats.Dependant, ShouldEqual, 4)

		err = queue.Remove(ctx, "key_4")
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		So(item.Stats().State, ShouldEqual, ItemStateReady)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 5)
		So(stats.Ready, ShouldEqual, 2)
		So(stats.Dependant, ShouldEqual, 3)

		err = queue.Remove(ctx, key6)
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 4)
		So(stats.Ready, ShouldEqual, 1)
		So(stats.Dependant, ShouldEqual, 3)

		item, err = queue.Get("key_5")
		So(err, ShouldBeNil)
		So(item.Stats().State, ShouldEqual, ItemStateDependent)

		err = queue.Remove(ctx, "key_2")
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		So(item.Stats().State, ShouldEqual, ItemStateReady)

		stats = queue.Stats()
		So(stats.Items, ShouldEqual, 3)
		So(stats.Ready, ShouldEqual, 1)
		So(stats.Dependant, ShouldEqual, 2)

		item7, err := queue.Get(key7)
		So(err, ShouldBeNil)
		So(item7.Stats().State, ShouldEqual, ItemStateDependent)
		item8, err := queue.Get("key_8")
		So(err, ShouldBeNil)
		So(item8.Stats().State, ShouldEqual, ItemStateDependent)

		err = queue.Remove(ctx, "key_5")
		So(err, ShouldBeNil)
		<-time.After(6 * time.Millisecond)

		So(item7.Stats().State, ShouldEqual, ItemStateReady)
		So(item8.Stats().State, ShouldEqual, ItemStateReady)
		So(item7.Dependencies(), ShouldResemble, []string{"key_5", key6})
	})
}

func qdestroy(q *Queue) {
	err := q.Destroy()
	if err != nil {
		if qerr, ok := err.(Error); ok && qerr.Err == ErrQueueClosed {
			return
		}
		fmt.Printf("queue.Destroy failed: %s\n", err)
	}
}
