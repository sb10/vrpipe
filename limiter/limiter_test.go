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

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	. "github.com/smartystreets/goconvey/convey"
)

func BenchmarkLimiterIncDec(b *testing.B) {
	ctx := context.Background()
	limits := make(map[string]int)
	limits["l1"] = 5
	limits["l2"] = 6
	cb := func(ctx context.Context, name string) int {
		if limit, exists := limits[name]; exists {
			return limit
		}
		return -1
	}
	both := []string{"l1", "l2"}
	first := []string{"l1"}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		l := New(cb)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Increment(ctx, both)
		l.Decrement(both)
		l.Decrement(both)
		l.Decrement(both)
		l.Decrement(both)
		l.Decrement(both)
		l.Decrement(both)

		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Increment(ctx, first)
		l.Decrement(first)
		l.Decrement(first)
		l.Decrement(first)
		l.Decrement(first)
		l.Decrement(first)
		l.Decrement(first)
	}
}
func BenchmarkLimiterCapacity(b *testing.B) {
	ctx := context.Background()
	limits := make(map[string]int)
	limits["l1"] = 5
	limits["l2"] = 6
	cb := func(ctx context.Context, name string) int {
		if limit, exists := limits[name]; exists {
			return limit
		}
		return -1
	}
	both := []string{"l1", "l2"}
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		l := New(cb)
		for {
			l.Increment(ctx, both)
			cap := l.GetRemainingCapacity(ctx, both)
			if cap == 0 {
				break
			}
		}
		for {
			l.Decrement(both)
			cap := l.GetRemainingCapacity(ctx, both)
			if cap == 5 {
				break
			}
		}
	}
}

func TestLimiter(t *testing.T) {
	ctx := context.Background()
	Convey("You can make a new Limiter with a limit defining callback", t, func() {
		limits := make(map[string]int)
		limits["l1"] = 3
		limits["l2"] = 2
		limits["l4"] = 100
		limits["l5"] = 200
		cb := func(ctx context.Context, name string) int {
			if limit, exists := limits[name]; exists {
				return limit
			}
			return -1
		}

		l := New(cb)
		So(l, ShouldNotBeNil)

		Convey("Increment and Decrement work as expected", func() {
			So(l.Increment(ctx, []string{"l1", "l2"}), ShouldBeTrue)
			l.Decrement([]string{"l1", "l2"})

			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeFalse)
			So(l.Increment(ctx, []string{"l1", "l2"}), ShouldBeFalse)
			l.Decrement([]string{"l1", "l2"})
			So(l.Increment(ctx, []string{"l1", "l2"}), ShouldBeTrue)
			l.Decrement([]string{"l2"})
			So(l.Increment(ctx, []string{"l1", "l2"}), ShouldBeTrue)

			So(l.Increment(ctx, []string{"l3"}), ShouldBeTrue)
			l.Decrement([]string{"l3"})
		})

		Convey("You can change limits with SetLimit(), and Decrement() forgets about unused groups", func() {
			groups := []string{"l1", "l2"}
			two := []string{"l2"}
			So(l.GetLowestLimit(ctx, groups), ShouldEqual, 2)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 2)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 1)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 0)
			So(l.Increment(ctx, two), ShouldBeFalse)
			l.SetLimit("l2", 3)
			So(l.GetLowestLimit(ctx, groups), ShouldEqual, 3)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 1)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 0)
			So(l.Increment(ctx, two), ShouldBeFalse)
			l.Decrement(two)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 1)
			l.Decrement(two)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 2)
			l.Decrement(two)
			// at this point l2 should have been forgotten about, which means
			// we forgot we set the limit to 3
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 2)
			l.Decrement(two) // doesn't panic or something
			So(l.GetLowestLimit(ctx, groups), ShouldEqual, 2)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 2)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 0)
			So(l.Increment(ctx, two), ShouldBeFalse)
			l.Decrement(two)
			l.Decrement(two)
			limits["l2"] = 3
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 3)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.GetLowestLimit(ctx, groups), ShouldEqual, 3)
			So(l.GetRemainingCapacity(ctx, groups), ShouldEqual, 2)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.Increment(ctx, two), ShouldBeTrue)
			So(l.Increment(ctx, two), ShouldBeFalse)
		})

		Convey("You can set multiple limits and then get them all", func() {
			l.SetLimit("l1", 1)
			l.SetLimit("l2", 2)
			lgs := l.GetLimits()
			So(lgs, ShouldResemble, map[string]int{"l1": 1, "l2": 2})
		})

		Convey("You can have limits of 0 and also RemoveLimit()s", func() {
			l.SetLimit("l2", 0)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeFalse)

			limits["l2"] = 0
			l.RemoveLimit("l2")
			So(l.Increment(ctx, []string{"l2"}), ShouldBeFalse)
			So(l.GetLimit(ctx, "l2"), ShouldEqual, 0)

			limits["l2"] = -1
			So(l.Increment(ctx, []string{"l2"}), ShouldBeFalse)
			So(l.GetLimit(ctx, "l2"), ShouldEqual, 0)

			l.RemoveLimit("l2")
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.Increment(ctx, []string{"l2"}), ShouldBeTrue)
			So(l.GetLimit(ctx, "l2"), ShouldEqual, -1)
		})

		Convey("Concurrent SetLimit(), Increment() and Decrement() work", func() {
			var incs uint64
			var fails uint64
			var wg sync.WaitGroup
			for i := 0; i < 200; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					groups := []string{"l4", "l5"}
					if i%2 == 0 {
						groups = []string{"l5", "l4"}
					}
					if l.Increment(ctx, groups) {
						atomic.AddUint64(&incs, 1)
						time.Sleep(100 * time.Millisecond)
						l.Decrement(groups)
					} else {
						atomic.AddUint64(&fails, 1)
						if atomic.LoadUint64(&fails) == 50 {
							l.SetLimit("l4", 125)
						}
					}
				}(i)
			}
			wg.Wait()

			So(atomic.LoadUint64(&incs), ShouldEqual, 125)
			So(atomic.LoadUint64(&fails), ShouldEqual, 75)
		})

		Convey("Concurrent Increment()s at the limit work with wait times", func() {
			groups := []string{"l1", "l2"}
			So(l.Increment(ctx, groups), ShouldBeTrue)
			So(l.Increment(ctx, groups), ShouldBeTrue)
			So(l.Increment(ctx, groups), ShouldBeFalse)
			start := time.Now()

			go func() {
				l.Decrement(groups)
				l.Decrement(groups)
				<-time.After(50 * time.Millisecond)
				l.Decrement(groups)
			}()

			go func() {
				<-time.After(60 * time.Millisecond)
				// (decrementing the higher capacity group doesn't make an
				// increment of the lower capacity group work)
				l.Decrement([]string{"l1"})
			}()

			var quickIncs uint64
			var slowIncs uint64
			var fails uint64
			wait := 125 * time.Millisecond
			var wg sync.WaitGroup
			for i := 0; i < 4; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if l.Increment(ctx, groups, wait) {
						if time.Since(start) < 35*time.Millisecond {
							atomic.AddUint64(&quickIncs, 1)
						} else {
							atomic.AddUint64(&slowIncs, 1)
						}
					} else {
						if time.Since(start) > 100*time.Millisecond {
							atomic.AddUint64(&fails, 1)
						}
					}
				}()
			}
			wg.Wait()

			So(atomic.LoadUint64(&quickIncs), ShouldEqual, 2)
			So(atomic.LoadUint64(&slowIncs), ShouldEqual, 1)
			So(atomic.LoadUint64(&fails), ShouldEqual, 1)
		})
	})
}
