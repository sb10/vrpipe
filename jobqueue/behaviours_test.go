// Copyright © 2017-2019, 2021 Genome Research Limited
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

package jobqueue

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBehaviours(t *testing.T) {
	Convey("You can create individual Behaviour", t, func() {
		b1 := &Behaviour{When: OnExit, Do: CleanupAll}
		b2 := &Behaviour{When: OnSuccess, Do: CleanupAll}
		b3 := &Behaviour{When: OnFailure, Do: CleanupAll}
		b4 := &Behaviour{When: OnSuccess, Do: Run, Arg: "touch ../../foo && true"}
		b5 := &Behaviour{When: OnSuccess, Do: Run, Arg: "touch foo"}
		b6 := &Behaviour{When: OnSuccess, Do: Run, Arg: []string{"in", "valid"}}
		b7 := &Behaviour{When: OnSuccess, Do: CopyToManager, Arg: []string{"a.file", "b.file"}}
		b8 := &Behaviour{When: OnSuccess, Do: CopyToManager, Arg: "a.file"}
		b9 := &Behaviour{When: OnSuccess | OnFailure, Do: Cleanup}
		b10 := &Behaviour{When: 10, Do: Cleanup}
		b11 := &Behaviour{When: OnFailure, Do: Remove}

		cwd, err := os.MkdirTemp("", "wr_jobqueue_test_behaviour_dir_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(cwd)
		actualCwd := filepath.Join(cwd, "a", "b", "c", "def", "cwd")
		err = os.MkdirAll(actualCwd, os.ModePerm)
		So(err, ShouldBeNil)
		_, err = os.OpenFile(filepath.Join(actualCwd, "a.file"), os.O_RDONLY|os.O_CREATE, 0666)
		So(err, ShouldBeNil)
		_, err = os.OpenFile(filepath.Join(actualCwd, "b.file"), os.O_RDONLY|os.O_CREATE, 0666)
		So(err, ShouldBeNil)
		foo := filepath.Join(cwd, "a", "b", "c", "foo")
		adir := filepath.Join(cwd, "a")
		job1 := &Job{Cwd: cwd, ActualCwd: actualCwd}
		job2 := &Job{Cwd: cwd}

		Convey("Individual Behaviour can be nicely stringified", func() {
			So(fmt.Sprintf("test Sprintf %s", b1), ShouldEqual, `test Sprintf {"on_exit":[{"cleanup_all":true}]}`)
			So(b1.String(), ShouldEqual, `{"on_exit":[{"cleanup_all":true}]}`)
			So(b2.String(), ShouldEqual, `{"on_success":[{"cleanup_all":true}]}`)
			So(b3.String(), ShouldEqual, `{"on_failure":[{"cleanup_all":true}]}`)
			So(b4.String(), ShouldEqual, `{"on_success":[{"run":"touch ../../foo && true"}]}`)
			So(b5.String(), ShouldEqual, `{"on_success":[{"run":"touch foo"}]}`)
			So(b6.String(), ShouldEqual, `{"on_success":[{"run":"!invalid!"}]}`)
			So(b7.String(), ShouldEqual, `{"on_success":[{"copy_to_manager":["a.file","b.file"]}]}`)
			So(b8.String(), ShouldEqual, `{"on_success":[{"copy_to_manager":["!invalid!"]}]}`)
			So(b9.String(), ShouldEqual, `{"on_failure|success":[{"cleanup":true}]}`)
			So(b10.String(), ShouldEqual, "{}")
			So(b11.String(), ShouldEqual, `{"on_failure":[{"remove":true}]}`)

			Convey("Behaviours can be nicely stringified", func() {
				bs := Behaviours{b1, b4}
				So(bs.String(), ShouldEqual, `{"on_success":[{"run":"touch ../../foo && true"}],"on_exit":[{"cleanup_all":true}]}`)

				bs = Behaviours{}
				So(bs.String(), ShouldBeEmpty)
			})
		})

		Convey("Individual Behaviour Trigger() correctly", func() {
			err = b7.Trigger(OnSuccess, job1)
			So(err, ShouldBeNil)
			err = b8.Trigger(OnSuccess, job1)
			So(err, ShouldNotBeNil)
			//*** CopyToManager not yet implemented, so no proper tests for it yet

			err = b6.Trigger(OnSuccess, job1)
			So(err, ShouldNotBeNil)
			err = b4.Trigger(OnFailure, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldNotBeNil)
			err = b4.Trigger(OnSuccess, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldBeNil)
			os.Remove(foo)
			_, err = os.Stat(foo)
			So(err, ShouldNotBeNil)
			err = b4.Trigger(OnExit, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldNotBeNil)

			err = b5.Trigger(OnSuccess, job2)
			So(err, ShouldBeNil)
			foo2 := filepath.Join(cwd, "foo")
			_, err = os.Stat(foo2)
			So(err, ShouldBeNil)

			err = b1.Trigger(OnSuccess|OnFailure, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(actualCwd)
			So(err, ShouldBeNil)
			err = b1.Trigger(OnExit, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(actualCwd)
			So(err, ShouldNotBeNil)
			_, err = os.Stat(cwd)
			So(err, ShouldBeNil)
			_, err = os.Stat(adir)
			So(err, ShouldNotBeNil)

			err = b1.Trigger(OnExit, job2)
			So(err, ShouldBeNil)
			_, err = os.Stat(cwd)
			So(err, ShouldBeNil)

			err = b11.Trigger(OnExit, job2)
			So(err, ShouldBeNil)
		})

		Convey("RemovalRequested works", func() {
			bs := Behaviours{b1, b11}
			So(bs.RemovalRequested(), ShouldBeTrue)

			bs = Behaviours{b1, b2}
			So(bs.RemovalRequested(), ShouldBeFalse)
		})

		Convey("CleanupAll works when actual cwd contains root-owned files", func() {
			rootFile := filepath.Join(actualCwd, "root")
			err = exec.Command("sh", "-c", "sudo -n touch "+rootFile).Run()
			if err != nil {
				SkipConvey("Can't do this test without ability to sudo", func() {})
			} else {
				_, err = os.Stat(rootFile)
				So(err, ShouldBeNil)

				err = b1.Trigger(OnExit, job1)
				So(err, ShouldBeNil)
				_, err = os.Stat(actualCwd)
				So(err, ShouldNotBeNil)
				_, err = os.Stat(cwd)
				So(err, ShouldBeNil)
				_, err = os.Stat(adir)
				So(err, ShouldNotBeNil)
			}
		})

		Convey("Behaviours are triggered in order b2,b4, as specified", func() {
			bs := Behaviours{b2, b4}
			err = bs.Trigger(true, job1)
			So(err, ShouldNotBeNil)
			_, err = os.Stat(adir)
			So(err, ShouldNotBeNil)
		})

		Convey("Behaviours are triggered in order b4,b2, as specified", func() {
			bs := Behaviours{b4, b2}
			err = bs.Trigger(true, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(adir)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldBeNil)
			_, err = os.Stat(filepath.Join(cwd, "a", "b", "c", "def"))
			So(err, ShouldNotBeNil)
		})

		Convey("OnExit triggers after others", func() {
			bs := Behaviours{b1, b4}
			err = bs.Trigger(true, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldBeNil)
			_, err = os.Stat(filepath.Join(cwd, "a", "b", "c", "def"))
			So(err, ShouldNotBeNil)
		})

		Convey("Non-matching Behaviours are ignored during a Trigger()", func() {
			bs := Behaviours{b3, b4}
			err = bs.Trigger(true, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(foo)
			So(err, ShouldBeNil)
			_, err = os.Stat(actualCwd)
			So(err, ShouldBeNil)

			os.Remove(foo)
			_, err = os.Stat(foo)
			So(err, ShouldNotBeNil)

			bs = Behaviours{b4, b3}
			err = bs.Trigger(false, job1)
			So(err, ShouldBeNil)
			_, err = os.Stat(adir)
			So(err, ShouldNotBeNil)
		})
	})

	Convey("You can go from JSON to Behaviours", t, func() {
		jsonStr := `[{"run":"tar -czf my.tar.bz '--include=*.err'"},{"copy_to_manager":["my.tar.bz"]},{"cleanup_all":true},{"remove":true}]`
		var bjs BehavioursViaJSON
		err := json.Unmarshal([]byte(jsonStr), &bjs)
		So(err, ShouldBeNil)
		So(len(bjs), ShouldEqual, 4)

		bs := bjs.Behaviours(OnFailure)

		So(bs[0].When, ShouldEqual, OnFailure)
		So(bs[0].Do, ShouldEqual, Run)
		So(bs[0].Arg, ShouldEqual, "tar -czf my.tar.bz '--include=*.err'")

		So(bs[1].When, ShouldEqual, OnFailure)
		So(bs[1].Do, ShouldEqual, CopyToManager)
		So(bs[1].Arg, ShouldResemble, []string{"my.tar.bz"})

		So(bs[2].When, ShouldEqual, OnFailure)
		So(bs[2].Do, ShouldEqual, CleanupAll)

		So(bs[3].When, ShouldEqual, OnFailure)
		So(bs[3].Do, ShouldEqual, Remove)

		jsonStr = `[{"cleanup":true}]`
		var bjs2 BehavioursViaJSON
		err = json.Unmarshal([]byte(jsonStr), &bjs2)
		So(err, ShouldBeNil)
		So(len(bjs2), ShouldEqual, 1)

		bs = append(bs, bjs2.Behaviours(OnSuccess)...)

		So(bs[4].When, ShouldEqual, OnSuccess)
		So(bs[4].Do, ShouldEqual, Cleanup)

		jsonStr = `[{"run":"true"}]`
		var bjs3 BehavioursViaJSON
		err = json.Unmarshal([]byte(jsonStr), &bjs3)
		So(err, ShouldBeNil)
		So(len(bjs3), ShouldEqual, 1)

		bs = append(bs, bjs3.Behaviours(OnExit)...)

		So(bs[5].When, ShouldEqual, OnExit)
		So(bs[5].Do, ShouldEqual, Run)
		So(bs[5].Arg, ShouldEqual, "true")

		Convey("You can convert back to JSON", func() {
			So(bs.String(), ShouldEqual, `{"on_failure":[{"run":"tar -czf my.tar.bz '--include=*.err'"},{"copy_to_manager":["my.tar.bz"]},{"cleanup_all":true},{"remove":true}],"on_success":[{"cleanup":true}],"on_exit":[{"run":"true"}]}`)
		})
	})
}
