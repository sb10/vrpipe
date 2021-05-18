// Copyright © 2016-2019, 2021 Genome Research Limited
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

package scheduler

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/internal"
	sync "github.com/sasha-s/go-deadlock"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
)

var maxCPU = runtime.NumCPU()
var otherReqs = make(map[string]string)

var testLogger = log15.New()

func init() {
	testLogger.SetHandler(log15.LvlFilterHandler(log15.LvlWarn, log15.StderrHandler))
}

func TestLocal(t *testing.T) {
	ctx := context.Background()
	runtime.GOMAXPROCS(maxCPU)

	var overhead time.Duration
	Convey("You can get a new local scheduler", t, func() {
		s, err := New(ctx, "local", &ConfigLocal{"bash", 1 * time.Second, 0, 0})
		So(err, ShouldBeNil)
		So(s, ShouldNotBeNil)

		possibleReq := &Requirements{1, 1 * time.Second, 1, 20, otherReqs, true, true, true}
		impossibleReq := &Requirements{9999999999, 999999 * time.Hour, 99999, 20, otherReqs, true, true, true}

		Convey("ReserveTimeout() returns 1 second", func() {
			So(s.ReserveTimeout(ctx, possibleReq), ShouldEqual, 1)
		})

		Convey("MaxQueueTime() returns req time plus 1m", func() {
			So(s.MaxQueueTime(possibleReq).Seconds(), ShouldEqual, 61)
		})

		Convey("Busy() starts off false", func() {
			So(s.Busy(ctx), ShouldBeFalse)
		})

		Convey("Requirements.Stringify() works", func() {
			So(possibleReq.Stringify(), ShouldEqual, "1:0:1:20")
			testReq := &Requirements{RAM: 300, Time: 2 * time.Hour, Cores: 2}
			So(testReq.Stringify(), ShouldEqual, "300:120:2:0")
			other := make(map[string]string)
			other["foo"] = "bar"
			other["goo"] = "lar"
			testReq.Other = other
			So(testReq.Stringify(), ShouldEqual, "300:120:2:0:f88250fdf9c81d47c18d63354b85f26e")
		})

		Convey("Schedule() gives impossible error when given impossible reqs", func() {
			err := s.Schedule(ctx, "foo", impossibleReq, 0, 1)
			So(err, ShouldNotBeNil)
			serr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(serr.Err, ShouldEqual, ErrImpossible)
		})

		Convey("Given a running command", func() {
			testProcessNotRunning(ctx, s, possibleReq)
		})

		Convey("Schedule() lets you schedule more jobs than localhost CPUs", func() {
			tmpdir, err := os.MkdirTemp("", "wr_schedulers_local_test_immediate_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)
			tmpdir2, err := os.MkdirTemp("", "wr_schedulers_local_test_end_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir2)

			defer waitToFinish(ctx, s, 30, 100)

			cmd := fmt.Sprintf("perl -MFile::Temp=tempfile -e '@a = tempfile(DIR => q[%s]); select(undef, undef, undef, 0.75); @a = tempfile(DIR => q[%s]); exit(0);'", tmpdir, tmpdir2) // creates a file, sleeps for 0.75s and then creates another file

			// different machines take different amounts of times to actually
			// run the above command, so we first need to run the command (in
			// parallel still, since it is slower to run when many are running
			// at once) to find how long it takes, as subsequent tests are very
			// timing dependent
			if overhead == 0 {
				Convey("You can first run with the number of CPUs", func() {
					err = s.Schedule(ctx, cmd, possibleReq, 0, maxCPU)
					So(err, ShouldBeNil)
					before := time.Now()
					for {
						if !s.Busy(ctx) {
							overhead = time.Since(before) - (750 * time.Millisecond) // about 150ms
							break
						}
						<-time.After(1 * time.Millisecond)
					}
				})
			}

			count := maxCPU * 2
			sched := func() {
				serr := s.Schedule(ctx, cmd, possibleReq, 0, count)
				So(serr, ShouldBeNil)
				So(s.Busy(ctx), ShouldBeTrue)
				scheduled, serr := s.Scheduled(ctx, cmd)
				So(serr, ShouldBeNil)
				So(scheduled, ShouldEqual, count)
			}

			Convey("It eventually runs them all", func() {
				sched()
				<-time.After(700 * time.Millisecond)

				numfiles := testDirForFiles(tmpdir, maxCPU)
				So(numfiles, ShouldEqual, maxCPU)

				<-time.After(750*time.Millisecond + overhead)

				numfiles = testDirForFiles(tmpdir, count)
				So(numfiles, ShouldEqual, count)
				numfiles = testDirForFiles(tmpdir2, count)
				if numfiles < count {
					So(s.Busy(ctx), ShouldBeTrue) // but they might not all have finished quite yet
				}

				<-time.After(200*time.Millisecond + overhead) // an extra 150ms for leeway
				numfiles = testDirForFiles(tmpdir2, count)
				So(numfiles, ShouldEqual, count)
				So(s.Busy(ctx), ShouldBeFalse)
			})

			Convey("Dropping the count below the number currently running doesn't kill those that are running", func() {
				sched()
				<-time.After(700 * time.Millisecond)

				numfiles := testDirForFiles(tmpdir, maxCPU)
				So(numfiles, ShouldEqual, maxCPU)

				newcount := maxCPU - 1
				err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
				So(err, ShouldBeNil)

				<-time.After(750*time.Millisecond + overhead)

				numfiles = testDirForFiles(tmpdir, maxCPU)
				So(numfiles, ShouldEqual, maxCPU)

				So(waitToFinish(ctx, s, 3, 100), ShouldBeTrue)

				numfiles = testDirForFiles(tmpdir2, maxCPU)
				So(numfiles, ShouldEqual, maxCPU)
			})

			Convey("You can Schedule() again to increase the count", func() {
				sched()
				<-time.After(700 * time.Millisecond)

				numfiles := testDirForFiles(tmpdir, maxCPU)
				So(numfiles, ShouldEqual, maxCPU)

				newcount := count + 1
				err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
				So(err, ShouldBeNil)

				<-time.After(1500*time.Millisecond + overhead + overhead)

				numfiles = testDirForFiles(tmpdir, newcount)
				So(numfiles, ShouldEqual, newcount)

				So(waitToFinish(ctx, s, 3, 100), ShouldBeTrue)

				numfiles = testDirForFiles(tmpdir2, newcount)
				So(numfiles, ShouldEqual, newcount)
			})

			if maxCPU > 1 {
				Convey("You can Schedule() again to drop the count", func() {
					sched()
					<-time.After(700 * time.Millisecond)

					numfiles := testDirForFiles(tmpdir, maxCPU)
					So(numfiles, ShouldEqual, maxCPU)

					newcount := maxCPU + 1 // (this test only really makes sense if newcount is now less than count, ie. we have more than 1 cpu)
					err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
					So(err, ShouldBeNil)

					<-time.After(750*time.Millisecond + overhead)

					numfiles = testDirForFiles(tmpdir, newcount)
					So(numfiles, ShouldEqual, newcount)

					So(waitToFinish(ctx, s, 3, 100), ShouldBeTrue)

					numfiles = testDirForFiles(tmpdir2, newcount)
					So(numfiles, ShouldEqual, newcount)
				})

				Convey("You can Schedule() a new job and have it run while the first is still running", func() {
					sched()
					<-time.After(700 * time.Millisecond)

					numfiles := testDirForFiles(tmpdir, maxCPU)
					So(numfiles, ShouldEqual, maxCPU)

					newcount := maxCPU + 1
					err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
					So(err, ShouldBeNil)
					newcmd := fmt.Sprintf("perl -MFile::Temp=tempfile -e '@b = tempfile(DIR => q[%s]); select(undef, undef, undef, 0.75);'", tmpdir)
					err = s.Schedule(ctx, newcmd, possibleReq, 0, 1)
					So(err, ShouldBeNil)

					<-time.After(750*time.Millisecond + overhead)

					numfiles = testDirForFiles(tmpdir, newcount+1)
					So(numfiles, ShouldEqual, newcount+1)

					So(waitToFinish(ctx, s, 3, 100), ShouldBeTrue)

					numfiles = testDirForFiles(tmpdir2, newcount)
					So(numfiles, ShouldEqual, newcount)
				})

				//*** want a test where the first job fills up all resources
				// and has more to do, and a second job could slip and complete
				// before resources for the first become available
			} else {
				SkipConvey("Skipping Schedule() tests that need more than 1 cpu", func() {})
			}
		})

		if maxCPU > 2 {
			Convey("Schedule() does bin packing and fills up the machine with different size cmds", func() {
				smallTmpdir, err := os.MkdirTemp("", "wr_schedulers_local_test_small_output_dir_")
				if err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(smallTmpdir)
				bigTmpdir, err := os.MkdirTemp("", "wr_schedulers_local_test_big_output_dir_")
				if err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(bigTmpdir)

				blockCmd := "sleep 0.25"
				blockReq := &Requirements{1, 1 * time.Second, float64(maxCPU), 0, otherReqs, true, true, true}
				smallCmd := fmt.Sprintf("mktemp --tmpdir=%s tmp.XXXXXX && sleep 0.75", smallTmpdir)
				smallReq := &Requirements{1, 1 * time.Second, 1, 0, otherReqs, true, true, true}
				bigCmd := fmt.Sprintf("mktemp --tmpdir=%s tmp.XXXXXX && sleep 0.75", bigTmpdir)
				bigReq := &Requirements{1, 1 * time.Second, float64(maxCPU - 1), 0, otherReqs, true, true, true}

				// schedule 2 big cmds and then a small one to prove the small
				// one fits the gap and runs before the second big one
				err = s.Schedule(ctx, bigCmd, bigReq, 0, 2)
				So(err, ShouldBeNil)
				err = s.Schedule(ctx, smallCmd, smallReq, 0, 1)
				So(err, ShouldBeNil)

				for {
					if !s.Busy(ctx) {
						break
					}
					<-time.After(1 * time.Millisecond)
				}

				bigTimes := mtimesOfFilesInDir(bigTmpdir, 2)
				So(len(bigTimes), ShouldEqual, 2)
				smallTimes := mtimesOfFilesInDir(smallTmpdir, 1)
				So(len(smallTimes), ShouldEqual, 1)
				firstBig := bigTimes[0]
				secondBig := bigTimes[1]
				if secondBig.Before(firstBig) {
					firstBig = bigTimes[1]
					secondBig = bigTimes[0]
				}
				So(smallTimes[0], ShouldHappenOnOrAfter, firstBig)
				So(smallTimes[0], ShouldHappenBefore, secondBig)

				// schedule a blocker so that subsequent schedules will be
				// compared to each other, then schedule 2 small cmds and a big
				// command that uses all cpus to prove that the biggest one
				// takes priority
				err = s.Schedule(ctx, blockCmd, blockReq, 0, 1)
				So(err, ShouldBeNil)
				err = s.Schedule(ctx, smallCmd, smallReq, 0, 2)
				So(err, ShouldBeNil)
				err = s.Schedule(ctx, bigCmd, blockReq, 0, 1)
				So(err, ShouldBeNil)

				for {
					if !s.Busy(ctx) {
						break
					}
					<-time.After(1 * time.Millisecond)
				}

				bigTimes = mtimesOfFilesInDir(bigTmpdir, 1)
				So(len(bigTimes), ShouldEqual, 1)
				smallTimes = mtimesOfFilesInDir(smallTmpdir, 2)
				So(len(smallTimes), ShouldEqual, 2)
				So(bigTimes[0], ShouldHappenOnOrBefore, smallTimes[0])
				So(bigTimes[0], ShouldHappenOnOrBefore, smallTimes[1])
				// *** one of the above 2 tests can fail; the jobs start in the
				// correct order, which is what we're trying to test for, but
				// finish in the wrong order. That is, the big job takes a few
				// extra ms before it does anything. Not sure how to test for
				// actual job start time order...
			})

			Convey("Priority overrides bin-packing for smaller cmds", func() {
				smallTmpdir, err := os.MkdirTemp("", "wr_schedulers_local_test_small_output_dir_")
				if err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(smallTmpdir)
				bigTmpdir, err := os.MkdirTemp("", "wr_schedulers_local_test_big_output_dir_")
				if err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(bigTmpdir)

				smallCmd := fmt.Sprintf("mktemp --tmpdir=%s tmp.XXXXXX && sleep 0.75", smallTmpdir)
				smallReq := &Requirements{1, 1 * time.Second, 1, 0, otherReqs, true, true, true}
				bigCmd := fmt.Sprintf("mktemp --tmpdir=%s tmp.XXXXXX && sleep 0.75", bigTmpdir)
				bigReq := &Requirements{1, 1 * time.Second, float64(maxCPU / 2), 0, otherReqs, true, true, true}

				// schedule 3 big cmds (where 2 can run at once, filling the
				// whole machine) and then a small one to prove the small
				// one with higher priority runs before the 3rd big one.
				err = s.Schedule(ctx, bigCmd, bigReq, 0, 3)
				So(err, ShouldBeNil)
				err = s.Schedule(ctx, smallCmd, smallReq, 1, 1)
				So(err, ShouldBeNil)

				for {
					if !s.Busy(ctx) {
						break
					}
					<-time.After(1 * time.Millisecond)
				}

				bigTimes := mtimesOfFilesInDir(bigTmpdir, 2)
				So(len(bigTimes), ShouldEqual, 3)
				smallTimes := mtimesOfFilesInDir(smallTmpdir, 1)
				So(len(smallTimes), ShouldEqual, 1)
				sort.Slice(bigTimes, func(i, j int) bool {
					return bigTimes[i].Before(bigTimes[j])
				})
				So(smallTimes[0], ShouldHappenAfter, bigTimes[0])
				So(smallTimes[0], ShouldHappenOnOrAfter, bigTimes[1])
				So(smallTimes[0], ShouldHappenOnOrBefore, bigTimes[2])
			})
		}

		// wait a while for any remaining jobs to finish
		So(waitToFinish(ctx, s, 30, 100), ShouldBeTrue)
	})

	if maxCPU > 1 {
		Convey("You can get a new local scheduler that uses less than all CPUs", t, func() {
			s, err := New(ctx, "local", &ConfigLocal{"bash", 1 * time.Second, 1, 0})
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)

			tmpDir, err := os.MkdirTemp("", "wr_schedulers_local_test_slee[_output_dir_")
			if err != nil {
				log.Fatal(err)
			}

			cmd := fmt.Sprintf("mktemp --tmpdir=%s tmp.XXXXXX && sleep 0.5", tmpDir)
			sleepReq := &Requirements{1, 1 * time.Second, 1, 0, otherReqs, true, true, true}

			err = s.Schedule(ctx, cmd, sleepReq, 0, 2)
			So(err, ShouldBeNil)

			for {
				if !s.Busy(ctx) {
					break
				}
				<-time.After(1 * time.Millisecond)
			}

			times := mtimesOfFilesInDir(tmpDir, 2)
			So(len(times), ShouldEqual, 2)
			first := times[0]
			second := times[1]
			if second.Before(first) {
				first = times[1]
				second = times[0]
			}
			So(first, ShouldHappenBefore, second)
			So(first, ShouldHappenBefore, second.Add(-400*time.Millisecond))
		})
	}
}

func TestLSF(t *testing.T) {
	ctx := context.Background()
	// check if LSF seems to be installed
	_, err := exec.LookPath("lsadmin")
	if err == nil {
		_, err = exec.LookPath("bqueues")
	}
	if err != nil {
		SkipConvey("You can't get a new lsf scheduler without LSF being installed", t, func() {
			_, err = New(ctx, "lsf", &ConfigLSF{"development", "bash", "~/.ssh/id_rsa"})
			So(err, ShouldNotBeNil)
		})
		return
	}

	if os.Getenv("WR_LSF_TEST_KEY") == "" {
		SkipConvey("LSF tests disabled since WR_LSF_TEST_KEY is not set", t, func() {})
		return
	}

	var specifiedOther = make(map[string]string)
	specifiedOther["scheduler_queue"] = "yesterday"
	specifiedOther["scheduler_misc"] = "-R avx"
	possibleReq := &Requirements{100, 1 * time.Minute, 1, 20, otherReqs, true, true, true}
	specifiedReq := &Requirements{100, 1 * time.Minute, 1, 20, specifiedOther, true, true, true}
	impossibleReq := &Requirements{9999999999, 999999 * time.Hour, 99999, 20, otherReqs, true, true, true}

	host, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	if host == "farm5-head1" {
		// author needs to disable access to his own queues to test normal
		// behaviour
		username := internal.CachedUsername
		internal.CachedUsername = "invalid"
		defer func() {
			internal.CachedUsername = username
		}()
	}

	Convey("You can get a new lsf scheduler", t, func() {
		s, err := New(ctx, "lsf", &ConfigLSF{"development", "bash", os.Getenv("WR_LSF_TEST_KEY")})
		So(err, ShouldBeNil)
		So(s, ShouldNotBeNil)

		Convey("ReserveTimeout() returns 25 seconds", func() {
			So(s.ReserveTimeout(ctx, possibleReq), ShouldEqual, 1)
		})

		// author specific tests, based on hostname, where we know what the
		// expected queue names are *** could also break out initialize() to
		// mock some textual input instead of taking it from lsadmin...
		if host == "farm5-head1" {
			Convey("determineQueue() picks the best queue depending on given resource requirements", func() {
				queue, err := s.impl.(*lsf).determineQueue(possibleReq, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "normal")

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{1, 5 * time.Minute, 1, 20, otherReqs, true, true, true}, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "normal")

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{1, 5 * time.Minute, 1, 20, otherReqs, true, true, true}, 10)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "normal") // used to be yesterday, but something changed? Or is this a bug?

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{37000, 1 * time.Hour, 1, 20, otherReqs, true, true, true}, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "normal")

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{1000000, 1 * time.Hour, 1, 20, otherReqs, true, true, true}, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "teramem")

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{1, 13 * time.Hour, 1, 20, otherReqs, true, true, true}, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "long")

				queue, err = s.impl.(*lsf).determineQueue(&Requirements{1, 49 * time.Hour, 1, 20, otherReqs, true, true, true}, 0)
				So(err, ShouldBeNil)
				So(queue, ShouldEqual, "basement")
			})

			Convey("MaxQueueTime() returns appropriate times depending on the requirements", func() {
				So(s.MaxQueueTime(possibleReq).Minutes(), ShouldEqual, 720)
				So(s.MaxQueueTime(&Requirements{1, 49 * time.Hour, 1, 20, otherReqs, true, true, true}).Minutes(), ShouldEqual, 43200)
			})
		}

		Convey("determineQueue() returns user queue if specified", func() {
			queue, err := s.impl.(*lsf).determineQueue(specifiedReq, 0)
			So(err, ShouldBeNil)
			So(queue, ShouldEqual, "yesterday")
		})

		Convey("generateBsubArgs() adds in user-specified options", func() {
			bsubArgs := s.impl.(*lsf).generateBsubArgs(ctx, "yesterday", specifiedReq, "mycmd", 2)
			So(strings.HasSuffix(bsubArgs[9], "[1-2]"), ShouldBeTrue)
			bsubArgs[9] = "random1"
			So(bsubArgs, ShouldResemble, []string{"-q", "yesterday", "-M", "100", "-R", "'select[mem>100] rusage[mem=100] span[hosts=1]'", "-R", "avx", "-J", "random1", "-o", "/dev/null", "-e", "/dev/null", "mycmd"})

			specifiedOther["scheduler_misc"] = `-R "avx foo"`
			bsubArgs = s.impl.(*lsf).generateBsubArgs(ctx, "yesterday", specifiedReq, "mycmd", 2)
			bsubArgs[9] = "random2"
			So(bsubArgs, ShouldResemble, []string{"-q", "yesterday", "-M", "100", "-R", "'select[mem>100] rusage[mem=100] span[hosts=1]'", "-R", "'avx foo'", "-J", "random2", "-o", "/dev/null", "-e", "/dev/null", "mycmd"})

			specifiedOther["scheduler_misc"] = `-E "also supported"`
			bsubArgs = s.impl.(*lsf).generateBsubArgs(ctx, "yesterday", specifiedReq, "mycmd", 2)
			bsubArgs[9] = "random3"
			So(bsubArgs, ShouldResemble, []string{"-q", "yesterday", "-M", "100", "-R", "'select[mem>100] rusage[mem=100] span[hosts=1]'", "-E", "'also supported'", "-J", "random3", "-o", "/dev/null", "-e", "/dev/null", "mycmd"})

			specifiedOther["scheduler_misc"] = `-E "'not supported'"`
			bsubArgs = s.impl.(*lsf).generateBsubArgs(ctx, "yesterday", specifiedReq, "mycmd", 2)
			bsubArgs[7] = "random3"
			So(bsubArgs, ShouldResemble, []string{"-q", "yesterday", "-M", "100", "-R", "'select[mem>100] rusage[mem=100] span[hosts=1]'", "-J", "random3", "-o", "/dev/null", "-e", "/dev/null", "mycmd"})
		})

		Convey("Busy() starts off false", func() {
			So(s.Busy(ctx), ShouldBeFalse)
		})

		Convey("Schedule() gives impossible error when given impossible reqs", func() {
			err := s.Schedule(ctx, "foo", impossibleReq, 0, 1)
			So(err, ShouldNotBeNil)
			serr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(serr.Err, ShouldEqual, ErrImpossible)
		})

		Convey("Given a cmd running on a host", func() {
			testProcessNotRunning(ctx, s, possibleReq)
		})

		// following tests are unreliable due to needing LSF nodes to be all
		// working well and for there to be capacity to run jobs
		if os.Getenv("WR_DISABLE_UNRELIABLE_LSF_TESTS") == "true" {
			SkipConvey("Further LSF tests disabled since WR_DISABLE_UNRELIABLE_LSF_TESTS is set", func() {})

			return
		}

		Convey("Schedule() lets you schedule more jobs than localhost CPUs", func() {
			// tmpdir, err := os.MkdirTemp("", "wr_schedulers_lsf_test_output_dir_")
			// if err != nil {
			// 	log.Fatal(err)
			// }
			// defer os.RemoveAll(tmpdir)

			// cmd := fmt.Sprintf("ssh %s 'perl -MFile::Temp=tempfile -e '\"'\"'$sleep = rand(60); select(undef, undef, undef, $sleep); @a = tempfile(DIR => q[%s]); select(undef, undef, undef, 5 - $sleep); exit(0);'\"'\"", host, tmpdir) // sleep for a random amount of time so that ssh does not fail due to too many run at once, then ssh back to us and create a file in our tmp dir

			// the above wouldn't work due to some issue with all the ssh's not
			// working and some high proportion of the LSF jobs immediately
			// failing; instead we assume, since this is LSF, that our current
			// directory is on a shared disk, and just have all the jobs write
			// their files here directly
			tmpdir, err := os.MkdirTemp("./", "wr_schedulers_lsf_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			cmd := fmt.Sprintf("perl -MFile::Temp=tempfile -e '@a = tempfile(DIR => q[%s]); sleep(5); exit(0);'", tmpdir)

			count := maxCPU * 2
			err = s.Schedule(ctx, cmd, possibleReq, 0, count)
			So(err, ShouldBeNil)
			So(s.Busy(ctx), ShouldBeTrue)

			Convey("It eventually runs them all", func() {
				So(waitToFinish(ctx, s, 300, 1000), ShouldBeTrue)
				numfiles := testDirForFiles(tmpdir, count)
				So(numfiles, ShouldEqual, count)
			})

			// *** no idea how to reliably test dropping the count, since I
			// don't have any way of ensuring some jobs are still pending by the
			// time I try and drop the count... unless I did something like
			// have a count of 1000000?...

			Convey("You can Schedule() again to increase the count", func() {
				newcount := count + 5
				err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
				So(err, ShouldBeNil)
				So(waitToFinish(ctx, s, 300, 1000), ShouldBeTrue)
				numfiles := testDirForFiles(tmpdir, newcount)
				So(numfiles, ShouldEqual, newcount)
			})

			Convey("You can Schedule() a new job and have it run while the first is still running", func() {
				<-time.After(6 * time.Second) // *** if the following test fails, it probably just because LSF didn't get any previous jobs running yet; not sure what to do about that
				numfiles := testDirForFiles(tmpdir, 1)
				So(numfiles, ShouldBeBetweenOrEqual, 1, count)
				So(s.Busy(ctx), ShouldBeTrue)

				newcmd := fmt.Sprintf("perl -MFile::Temp=tempfile -e '@a = tempfile(DIR => q[%s]); sleep(1); exit(0);'", tmpdir)
				err = s.Schedule(ctx, newcmd, possibleReq, 0, 1)
				So(err, ShouldBeNil)

				So(waitToFinish(ctx, s, 300, 1000), ShouldBeTrue)
				numfiles = testDirForFiles(tmpdir, count+1)
				So(numfiles, ShouldEqual, count+1)
			})
		})

		Convey("Schedule() lets you schedule more jobs than could reasonably start all at once", func() {
			tmpdir, err := os.MkdirTemp("./", "wr_schedulers_lsf_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			cmd := fmt.Sprintf("perl -MFile::Temp=tempfile -e '@a = tempfile(DIR => q[%s]); sleep(2); exit(0);'", tmpdir)

			count := 10000 // 1,000,000 just errors out, and 100,000 could be bad for LSF in some way
			err = s.Schedule(ctx, cmd, possibleReq, 0, count)
			So(err, ShouldBeNil)
			So(s.Busy(ctx), ShouldBeTrue)

			Convey("It runs some of them and you can Schedule() again to drop the count", func() {
				So(waitToFinish(ctx, s, 6, 1000), ShouldBeFalse)
				numfiles := testDirForFiles(tmpdir, 1)
				So(numfiles, ShouldBeBetween, 1, count-(maxCPU*2)-2)

				newcount := numfiles + maxCPU
				err = s.Schedule(ctx, cmd, possibleReq, 0, newcount)
				So(err, ShouldBeNil)

				So(waitToFinish(ctx, s, 300, 1000), ShouldBeTrue)
				numfiles = testDirForFiles(tmpdir, newcount)
				So(numfiles, ShouldBeBetweenOrEqual, newcount, numfiles*2) // we must allow it to run a few extra due to the implementation
			})
		})

		// wait a while for any remaining jobs to finish
		So(waitToFinish(ctx, s, 300, 1000), ShouldBeTrue)
	})
}

func TestOpenstack(t *testing.T) {
	ctx := context.Background()
	// check if we have our special openstack-related variable
	osPrefix := os.Getenv("OS_OS_PREFIX")
	osUser := os.Getenv("OS_OS_USERNAME")
	localUser := os.Getenv("OS_LOCAL_USERNAME")
	flavorRegex := os.Getenv("OS_FLAVOR_REGEX")
	rName := "wr-testing-" + localUser
	config := &ConfigOpenStack{
		ResourceName:         rName,
		OSPrefix:             osPrefix,
		OSUser:               osUser,
		OSRAM:                2048,
		FlavorRegex:          flavorRegex,
		FlavorSets:           os.Getenv("OS_FLAVOR_SETS"),
		ServerPorts:          []int{22},
		ServerKeepTime:       15 * time.Second,
		StateUpdateFrequency: 1 * time.Second,
		Shell:                "bash",
		MaxInstances:         -1,
		SimultaneousSpawns:   1,
	}
	if osPrefix == "" || osUser == "" || localUser == "" {
		Convey("You can't get a new openstack scheduler without the required environment variables", t, func() {
			_, err := New(ctx, "openstack", config)
			So(err, ShouldNotBeNil)
		})
		return
	}
	if flavorRegex == "" {
		SkipConvey("OpenStack scheduler tests are skipped without special OS_FLAVOR_REGEX environment variable being set", t, func() {})
		return
	}
	host, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	var novaCmd string
	if _, errl := exec.LookPath("openstack"); errl == nil {
		novaCmd = "openstack server"
	} else if _, errl := exec.LookPath("nova"); errl == nil {
		novaCmd = "nova"
	}

	Convey("You can get a new openstack scheduler", t, func() {
		tmpdir, errt := os.MkdirTemp("", "wr_schedulers_openstack_test_output_dir_")
		if errt != nil {
			log.Fatal(errt)
		}
		defer os.RemoveAll(tmpdir)
		config.SavePath = filepath.Join(tmpdir, "os_resources")
		s, errn := New(ctx, "openstack", config)
		So(errn, ShouldBeNil)
		So(s, ShouldNotBeNil)
		defer s.Cleanup(ctx)
		oss := s.impl.(*opst)

		possibleReq := &Requirements{100, 1 * time.Minute, 1, 1, otherReqs, true, true, true}
		impossibleReq := &Requirements{9999999999, 999999 * time.Hour, 99999, 20, otherReqs, true, true, true}
		Convey("ReserveTimeout() returns 25 seconds", func() {
			So(s.ReserveTimeout(ctx, possibleReq), ShouldEqual, 1)
		})

		// author specific tests, based on hostname, where we know what the
		// expected server types are
		if host == "vr-2-2-02" {
			Convey("determineFlavor() picks the best server flavor depending on given resource requirements", func() {
				flavor, err := oss.determineFlavor(ctx, possibleReq, "a")
				So(err, ShouldBeNil)

				if os.Getenv("OS_TENANT_ID") != "" {
					// author's pre-pike install
					So(flavor.ID, ShouldEqual, "2000")
					So(flavor.RAM, ShouldEqual, 1024)
					So(flavor.Disk, ShouldEqual, 8)
					So(flavor.Cores, ShouldEqual, 1)

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 1, 20, otherReqs, true, true, true}, "b")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2000") // we now ignore the 20GB disk requirement

					flavor, err = oss.determineFlavor(ctx, oss.reqForSpawn(possibleReq), "c")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2001")
					So(flavor.RAM, ShouldEqual, 4096)

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 2, 1, otherReqs, true, true, true}, "d")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2001")
					So(flavor.RAM, ShouldEqual, 4096)
					So(flavor.Disk, ShouldEqual, 12)
					So(flavor.Cores, ShouldEqual, 2)

					flavor, err = oss.determineFlavor(ctx, &Requirements{5000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "e")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2002")
					So(flavor.RAM, ShouldEqual, 16384)
					So(flavor.Disk, ShouldEqual, 20)
					So(flavor.Cores, ShouldEqual, 4)

					flavor, err = oss.determineFlavor(ctx, &Requirements{64000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "f")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2003")
					So(flavor.RAM, ShouldEqual, 65536)
					So(flavor.Disk, ShouldEqual, 20)
					So(flavor.Cores, ShouldEqual, 8)

					flavor, err = oss.determineFlavor(ctx, &Requirements{66000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "g")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2004")
					So(flavor.RAM, ShouldEqual, 122880)
					So(flavor.Disk, ShouldEqual, 128)
					So(flavor.Cores, ShouldEqual, 16)

					flavor, err = oss.determineFlavor(ctx, &Requirements{261000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "h")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2005")
					So(flavor.RAM, ShouldEqual, 262144)
					So(flavor.Disk, ShouldEqual, 128)
					So(flavor.Cores, ShouldEqual, 52)

					flavor, err = oss.determineFlavor(ctx, &Requirements{263000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "i")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2006")
					So(flavor.RAM, ShouldEqual, 496640)
					So(flavor.Disk, ShouldEqual, 128)
					So(flavor.Cores, ShouldEqual, 56)

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 3, 1, otherReqs, true, true, true}, "j")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2002")

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 5, 1, otherReqs, true, true, true}, "k")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2003")
				} else {
					// author's pike install
					So(flavor.ID, ShouldEqual, "2000")
					So(flavor.RAM, ShouldEqual, 8600)
					So(flavor.Disk, ShouldEqual, 15)
					So(flavor.Cores, ShouldEqual, 1)

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 1, 20, otherReqs, true, true, true}, "l")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2000")

					flavor, err = oss.determineFlavor(ctx, oss.reqForSpawn(possibleReq), "m")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2000")

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 2, 1, otherReqs, true, true, true}, "n")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2001")
					So(flavor.RAM, ShouldEqual, 17200)
					So(flavor.Disk, ShouldEqual, 31)
					So(flavor.Cores, ShouldEqual, 2)

					flavor, err = oss.determineFlavor(ctx, &Requirements{30000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "o")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2002")
					So(flavor.RAM, ShouldEqual, 34400)
					So(flavor.Disk, ShouldEqual, 62)
					So(flavor.Cores, ShouldEqual, 4)

					flavor, err = oss.determineFlavor(ctx, &Requirements{64000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "p")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2003")
					So(flavor.RAM, ShouldEqual, 68800)
					So(flavor.Disk, ShouldEqual, 125)
					So(flavor.Cores, ShouldEqual, 8)

					flavor, err = oss.determineFlavor(ctx, &Requirements{120000, 1 * time.Minute, 1, 1, otherReqs, true, true, true}, "q")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2004")
					So(flavor.RAM, ShouldEqual, 137600)
					So(flavor.Disk, ShouldEqual, 250)
					So(flavor.Cores, ShouldEqual, 16)

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 3, 1, otherReqs, true, true, true}, "r")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2002")

					flavor, err = oss.determineFlavor(ctx, &Requirements{100, 1 * time.Minute, 5, 1, otherReqs, true, true, true}, "s")
					So(err, ShouldBeNil)
					So(flavor.ID, ShouldEqual, "2003")
				}
			})

			Convey("MaxQueueTime() always returns enough time to complete 1 job, plus a minute leeway", func() {
				So(s.MaxQueueTime(possibleReq).Minutes(), ShouldEqual, 2)
				So(s.MaxQueueTime(&Requirements{1, 13 * time.Hour, 1, 20, otherReqs, true, true, true}).Minutes(), ShouldEqual, 781)
			})
		}

		Convey("Busy() starts off false", func() {
			So(s.Busy(ctx), ShouldBeFalse)
		})

		Convey("Schedule() gives impossible error when given impossible reqs", func() {
			err := s.Schedule(ctx, "foo", impossibleReq, 0, 1)
			So(err, ShouldNotBeNil)
			serr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(serr.Err, ShouldEqual, ErrImpossible)
		})

		if os.Getenv("OS_TENANT_ID") == "" {
			Convey("Schedule() gives impossible error when reqs don't fit in the requested flavor", func() {
				flavor, err := oss.determineFlavor(ctx, possibleReq, "a")
				So(err, ShouldBeNil)
				other := make(map[string]string)
				other["cloud_flavor"] = flavor.Name
				brokenReq := &Requirements{flavor.RAM + 1, 1 * time.Minute, 1, 1, other, true, true, true}
				err = s.Schedule(ctx, "foo", brokenReq, 0, 1)
				So(err, ShouldNotBeNil)
				serr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(serr.Err, ShouldEqual, ErrImpossible)
			})
		}

		// we need to not actually run the real scheduling tests if we're not
		// running in openstack, because the scheduler will try to ssh to
		// the servers it spawns
		if novaCmd != "" && oss.provider.InCloud() {
			oFile := filepath.Join(tmpdir, "out")

			Convey("Schedule() lets you...", func() {
				Convey("Run lots of jobs on a deathrow server", func() {
					count := 10
					eta := 200
					oReqs := make(map[string]string)
					oReqs["cloud_script"] = "touch /tmp/foo" // force a server to be spawned
					thisReq := &Requirements{1, 1 * time.Minute, 0, 0, oReqs, true, true, true}
					err := s.Schedule(ctx, "echo first", thisReq, 0, 1)
					So(err, ShouldBeNil)
					So(s.Busy(ctx), ShouldBeTrue)

					// spawn a server, run the first job, get on deathrow
					So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)

					// now Schedule a bunch of cmds in quick succession
					var wg sync.WaitGroup
					for i := 0; i < count; i++ {
						wg.Add(1)
						go func(i int) {
							defer wg.Done()
							s.Schedule(ctx, fmt.Sprintf("echo %d", i), thisReq, 0, count)
						}(i)
					}
					wg.Wait()

					// the test is that we don't hit a deadlock
					So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)
				})

				Convey("Run jobs that use a NFS shared disk", func() {
					cmd := "touch /shared/test1"
					other := make(map[string]string)
					other["cloud_shared"] = "true"
					localReq := &Requirements{100, 1 * time.Minute, 1, 1, other, true, true, true}
					err := s.Schedule(ctx, cmd, localReq, 0, 1)
					So(err, ShouldBeNil)

					remoteReq := oss.reqForSpawn(localReq)
					for _, server := range oss.servers {
						if server.Flavor.RAM >= remoteReq.RAM {
							remoteReq.RAM = server.Flavor.RAM + 1000
						}
					}
					remoteReq.Other = other
					cmd = "touch /shared/test2"
					err = s.Schedule(ctx, cmd, remoteReq, 0, 1)
					So(err, ShouldBeNil)

					So(s.Busy(ctx), ShouldBeTrue)
					So(waitToFinish(ctx, s, 240, 1000), ShouldBeTrue)

					_, err = os.Stat("/shared/test1")
					So(err, ShouldBeNil)
					_, err = os.Stat("/shared/test2")
					So(err, ShouldBeNil)

					err = os.Remove("/shared/test1")
					So(err, ShouldBeNil)
					err = os.Remove("/shared/test2")
					So(err, ShouldBeNil)
				})

				if flavorRegex == `^[mso].*$` && os.Getenv("OS_TENANT_ID") == "" {
					Convey("Run a job on a specific flavor", func() {
						cmd := "sleep 10"
						other := make(map[string]string)
						other["cloud_flavor"] = "o2.small"
						thisReq := &Requirements{100, 1 * time.Minute, 1, 1, other, true, true, true}
						err := s.Schedule(ctx, cmd, thisReq, 0, 1)
						So(err, ShouldBeNil)
						So(s.Busy(ctx), ShouldBeTrue)

						spawnedCh := make(chan int)
						stopCh := make(chan bool)
						go func() {
							max := 0
							ticker := time.NewTicker(5 * time.Second)
							for {
								select {
								case <-ticker.C:
									novaCount := novaCountServers(novaCmd, rName, "", "o2.small")
									if novaCount > max {
										max = novaCount
									}
									continue
								case <-stopCh:
									ticker.Stop()
									spawnedCh <- max
									return
								}
							}
						}()

						So(waitToFinish(ctx, s, 120, 1000), ShouldBeTrue)
						stopCh <- true
						spawned := <-spawnedCh
						close(spawnedCh)
						So(spawned, ShouldEqual, 1)
					})
				} else {
					SkipConvey("Skipping author's flavor test", func() {})
				}

				Convey("Run jobs with no inputs/outputs", func() {
					// on authors setup, the following count is sufficient to
					// get up to 3 instances and then kill an un-needed 4th
					// prior to cleaning up *** would be good to test hitting
					// the quota as well, but that takes too long and is
					// unreliable
					count := 18
					eta := 200
					cmd := "sleep 10"
					oReqs := make(map[string]string)
					thisReq := &Requirements{100, 1 * time.Minute, 16, 1, oReqs, true, true, true}
					err := s.Schedule(ctx, cmd, thisReq, 0, count)
					So(err, ShouldBeNil)
					So(s.Busy(ctx), ShouldBeTrue)

					spawnedCh := make(chan int)
					stopCh := make(chan bool)
					go func() {
						max := 0
						ticker := time.NewTicker(5 * time.Second)
						for {
							select {
							case <-ticker.C:
								novaCount := novaCountServers(novaCmd, rName, "")
								if novaCount > max {
									max = novaCount
								}
								continue
							case <-stopCh:
								ticker.Stop()
								spawnedCh <- max
								return
							}
						}
					}()

					So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)
					stopCh <- true
					spawned := <-spawnedCh
					close(spawnedCh)
					So(spawned, ShouldBeBetweenOrEqual, 2, count)

					foundServers := novaCountServers(novaCmd, rName, "")
					So(foundServers, ShouldBeBetweenOrEqual, 1, eta/10) // (assuming a ~10s spawn time)

					// after the last run, they are all auto-destroyed
					<-time.After(20 * time.Second)

					foundServers = novaCountServers(novaCmd, rName, "")
					So(foundServers, ShouldEqual, 0)

					// *** not really confirming that the cmds actually ran on
					// the spawned servers
				})

				// *** we really need to mock OpenStack instead of setting
				// these debug package variables...
				Convey("Run everything even when a server fails to spawn", func() {
					debugCounter = 0
					debugEffect = "failFirstSpawn"
					oReqs := make(map[string]string)
					newReq := &Requirements{100, 1 * time.Minute, 1, 1, oReqs, true, true, true}
					newCount := 3
					eta := 120
					cmd := "sleep 10"
					err := s.Schedule(ctx, cmd, newReq, 0, newCount)
					So(err, ShouldBeNil)
					So(s.Busy(ctx), ShouldBeTrue)
					So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)
				})

				Convey("Run jobs and have servers still self-terminate when a server is slow to spawn", func() {
					debugCounter = 0
					debugEffect = "slowSecondSpawn"
					oReqs := make(map[string]string)
					newReq := &Requirements{100, 1 * time.Minute, 1, 1, oReqs, true, true, true}
					newCount := 3
					eta := 120
					cmd := "sleep 10"
					err := s.Schedule(ctx, cmd, newReq, 0, newCount)
					So(err, ShouldBeNil)
					So(s.Busy(ctx), ShouldBeTrue)
					So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)

					<-time.After(20 * time.Second)

					foundServers := novaCountServers(novaCmd, rName, "")
					So(foundServers, ShouldEqual, 0)

					debugCounter = 0
					debugEffect = ""
				})

				// *** test if we have a Centos 7 image to use...
				if osPrefix != "CentOS-7" {
					oReqs := make(map[string]string)
					oReqs["cloud_os"] = "CentOS-7"
					oReqs["cloud_user"] = "centos"
					oReqs["cloud_os_ram"] = "4096"

					Convey("Override the default os image and ram", func() {
						newReq := &Requirements{100, 1 * time.Minute, 1, 1, oReqs, true, true, true}
						newCount := 3
						eta := 120
						cmd := "sleep 10 && (echo override > " + oFile + ") || true"
						err := s.Schedule(ctx, cmd, newReq, 0, newCount)
						So(err, ShouldBeNil)
						So(s.Busy(ctx), ShouldBeTrue)

						spawned := 0
						var ssync sync.Mutex
						go func() {
							ticker := time.NewTicker(1 * time.Second)
							limit := time.After(time.Duration(eta-5) * time.Second)
							for {
								select {
								case <-ticker.C:
									ssync.Lock()
									spawned = novaCountServers(novaCmd, rName, oReqs["cloud_os"])
									if spawned > 0 {
										ticker.Stop()
										ssync.Unlock()
										return
									}
									ssync.Unlock()
									continue
								case <-limit:
									ticker.Stop()
									return
								}
							}
						}()

						So(waitToFinish(ctx, s, eta, 1000), ShouldBeTrue)
						ssync.Lock()
						So(spawned, ShouldBeBetweenOrEqual, 1, newCount)
						ssync.Unlock()

						<-time.After(20 * time.Second)

						foundServers := novaCountServers(novaCmd, rName, "")
						So(foundServers, ShouldEqual, 0)

						// none of the cmds should have run on the local machine
						_, err = os.Stat(oFile)
						So(err, ShouldNotBeNil)
						So(os.IsNotExist(err), ShouldBeTrue)
					})
				}

				numCores := 5
				oReqsm := make(map[string]string)
				multiCoreFlavor, err := oss.determineFlavor(ctx, &Requirements{1024, 1 * time.Minute, float64(numCores), 0, oReqsm, true, true, true}, "u")
				if err == nil && multiCoreFlavor.Cores >= numCores {
					oReqs := make(map[string]string)
					oReqs["cloud_os_ram"] = strconv.Itoa(multiCoreFlavor.RAM)
					jobReq := &Requirements{multiCoreFlavor.RAM / numCores, 1 * time.Minute, 1, 0, oReqs, true, true, true}
					confirmFlavor, err := oss.determineFlavor(ctx, oss.reqForSpawn(jobReq), "v")
					if err == nil && confirmFlavor.Cores >= numCores {
						Convey("Run multiple jobs at once on multi-core servers", func() {
							cmd := "sleep 30"
							jobReq := &Requirements{multiCoreFlavor.RAM / numCores, 1 * time.Minute, 1, 0, oReqs, true, true, true}
							err = s.Schedule(ctx, cmd, jobReq, 0, numCores)
							So(err, ShouldBeNil)
							So(s.Busy(ctx), ShouldBeTrue)

							waitSecs := 150
							spawnedCh := make(chan int, 1)
							go func() {
								maxSpawned := 0
								ticker := time.NewTicker(1 * time.Second)
								limit := time.After(time.Duration(waitSecs-5) * time.Second)
								for {
									select {
									case <-ticker.C:
										spawned := novaCountServers(novaCmd, rName, oReqs["cloud_os"])
										if spawned > maxSpawned {
											maxSpawned = spawned
										}
										continue
									case <-limit:
										ticker.Stop()
										spawnedCh <- maxSpawned
										return
									}
								}
							}()

							// wait for enough time to have spawned a server
							// and run the commands in parallel, but not
							// sequentially *** but how long does it take to
							// spawn?! (50s in authors test area, but this
							// will vary...) we need better confirmation of
							// parallel run...
							So(waitToFinish(ctx, s, waitSecs, 1000), ShouldBeTrue)
							spawned := <-spawnedCh
							So(spawned, ShouldEqual, 1)
						})
					} else {
						SkipConvey("Skipping multi-core server tests due to lack of suitable multi-core server flavors", func() {})
					}
				} else {
					SkipConvey("Skipping multi-core server tests due to lack of suitable multi-core server flavors", func() {})
				}

				// *** when we have mocks, need to test that flavor sets work
				// as expected by filling up hardware in one set and seeing that
				// we fail over to the other set etc.
			})

			// wait a while for any remaining jobs to finish
			So(waitToFinish(ctx, s, 60, 1000), ShouldBeTrue)
		} else {
			SkipConvey("Actual OpenStack scheduling tests are skipped if not in OpenStack with nova or openstack installed", func() {})
		}
	})

	if novaCmd != "" {
		Convey("You can get a new openstack scheduler that can do multiple spawns", t, func() {
			tmpdir, errt := os.MkdirTemp("", "wr_schedulers_openstack_test_output_dir_")
			if errt != nil {
				log.Fatal(errt)
			}
			defer os.RemoveAll(tmpdir)
			config.SavePath = filepath.Join(tmpdir, "os_resources")
			config.SimultaneousSpawns = 5
			s, errn := New(ctx, "openstack", config)
			So(errn, ShouldBeNil)
			So(s, ShouldNotBeNil)
			defer func() {
				s.Cleanup(ctx)
			}()
			oss := s.impl.(*opst)

			if oss.provider.InCloud() {
				ignoreServers := make(map[string]bool)
				oss.serversMutex.RLock()
				for _, server := range oss.servers {
					ignoreServers[server.ID] = true
				}
				oss.serversMutex.RUnlock()

				getServerFlavors := func() map[int]int {
					oss.serversMutex.RLock()
					defer oss.serversMutex.RUnlock()
					flavors := make(map[int]int)
					for _, server := range oss.servers {
						if ignoreServers[server.ID] {
							continue
						}
						flavors[server.Flavor.Cores]++
					}
					return flavors
				}

				waitForServers := func(wanted map[int]int) bool {
					limit := time.After(120 * time.Second)
					ticker := time.NewTicker(1 * time.Second)
					for {
						select {
						case <-ticker.C:
							if len(wanted) == 0 {
								oss.stateUpdate(ctx)
							}
							have := getServerFlavors()
							ok := true
							for cpus, desired := range wanted {
								if actual, exists := have[cpus]; exists {
									if actual < desired {
										ok = false
										// fmt.Printf("only %d not %d for flavor %d\n", actual, desired, cpus)
										break
									}
								} else {
									ok = false
									// fmt.Printf("missing flavor %d\n", cpus)
									break
								}
							}
							for cpus := range have {
								if _, exists := wanted[cpus]; !exists {
									ok = false
									// fmt.Printf("extra flavor %d\n", cpus)
									break
								}
							}

							if ok {
								ticker.Stop()
								<-time.After(2 * time.Second)
								return true
							}
							continue
						case <-limit:
							ticker.Stop()
							return false
						}
					}
				}

				other := make(map[string]string)
				other["cloud_script"] = "echo forced new servers"

				Convey("You can Schedule many cmds and a bunch run right away", func() {
					smallCmd := "sleep 30"
					smallReq := &Requirements{100, 1 * time.Minute, 2, 1, other, true, true, true}
					err := s.Schedule(ctx, smallCmd, smallReq, 0, config.SimultaneousSpawns*2)
					So(err, ShouldBeNil)

					wanted := make(map[int]int)
					wanted[2] = config.SimultaneousSpawns
					So(waitForServers(wanted), ShouldBeTrue)

					err = s.Schedule(ctx, smallCmd, smallReq, 0, 0)
					So(err, ShouldBeNil)

					wanted = make(map[int]int)
					So(waitForServers(wanted), ShouldBeTrue)
				})

				Convey("You can Schedule many small cmds and then a higher priority large cmd and the large runs asap", func() {
					smallCmd := "sleep 60"
					smallReq := &Requirements{100, 1 * time.Minute, 2, 1, other, true, true, true}
					err := s.Schedule(ctx, smallCmd, smallReq, 0, config.SimultaneousSpawns*3)
					So(err, ShouldBeNil)

					bigCmd := "sleep 2"
					bigReq := &Requirements{100, 1 * time.Minute, 4, 1, other, true, true, true}
					err = s.Schedule(ctx, bigCmd, bigReq, 1, 1)
					So(err, ShouldBeNil)

					wanted := make(map[int]int)
					wanted[2] = (config.SimultaneousSpawns * 2) - 1
					wanted[4] = 1
					So(waitForServers(wanted), ShouldBeTrue)

					err = s.Schedule(ctx, smallCmd, smallReq, 0, 0)
					So(err, ShouldBeNil)
					err = s.Schedule(ctx, bigCmd, bigReq, 0, 0)
					So(err, ShouldBeNil)

					wanted = make(map[int]int)
					So(waitForServers(wanted), ShouldBeTrue)
				})

				Convey("You can Schedule a large command and then a small cmd and get both running and sharing servers", func() {
					bigCmd := "sleep 15"
					bigReq := &Requirements{100, 1 * time.Minute, 6, 1, other, true, true, true}
					err := s.Schedule(ctx, bigCmd, bigReq, 0, config.SimultaneousSpawns-1)
					So(err, ShouldBeNil)

					smallCmd := "sleep 16"
					smallReq := &Requirements{100, 1 * time.Minute, 2, 1, other, true, true, true}
					err = s.Schedule(ctx, smallCmd, smallReq, 0, config.SimultaneousSpawns)
					So(err, ShouldBeNil)

					wanted := make(map[int]int)
					wanted[8] = config.SimultaneousSpawns - 1
					wanted[2] = 1
					So(waitForServers(wanted), ShouldBeTrue)

					oss.serversMutex.RLock()
					eightcores := 0
					twocores := 0
					space := 0
					for _, server := range oss.servers {
						if server.Flavor.Cores == 8 {
							eightcores++
							thisSpace := server.HasSpaceFor(2, 1, 1)
							space += thisSpace
						} else {
							twocores++
						}
					}
					oss.serversMutex.RUnlock()

					err = s.Schedule(ctx, smallCmd, smallReq, 0, 0)
					So(err, ShouldBeNil)
					err = s.Schedule(ctx, bigCmd, bigReq, 0, 0)
					So(err, ShouldBeNil)

					wanted = make(map[int]int)
					So(waitForServers(wanted), ShouldBeTrue)

					So(eightcores, ShouldEqual, config.SimultaneousSpawns-1)
					So(space, ShouldEqual, 0)
					So(twocores, ShouldBeBetweenOrEqual, 1, config.SimultaneousSpawns)
				})
			}
		})
	}
}

func getInfoOfFilesInDir(tmpdir string, expected int) []fs.DirEntry {
	files, err := os.ReadDir(tmpdir)
	if err != nil {
		log.Fatal(err)
	}
	if len(files) < expected {
		// wait a little longer for things to sync up, by running ls
		cmd := exec.Command("ls", tmpdir)
		err = cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
		files, err = os.ReadDir(tmpdir)
		if err != nil {
			log.Fatal(err)
		}
	}
	return files
}

func testDirForFiles(tmpdir string, expected int) (numfiles int) {
	return len(getInfoOfFilesInDir(tmpdir, expected))
}

func mtimesOfFilesInDir(tmpdir string, expected int) []time.Time {
	files := getInfoOfFilesInDir(tmpdir, expected)
	times := make([]time.Time, 0, len(files))
	for _, entry := range files {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		times = append(times, info.ModTime())
		os.Remove(filepath.Join(tmpdir, info.Name()))
	}
	return times
}

func waitToFinish(ctx context.Context, s *Scheduler, maxS int, interval int) bool {
	done := make(chan bool, 1)
	go func() {
		limit := time.After(time.Duration(maxS) * time.Second)
		ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				if !s.Busy(ctx) {
					ticker.Stop()
					done <- true
					return
				}
				continue
			case <-limit:
				ticker.Stop()
				done <- false
				return
			}
		}
	}()
	answer := <-done
	return answer
}

func novaCountServers(novaCmd string, rName, osPrefix string, flavor ...string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var extra string
	if len(flavor) == 1 {
		extra = "--flavor " + flavor[0] + " "
	}

	cmdStr := novaCmd + " list " + extra
	if osPrefix == "" {
		cmdStr += "| grep -c "
	} else {
		cmdStr += "| grep "
	}
	cmdStr += rName
	cmd := exec.CommandContext(ctx, "bash", "-c", cmdStr)
	out, err := cmd.Output()
	if ctx.Err() != nil {
		log.Printf("exec of [%s] timed out\n", cmdStr)
		return 0
	}
	if err != nil {
		// uncomment if debugging failures where count is always 0:
		// log.Printf("cmd [%s] failed: %s\n", cmdStr, err)
		return 0
	}

	if osPrefix == "" {
		count, err := strconv.Atoi(strings.TrimSpace(string(out)))
		if err == nil {
			return count
		}
		log.Printf("Atoi following [%s] failed: %s\n", cmdStr, err)
	} else {
		r := regexp.MustCompile(rName + "-\\S+")
		count := 0
		for _, name := range r.FindAll(out, -1) {
			showCmdStr := novaCmd + " show " + string(name) + " | grep image"
			showCmd := exec.Command("bash", "-c", showCmdStr)
			showOut, err := showCmd.Output()
			if err == nil {
				if strings.Contains(string(showOut), osPrefix) {
					count++
				}
			} else {
				log.Printf("cmd [%s] failed: %s\n", showCmdStr, err)
			}
		}
		return count
	}
	return 0
}

func testProcessNotRunning(ctx context.Context, s *Scheduler, r *Requirements) {
	tmpdir, err := os.MkdirTemp("./", "wr_schedulers_test_output_dir_")
	So(err, ShouldBeNil)
	defer os.RemoveAll(tmpdir)

	pidHostFile, err := filepath.Abs(path.Join(tmpdir, "pid.host"))
	So(err, ShouldBeNil)
	pidHostFileTmp := pidHostFile + ".tmp"

	cmd := fmt.Sprintf("perl -e '$tmp = shift; $path = shift; open($fh, q[>], $tmp); print $fh qq[$$\n]; use Sys::Hostname qw(hostname); print $fh hostname(), qq[\n]; close($fh); rename $tmp, $path; for (1..15) { sleep(1) }' %s %s", pidHostFileTmp, pidHostFile)

	err = s.Schedule(ctx, cmd, r, 0, 1)
	So(err, ShouldBeNil)
	So(s.Busy(ctx), ShouldBeTrue)

	pid, host, worked := parsePidHostFile(pidHostFile)
	So(worked, ShouldBeTrue)

	Convey("ProcessNotRunngingOnHost() returns false if its still running", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		So(s.ProcessNotRunngingOnHost(ctx, pid, host), ShouldBeFalse)

		Convey("But true if we kill it", func() {
			server, exists := s.impl.getHost(host)
			So(exists, ShouldBeTrue)
			So(server, ShouldNotBeNil)

			_, _, err := server.RunCmd(context.Background(), fmt.Sprintf("kill -9 %d", pid), false)
			So(err, ShouldBeNil)
			<-time.After(1 * time.Second)

			ctx2, cancel2 := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel2()

			So(s.ProcessNotRunngingOnHost(ctx2, pid, host), ShouldBeTrue)
		})
	})
}

func parsePidHostFile(path string) (int, string, bool) {
	dir := filepath.Dir(path)
	parsed := make(chan bool, 1)
	pidCh := make(chan int, 1)
	hostCh := make(chan string, 1)
	go func() {
		limit := time.After(13 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				// read the dir because on NFS we never see the file as existing
				// until the dir is read
				_, err := os.ReadDir(dir)
				if err != nil {
					fmt.Printf("error reading directory %s: %s\n", dir, err)
					ticker.Stop()
					parsed <- false
					return
				}

				_, err = os.Stat(path)
				if os.IsNotExist(err) {
					continue
				}

				content, err := os.ReadFile(path)
				if err != nil {
					fmt.Printf("%s couldn't be read: %s\n", path, err)
					ticker.Stop()
					parsed <- false
					return
				}

				split := strings.Split(string(content), "\n")
				pid, err := strconv.Atoi(split[0])
				if err != nil {
					fmt.Printf("%s pid didn't parse: %s\n", path, err)
					ticker.Stop()
					parsed <- false
					return
				}

				pidCh <- pid
				hostCh <- split[1]
				parsed <- true
				return
			case <-limit:
				ticker.Stop()
				parsed <- false
				return
			}
		}
	}()

	ok := <-parsed
	if !ok {
		return 0, "", ok
	}

	pid := <-pidCh
	host := <-hostCh
	return pid, host, ok
}
