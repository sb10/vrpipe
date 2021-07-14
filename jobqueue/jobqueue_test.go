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

package jobqueue

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	muxfys "github.com/VertebrateResequencing/muxfys/v4"
	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/shirou/gopsutil/process"
	. "github.com/smartystreets/goconvey/convey"
)

const maxSpawnTime = 240 * time.Second
const serverRC = `echo %s %s %s %s %d %d`

var runnermode bool
var runnerfail bool
var runnerdebug bool
var schedgrp string
var runnermodetmpdir string
var rdeployment string
var rserver string
var rdomain string
var rtimeout int
var maxmins int
var envVars = os.Environ()
var servermode bool
var serverKeepDB bool
var serverEnableRunners bool

func init() {
	clog.ToDefault()

	sync.Opts.DeadlockTimeout = 5 * time.Minute // some openstack behaviour needs a pretty long timeout

	flag.BoolVar(&runnermode, "runnermode", false, "enable to disable tests and act as a 'runner' client")
	flag.BoolVar(&runnerfail, "runnerfail", false, "make the runner client fail")
	flag.BoolVar(&runnerdebug, "runnerdebug", false, "make the runner create debug files")
	flag.StringVar(&schedgrp, "schedgrp", "", "schedgrp for runnermode")
	flag.StringVar(&rdeployment, "rdeployment", "", "deployment for runnermode")
	flag.StringVar(&rserver, "rserver", "", "server for runnermode")
	flag.StringVar(&rdomain, "rdomain", "", "domain for runnermode")
	flag.IntVar(&rtimeout, "rtimeout", 1, "reserve timeout for runnermode")
	flag.IntVar(&maxmins, "maxmins", 0, "maximum mins allowed for  runnermode")
	flag.StringVar(&runnermodetmpdir, "tmpdir", "", "tmp dir for runnermode")
	flag.BoolVar(&servermode, "servermode", false, "enable to disable tests and act as a 'server'")
	flag.BoolVar(&serverKeepDB, "keepdb", false, "have the server keep its database when it starts")
	flag.BoolVar(&serverEnableRunners, "enablerunners", false, "have the server spawn runners for jobs")
	ServerLogClientErrors = false
}

func serverShutDownTime() time.Duration {
	// golang can't actually do exec.Command.Start() in parallel and has a
	// global lock on them, so we have to allow the 500ms of time to any pending
	// starts to resolve before we can shut down.
	return ClientTouchInterval + httpServerShutdownTime + serverShutdownRunnerTickerTime + 500*time.Millisecond
}

func TestJobqueueUtils(t *testing.T) {
	if runnermode || servermode {
		return
	}

	Convey("CurrentIP() works", t, func() {
		ip, err := internal.CurrentIP("")
		So(err, ShouldBeNil)
		So(ip, ShouldNotBeBlank)
		ip, err = internal.CurrentIP("9.9.9.9/24")
		So(err, ShouldBeNil)
		So(ip, ShouldBeBlank)
		ip, err = internal.CurrentIP(ip + "/16")
		So(err, ShouldBeNil)
		So(ip, ShouldEqual, ip)
	})

	Convey("generateToken() and tokenMatches() work", t, func() {
		tokenFile, err := os.CreateTemp("", "wr.test.token")
		So(err, ShouldBeNil)
		tokenFile.Close()
		tokenPath := tokenFile.Name()
		defer func() {
			err = os.Remove(tokenPath)
			So(err, ShouldBeNil)
		}()

		err = os.Remove(tokenPath)
		So(err, ShouldBeNil)
		token, err := generateToken(tokenPath)
		So(err, ShouldBeNil)
		So(len(token), ShouldEqual, tokenLength)

		token2, err := generateToken(tokenPath)
		So(err, ShouldBeNil)
		So(len(token2), ShouldEqual, tokenLength)
		So(token, ShouldNotResemble, token2)
		So(tokenMatches(token, token2), ShouldBeFalse)
		So(tokenMatches(token, token), ShouldBeTrue)

		// if tokenPath is a file that contains a token, generateToken doesn't
		// generate a new token, but returns that one
		err = os.WriteFile(tokenPath, token2, 0600)
		So(err, ShouldBeNil)

		token3, err := generateToken(tokenPath)
		So(err, ShouldBeNil)
		So(len(token3), ShouldEqual, tokenLength)
		So(token3, ShouldResemble, token2)
		So(tokenMatches(token2, token3), ShouldBeTrue)
	})

	Convey("GenerateCerts creates certificate files", t, func() {
		certtmpdir, err := os.MkdirTemp("", "wr_jobqueue_cert_dir_")
		So(err, ShouldBeNil)
		defer os.RemoveAll(certtmpdir)

		caFile := filepath.Join(certtmpdir, "ca.pem")
		certFile := filepath.Join(certtmpdir, "cert.pem")
		keyFile := filepath.Join(certtmpdir, "key.pem")
		certDomain := "localhost"
		err = internal.GenerateCerts(caFile, certFile, keyFile, certDomain)
		So(err, ShouldBeNil)
		_, err = os.Stat(caFile)
		So(err, ShouldBeNil)
		_, err = os.Stat(certFile)
		So(err, ShouldBeNil)
		_, err = os.Stat(keyFile)
		So(err, ShouldBeNil)

		Convey("CertExpiry shows they expire in a year", func() {
			expiry, err := internal.CertExpiry(caFile)
			So(err, ShouldBeNil)
			So(expiry, ShouldHappenBetween, time.Now().Add(364*24*time.Hour), time.Now().Add(366*24*time.Hour))
		})
	})

	Convey("currentDisk works recursively with ignores", t, func() {
		dir, err := os.MkdirTemp("", "wr_currentDisk_test")
		So(err, ShouldBeNil)
		defer os.RemoveAll(dir)

		subdir := filepath.Join(dir, ".mnt", "sub")
		err = os.MkdirAll(subdir, fs.ModePerm)
		So(err, ShouldBeNil)

		createLargeFile := func(path string, size int64) error {
			f, errc := os.Create(path)
			if errc != nil {
				return errc
			}
			errc = f.Truncate(size)
			if errc != nil {
				return errc
			}

			return f.Close()
		}

		err = createLargeFile(filepath.Join(dir, "a.txt"), 1024*1024)
		So(err, ShouldBeNil)
		err = createLargeFile(filepath.Join(subdir, "b.txt"), 1024*1024)
		So(err, ShouldBeNil)

		s, err := currentDisk(dir)
		So(err, ShouldBeNil)
		So(s, ShouldEqual, 2)

		s, err = currentDisk(dir, map[string]bool{subdir: true})
		So(err, ShouldBeNil)
		So(s, ShouldEqual, 1)
	})
}

func jobqueueTestInit(shortTTR bool) (internal.Config, ServerConfig, string, *jqs.Requirements, time.Duration) {
	ctx := context.Background()
	// load our config to know where our development manager port is supposed to
	// be; we'll use that to test jobqueue
	config := internal.ConfigLoadFromParentDir(ctx, "development")
	managerDBBkFile := config.ManagerDBFile + "_bk" // not config.ManagerDBBkFile in case it is an s3 url
	serverConfig := ServerConfig{
		Port:            config.ManagerPort,
		WebPort:         config.ManagerWeb,
		SchedulerName:   "local",
		SchedulerConfig: &jqs.ConfigLocal{Shell: config.RunnerExecShell},
		DBFile:          config.ManagerDBFile,
		DBFileBackup:    managerDBBkFile,
		TokenFile:       config.ManagerTokenFile,
		CAFile:          config.ManagerCAFile,
		CertFile:        config.ManagerCertFile,
		CertDomain:      config.ManagerCertDomain,
		KeyFile:         config.ManagerKeyFile,
		Deployment:      config.Deployment,
	}
	addr := "localhost:" + config.ManagerPort

	setDomainIP(config.ManagerCertDomain)

	ServerInterruptTime = 10 * time.Millisecond
	ServerReserveTicker = 10 * time.Millisecond
	ClientReleaseDelay = 100 * time.Millisecond
	clientConnectTime := 1500 * time.Millisecond

	if shortTTR {
		ServerItemTTR = 1 * time.Second
		ClientTouchInterval = 500 * time.Millisecond
	}

	standardReqs := &jqs.Requirements{RAM: 10, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}

	return *config, serverConfig, addr, standardReqs, clientConnectTime
}

// startServer runs the given exe with the --servermode arg. It is assumed that
// doing so starts a jobqueue server in another process that will kill itself
// after some time or when signalled. We return a client that is connected to
// that server, along with the client token and the server's pid. If keepDB is
// true, the exe will be run with --keepdb arg as well. Same idea with
// enableRunners. This also creates config.ManagerDir dir on disk if necessary,
// and does not delete it afterwards.
func startServer(serverExe string, keepDB, enableRunners bool, config internal.Config, addr string) (*Client, []byte, *exec.Cmd, error) {
	err := os.MkdirAll(config.ManagerDir, 0700)
	if err != nil {
		log.Fatal(err)
	}

	preStart := time.Now()

	args := []string{"--servermode"}
	if keepDB {
		args = append(args, "--keepdb")
	}
	if enableRunners {
		args = append(args, "--enablerunners")
	}

	// run the server in the background
	cmd := exec.Command(serverExe, args...)
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	// wait a while for our server cmd to actually start serving
	mTimeout := 10 * time.Second
	internal.WaitForFile(config.ManagerTokenFile, preStart, mTimeout)
	token, err := os.ReadFile(config.ManagerTokenFile)
	if err != nil || len(token) == 0 {
		return nil, nil, cmd, err
	}
	jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, 2*time.Second)
	return jq, token, cmd, err
}

// runServer starts a jobqueue server, and is what calling this test script in
// --servermode runs.
func runServer(ctx context.Context) {
	// uncomment and set a log path to debug server issues in TestJobqueueSignal
	// fh, err := log15.FileHandler("/log", log15.LogfmtFormat())
	// if err != nil {
	// 	log.Fatalf("error opening file: %v", err)
	// }
	// h := l15h.CallerInfoHandler(fh)
	// testLogger.SetHandler(log15.LvlFilterHandler(log15.LvlDebug, h))
	// pid := os.Getpid()
	// testLogger = testLogger.New("pid", pid)

	_, serverConfig, _, _, _ := jobqueueTestInit(false)

	if serverKeepDB {
		wipeDevDBOnInit = false
	}

	if serverEnableRunners {
		self, err := os.Executable()
		if err != nil {
			clog.Crit(ctx, "os.Executable() failed", "err", err)
			os.Exit(1)
		}

		// we can't use the --tmpdir option, since that means the runner cmds
		// won't match between invocations, so recovery won't be complete. We
		// don't need it anyway
		serverConfig.RunnerCmd = self + " --runnermode --schedgrp '%s' --rdeployment %s --rserver '%s' --rdomain %s --rtimeout %d --maxmins %d"
	}

	ServerItemTTR = 200 * time.Millisecond
	server, msg, _, err := serve(ctx, serverConfig)
	if err != nil {
		clog.Crit(ctx, "test daemon failed to start", "err", err)
		os.Exit(1)
	}
	if msg != "" {
		clog.Warn(ctx, msg)
	}

	// we'll Block() later, but just in case the parent tests bomb out
	// without killing us, we'll stop after 20s
	go func() {
		<-time.After(20 * time.Second)
		clog.Warn(ctx, "test daemon stopping after 20s")
		server.Stop(ctx, true)
	}()

	clog.Warn(ctx, "test daemon up, will block")

	// wait until we are killed
	err = server.Block()
	clog.Warn(ctx, "test daemon exiting", "reason", err)
	os.Exit(0)
}

// serve calls Serve() but with a retry for 5s on failure. This allows time for
// a server that we recently stopped in a prior test to really not be listening
// on the ports any more.
func serve(ctx context.Context, config ServerConfig) (*Server, string, []byte, error) {
	server, msg, token, err := Serve(ctx, config)
	if err != nil {
		limit := time.After(5 * time.Second)
		ticker := time.NewTicker(500 * time.Millisecond)
	RETRY:
		for {
			select {
			case <-ticker.C:
				server, msg, token, err = Serve(ctx, config)
				if err != nil {
					continue
				}
				ticker.Stop()
				break RETRY
			case <-limit:
				ticker.Stop()
				break RETRY
			}
		}
	}
	return server, msg, token, err
}

// waitUntilPidsAreGone waits up to the given number of seconds for the pids in
// the map to not exist. The pids are deleted from the map when they no longer
// exist. Returns true if all pids gone (and the map will be empty).
func waitUntilPidsAreGone(pids map[int]bool, seconds int) bool {
	for i := 0; i < seconds; i++ {
		for pid := range pids {
			process, errf := os.FindProcess(pid)
			if errf != nil && process == nil {
				delete(pids, pid)
			}
			errs := process.Signal(syscall.Signal(0))
			if errs != nil {
				delete(pids, pid)
			}
		}
		if len(pids) == 0 {
			break
		}
		<-time.After(1 * time.Second)
	}
	return len(pids) == 0
}

func TestJobqueueSignal(t *testing.T) {
	ctx := context.Background()

	if runnermode {
		return
	}
	if servermode {
		runServer(ctx)
		return
	}

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	config, _, addr, _, clientConnectTime := jobqueueTestInit(false)

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	servertmpdir, err := os.MkdirTemp(pwd, "wr_jobqueue_test_server_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(servertmpdir)

	// these tests need the server running in it's own pid so we can test signal
	// handling in the client. Our server will be ourself in --servermode, so
	// first we'll compile ourselves to the tmpdir
	serverExe := filepath.Join(servertmpdir, "server")
	cmd := exec.Command("go", "test", "-tags", "netgo", "-run", "TestJobqueue", "-c", "-o", serverExe)
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	errr := os.Remove(config.ManagerTokenFile)
	if errr != nil && !os.IsNotExist(errr) {
		t.Fatalf("failed to delete token file before test: %s\n", errr)
	}
	defer func() {
		errr := os.Remove(config.ManagerTokenFile)
		if errr != nil && !os.IsNotExist(errr) {
			t.Fatalf("failed to delete token file after test: %s\n", errr)
		}
	}()

	ClientTouchInterval = 50 * time.Millisecond
	ClientRetryWait = 1 * time.Second

	alreadyKilled := make(map[int]bool)
	killServer := func(jq *Client, serverPid int, serverCmd *exec.Cmd) {
		if alreadyKilled[serverPid] {
			return
		}
		errd := jq.Disconnect()
		if errd != nil && !strings.HasSuffix(errd.Error(), "connection closed") {
			fmt.Printf("failed to disconnect: %s\n", errd)
		}

		waited := make(chan bool)
		go func() {
			errw := serverCmd.Wait()
			if errw != nil {
				fmt.Printf("failed to reap server pid: %s\n", errw)
			}
			waited <- true
		}()

		<-time.After(500 * time.Millisecond)
		errk := syscall.Kill(serverPid, syscall.SIGTERM)
		if errk != nil {
			fmt.Printf("failed to send SIGTERM to server: %s\n", errk)
		}

		<-waited
		alreadyKilled[serverPid] = true
	}

	Convey("Once a jobqueue server is up as a daemon", t, func() {
		jq, token, serverCmd, errf := startServer(serverExe, false, false, config, addr)
		serverPid := serverCmd.Process.Pid
		So(errf, ShouldBeNil)

		defer killServer(jq, serverPid, serverCmd)

		So(jq.ServerInfo.PID, ShouldEqual, serverPid)

		Convey("You can set up a long-running job for execution", func() {
			cmd := "perl -e 'for (1..3) { sleep(1) }'"
			cmd2 := "perl -e 'for (2..4) { sleep(1) }'"
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 4 * time.Second, Cores: 1}, Retries: uint8(0), RepGroup: "3secs_pass"})
			jobs = append(jobs, &Job{Cmd: cmd2, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), RepGroup: "3secs_fail"})
			RecSecRound = 1
			defer func() {
				// revert back to normal
				RecSecRound = 1800
			}()
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateReserved)

			job2, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job2.Cmd, ShouldEqual, cmd2)
			So(job2.State, ShouldEqual, JobStateReserved)

			Convey("Signals are handled during execution, and we can see when jobs take too long", func() {
				go func() {
					<-time.After(2 * time.Second)
					errk := syscall.Kill(os.Getpid(), syscall.SIGTERM)
					if errk != nil {
						fmt.Printf("failed to send SIGTERM: %s\n", errk)
					}
				}()

				j1worked := make(chan bool)
				go func() {
					err := jq.Execute(ctx, job, config.RunnerExecShell)
					if err != nil {
						if jqerr, ok := err.(Error); ok && jqerr.Err == FailReasonSignal && job.State == JobStateBuried && job.Exited && job.Exitcode == -1 && job.FailReason == FailReasonSignal {
							j1worked <- true
						}
					}
					j1worked <- false
				}()

				j2worked := make(chan bool)
				go func() {
					err := jq.Execute(ctx, job2, config.RunnerExecShell)
					if err != nil {
						if jqerr, ok := err.(Error); ok && jqerr.Err == FailReasonTime && job2.State == JobStateBuried && job2.Exited && job2.Exitcode == -1 && job2.FailReason == FailReasonTime {
							j2worked <- true
						}
					}
					j2worked <- false
				}()

				So(<-j1worked, ShouldBeTrue)
				So(<-j2worked, ShouldBeTrue)

				jq2, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				defer disconnect(jq2)
				job, err = jq2.GetByEssence(&JobEssence{Cmd: cmd}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.Cmd, ShouldEqual, cmd)
				So(job.State, ShouldEqual, JobStateBuried)
				So(job.FailReason, ShouldEqual, FailReasonSignal)

				job2, err = jq2.GetByEssence(&JobEssence{Cmd: cmd2}, false, false)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.Cmd, ShouldEqual, cmd2)
				So(job2.State, ShouldEqual, JobStateBuried)
				So(job2.FailReason, ShouldEqual, FailReasonTime)
				So(job2.Requirements.Time.Seconds(), ShouldEqual, 1)

				// requirements only change on becoming ready
				kicked, err := jq.Kick([]*JobEssence{job2.ToEssense()})
				So(err, ShouldBeNil)
				So(kicked, ShouldEqual, 1)
				job2, err = jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.Cmd, ShouldEqual, cmd2)
				So(job2.State, ShouldEqual, JobStateReserved)
				So(job2.FailReason, ShouldEqual, FailReasonTime)
				So(job2.Requirements.Time.Seconds(), ShouldBeBetweenOrEqual, 3601, 3604)

				// all signals handled the same way, so no need for further
				// tests
			})
		})

		Convey("Running jobs are recovered after a hard server crash", func() {
			cmd := "sleep 10"
			cmd2 := "perl -e 'for (1..10) { sleep(1) }'" // we want to kill this part way, but `sleep` processes don't seem to die straight away when killed
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: 0}, Retries: uint8(0), RepGroup: "recover"})
			jobs = append(jobs, &Job{Cmd: cmd2, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: 0}, Retries: uint8(0), RepGroup: "buried"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateReserved)

			job2, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job2.Cmd, ShouldEqual, cmd2)
			So(job2.State, ShouldEqual, JobStateReserved)

			serverCmdCh := make(chan *exec.Cmd)
			go func() {
				<-time.After(2 * time.Second)
				gotJob, errg := jq.GetByEssence(&JobEssence{Cmd: cmd2}, false, false)
				killServer(jq, serverPid, serverCmd)
				if errg == nil && gotJob != nil {
					<-time.After(2 * time.Second)
					errk := syscall.Kill(gotJob.Pid, syscall.SIGKILL)
					if errk != nil {
						fmt.Printf("failed to send SIGKILL to job: %s\n", errk)
					}
				} else {
					log.Printf("failed to get job: %s\n", errg)
				}
				<-time.After(4 * time.Second)
				newJQ, _, newServerCmd, errf := startServer(serverExe, true, false, config, addr)
				if errf != nil {
					log.Printf("failed to start new server: %s\n", errf)
				} else if newJQ != nil {
					errd := newJQ.Disconnect()
					if errd != nil {
						fmt.Printf("failed to disconnect after making a new server: %s\n", errd)
					}
				}
				serverCmdCh <- newServerCmd
			}()

			j1worked := make(chan bool)
			giveUp1 := time.After(30 * time.Second)
			go func() {
				errch := make(chan error)
				go func() {
					errch <- jq.Execute(ctx, job, config.RunnerExecShell)
				}()
				select {
				case erre := <-errch:
					if erre != nil {
						// we expect that we lost the connection when we killed
						// the server, then reconnected to the new server and
						// therefore got ErrStopReserving, but otherwise
						// everything was fine
						jqerr, ok := erre.(Error)
						if !ok || jqerr.Err != ErrStopReserving {
							fmt.Printf("\nexecute had err: %s\n", erre)
							j1worked <- false
							return
						}
					} // though sometimes we manage to not lose the connection
					j1worked <- true
					return
				case <-giveUp1:
					fmt.Printf("\ngave up waiting for job to finish\n")
					j1worked <- false
				}
			}()

			j2worked := make(chan bool)
			giveUp2 := time.After(30 * time.Second)
			go func() {
				errch := make(chan error)
				go func() {
					errch <- jq.Execute(ctx, job2, config.RunnerExecShell)
				}()
				select {
				case erre := <-errch:
					if erre != nil && strings.Contains(erre.Error(), "exited with code -1") {
						j2worked <- true
						return
					}
					fmt.Printf("\njob2 had err %s\n", erre)
					j2worked <- false
					return
				case <-giveUp2:
					j2worked <- false
				}
			}()

			serverCmd = <-serverCmdCh
			serverPid = serverCmd.Process.Pid
			So(<-j1worked, ShouldBeTrue)
			So(<-j2worked, ShouldBeTrue)

			jq2, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer func() {
				errd := jq2.Disconnect()
				if errd != nil {
					fmt.Printf("failed to disconnect: %s\n", errd)
				}
			}()
			job, err = jq2.GetByEssence(&JobEssence{Cmd: cmd}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateComplete)

			job2, err = jq2.GetByEssence(&JobEssence{Cmd: cmd2}, false, false)
			So(err, ShouldBeNil)
			So(job2, ShouldNotBeNil)
			So(job2.Cmd, ShouldEqual, cmd2)
			So(job2.State, ShouldEqual, JobStateBuried)
		})

		Reset(func() {
			killServer(jq, serverPid, serverCmd)
		})
	})

	// the next tests will have runners enabled so that we can see what happens
	// when we force kill both the server and a runner
	Convey("Once a jobqueue server using local scheduler is up as a daemon", t, func() {
		jq, token, serverCmd, errf := startServer(serverExe, false, true, config, addr)
		serverPid := serverCmd.Process.Pid
		So(errf, ShouldBeNil)
		defer killServer(jq, serverPid, serverCmd)

		So(jq.ServerInfo.PID, ShouldEqual, serverPid)

		Convey("Killed runners after a hard server crash come up lost, and new runners don't overcommit resources due to existing runners", func() {
			cmd := "sleep 10"
			cmd2 := "perl -e 'for (1..10) { sleep(1) }'"
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1, Time: 10 * time.Second, Cores: float64(runtime.NumCPU())}, Retries: uint8(0), RepGroup: "recover"})
			jobs = append(jobs, &Job{Cmd: cmd2, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1, Time: 10 * time.Second, Cores: 0}, Retries: uint8(0), RepGroup: "lost"})
			inserts, already, erra := jq.Add(jobs, envVars, true)
			So(erra, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			// get pids of the runners the server spawned
			<-time.After(2 * time.Second)
			processes, err := process.Processes()
			So(err, ShouldBeNil)
			runnerPids := make(map[int]bool)
			var runnerPidToKill int
			for _, p := range processes {
				thisCmd, errc := p.Cmdline()
				if errc != nil {
					continue
				}
				if strings.Contains(thisCmd, "sleep") {
					parent, errp := p.Parent()
					if errp != nil {
						continue
					}
					parentCmd, errp := parent.Cmdline()
					if errp != nil {
						continue
					}
					if strings.Contains(parentCmd, serverExe) {
						pid := int(parent.Pid)

						if strings.Contains(thisCmd, "perl") {
							runnerPidToKill = pid
						}

						runnerPids[pid] = true
						if len(runnerPids) == 2 {
							break
						}
					}
				}
			}
			So(len(runnerPids), ShouldEqual, 2)
			So(runnerPidToKill, ShouldNotEqual, 0)
			So(runnerPidToKill, ShouldNotEqual, serverPid)

			// kill server and then second runner, then wait before starting new
			// server. We don't use killServer here because we don't want to
			// jq.Disconnect and reap before killing the runner
			errk := syscall.Kill(serverPid, syscall.SIGKILL)
			if errk != nil {
				fmt.Printf("failed to send SIGKILL to server: %s\n", errk)
			}
			<-time.After(2 * time.Second)
			errk = syscall.Kill(runnerPidToKill, syscall.SIGKILL)
			if errk != nil {
				fmt.Printf("failed to send SIGKILL to runner: %s\n", errk)
			}
			errd := jq.Disconnect()
			if errd != nil && !strings.HasSuffix(errd.Error(), "connection closed") {
				fmt.Printf("failed to disconnect: %s\n", errd)
			}
			errw := serverCmd.Wait()
			if errw != nil && !strings.Contains(errw.Error(), "signal: killed") {
				fmt.Printf("failed to reap server pid: %s\n", errw)
			}
			alreadyKilled[serverPid] = true

			<-time.After(4 * time.Second)
			var errf error
			jq, _, serverCmd, errf = startServer(serverExe, true, true, config, addr)
			serverPid = serverCmd.Process.Pid
			So(errf, ShouldBeNil)
			So(jq, ShouldNotBeNil)

			// add a new job which should wait until job 1 completes, since it
			// uses all CPUs
			cmd3 := "echo 1"
			jobs = []*Job{{Cmd: cmd3, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1, Time: 10 * time.Second, Cores: 1}, Retries: uint8(0), RepGroup: "wait"}}
			inserts, already, err = jq.Add(jobs, envVars, true)
			errd = jq.Disconnect()
			if errd != nil {
				fmt.Printf("failed to disconnect: %s\n", errd)
			}
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for runner pids to no longer exist
			waitUntilPidsAreGone(runnerPids, 16)
			So(len(runnerPids), ShouldEqual, 0)

			jq2, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer func() {
				errd := jq2.Disconnect()
				if errd != nil {
					fmt.Printf("failed to disconnect: %s\n", errd)
				}
			}()
			job, err := jq2.GetByEssence(&JobEssence{Cmd: cmd}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateComplete)
			So(job.EndTime, ShouldHappenOnOrAfter, job.StartTime.Add(9900*time.Millisecond))

			job2, err := jq2.GetByEssence(&JobEssence{Cmd: cmd2}, false, false)
			So(err, ShouldBeNil)
			So(job2, ShouldNotBeNil)
			So(job2.Cmd, ShouldEqual, cmd2)
			So(job2.State, ShouldEqual, JobStateLost)

			// now allow 2 secs for job3 to start and complete
			var job3 *Job
			for i := 0; i < 8; i++ {
				job3, err = jq2.GetByEssence(&JobEssence{Cmd: cmd3}, false, false)
				if job3 != nil && job3.State == JobStateComplete {
					break
				}
				<-time.After(250 * time.Millisecond)
			}
			So(err, ShouldBeNil)
			So(job3, ShouldNotBeNil)
			So(job3.Cmd, ShouldEqual, cmd3)
			So(job3.State, ShouldEqual, JobStateComplete)
			So(job3.StartTime, ShouldHappenAfter, job.EndTime)

			// for subsequent tests to work, we need to wait for the server to
			// really be gone
			killServer(jq, serverPid, serverCmd)
			So(waitUntilPidsAreGone(map[int]bool{serverPid: true}, 5), ShouldBeTrue)
		})

		Reset(func() {
			killServer(jq, serverPid, serverCmd)
		})
	})
}

func TestJobqueueBasics(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, standardReqs, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	var server *Server
	var token []byte
	var errs error
	Convey("Without the jobserver being up, clients can't connect and time out", t, func() {
		os.Remove(config.ManagerTokenFile)

		_, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
		So(err, ShouldNotBeNil)
		jqerr, ok := err.(Error)
		So(ok, ShouldBeTrue)
		So(jqerr.Err, ShouldEqual, ErrNoServer)

		jq, err := ConnectUsingConfig(ctx, "development", clientConnectTime)
		So(jq, ShouldBeNil)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "could not read token file")
	})

	Convey("Once the jobqueue server is up", t, func() {
		server, _, token, errs = serve(ctx, serverConfig)
		So(errs, ShouldBeNil)

		server.rc = serverRC // ReserveScheduled() only works if we have an rc

		Convey("You can connect to the server using config", func() {
			jq, err := ConnectUsingConfig(ctx, "development", clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)
		})

		Convey("You can connect to the server and add jobs and get back their IDs", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			req := &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}
			jobs = append(jobs, &Job{Cmd: "echo 1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: req, Retries: uint8(0), RepGroup: "test"})
			jobs = append(jobs, &Job{Cmd: "echo 2", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: req, Retries: uint8(0), RepGroup: "test"})
			ids, err := jq.AddAndReturnIDs(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(len(ids), ShouldEqual, 2)
			So(ids[0], ShouldEqual, "9a456dee1e351f82e3d562769c27d803")
			So(ids[1], ShouldEqual, "2bb7055e49e21ea85066899a5ba38d8e")
		})

		Convey("You can connect to the server and add jobs to the queue", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			So(jq.ServerInfo.Port, ShouldEqual, serverConfig.Port)
			So(jq.ServerInfo.PID, ShouldBeGreaterThan, 0)
			So(jq.ServerInfo.Deployment, ShouldEqual, "development")

			var jobs []*Job
			for i := 0; i < 10; i++ {
				pri := i
				if i == 7 {
					pri = 4
				} else if i == 4 {
					pri = 7
				}
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("test cmd %d", i), Cwd: "/fake/cwd", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1024, Time: 4 * time.Hour, Cores: 1}, Priority: uint8(pri), Retries: uint8(3), RepGroup: "manually_added"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 10)
			So(already, ShouldEqual, 0)

			Convey("You can't add the same jobs to the queue again", func() {
				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 0)
				So(already, ShouldEqual, 10)
			})

			Convey("You can get back jobs you've just added", func() {
				job, err := jq.GetByEssence(&JobEssence{Cmd: "test cmd 3"}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.Cmd, ShouldEqual, "test cmd 3")
				So(job.State, ShouldEqual, JobStateReady)

				job, err = jq.GetByEssence(&JobEssence{Cmd: "test cmd x"}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldBeNil)

				var jes []*JobEssence
				for i := 0; i < 10; i++ {
					jes = append(jes, &JobEssence{Cmd: fmt.Sprintf("test cmd %d", i)})
				}
				jobs, err = jq.GetByEssences(jes)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 10)
				for i, job := range jobs {
					So(job.Cmd, ShouldEqual, fmt.Sprintf("test cmd %d", i))
					So(job.State, ShouldEqual, JobStateReady)
				}

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 10)

				jobs, err = jq.GetByRepGroup("foo", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 0)
			})

			Convey("You can store their (fake) runtime stats and get recommendations", func() {
				// these are ignored by the learning system unless the job
				// failed due to running out of a resource
				for index, job := range jobs {
					job.PeakRAM = index + 1
					job.PeakDisk = int64(index + 2)
					job.StartTime = time.Now()
					job.EndTime = job.StartTime.Add(time.Duration(index+1) * time.Second)
					server.db.updateJobAfterExit(ctx, job, []byte{}, []byte{}, false)
				}
				<-time.After(200 * time.Millisecond)
				rmem, err := server.db.recommendedReqGroupMemory("fake_group")
				So(err, ShouldBeNil)
				So(rmem, ShouldEqual, 0)
				rdisk, err := server.db.recommendedReqGroupDisk("fake_group")
				So(err, ShouldBeNil)
				So(rdisk, ShouldEqual, 0)
				rtime, err := server.db.recommendedReqGroupTime("fake_group")
				So(err, ShouldBeNil)
				So(rtime, ShouldEqual, 0)

				for index, job := range jobs {
					job.PeakRAM = index + 1
					job.PeakDisk = int64(index + 2)
					job.StartTime = time.Now()
					job.EndTime = job.StartTime.Add(time.Duration(index+1) * time.Second)

					switch index {
					case 1, 2, 3:
						job.FailReason = FailReasonRAM
					case 4, 5, 6:
						job.FailReason = FailReasonDisk
					case 7, 8, 9:
						job.FailReason = FailReasonTime
					}

					server.db.updateJobAfterExit(ctx, job, []byte{}, []byte{}, false)
				}
				<-time.After(200 * time.Millisecond)
				rmem, err = server.db.recommendedReqGroupMemory("fake_group")
				So(err, ShouldBeNil)
				So(rmem, ShouldEqual, 100)
				rdisk, err = server.db.recommendedReqGroupDisk("fake_group")
				So(err, ShouldBeNil)
				So(rdisk, ShouldEqual, 100)
				rtime, err = server.db.recommendedReqGroupTime("fake_group")
				So(err, ShouldBeNil)
				So(rtime, ShouldEqual, 1800)

				for i := 11; i <= 100; i++ {
					job := &Job{Cmd: fmt.Sprintf("test cmd %d", i), Cwd: "/fake/cwd", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1024, Time: 4 * time.Hour, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"}
					job.PeakRAM = i * 100
					job.PeakDisk = int64(i * 200)
					job.StartTime = time.Now()
					job.EndTime = job.StartTime.Add(time.Duration(i*100) * time.Second)

					switch {
					case i < 40:
						job.FailReason = FailReasonRAM
					case i < 70:
						job.FailReason = FailReasonDisk
					default:
						job.FailReason = FailReasonTime
					}

					server.db.updateJobAfterExit(ctx, job, []byte{}, []byte{}, false)
				}
				<-time.After(500 * time.Millisecond)
				rmem, err = server.db.recommendedReqGroupMemory("fake_group")
				So(err, ShouldBeNil)
				So(rmem, ShouldEqual, 3400)
				rdisk, err = server.db.recommendedReqGroupDisk("fake_group")
				So(err, ShouldBeNil)
				So(rdisk, ShouldEqual, 12800)
				rtime, err = server.db.recommendedReqGroupTime("fake_group")
				So(err, ShouldBeNil)
				So(rtime, ShouldEqual, 10800)
			})

			Convey("You can reserve jobs from the queue in the correct order", func() {
				for i := 9; i >= 0; i-- {
					jid := i
					if i == 7 {
						jid = 4
					} else if i == 4 {
						jid = 7
					}
					job, err := jq.ReserveScheduled(50*time.Millisecond, "1024:240:1:0")
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, fmt.Sprintf("test cmd %d", jid))
					So(job.EnvC, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateReserved)
				}

				Convey("Reserving when all have been reserved returns nil", func() {
					job, err := jq.ReserveScheduled(50*time.Millisecond, "1024:240:1:0")
					So(err, ShouldBeNil)
					So(job, ShouldBeNil)

					Convey("Adding one while waiting on a Reserve will return the new job", func() {
						worked := make(chan bool)
						go func() {
							job, err := jq.ReserveScheduled(1000*time.Millisecond, "1024:300:1:0")
							if err != nil {
								worked <- false
								return
							}
							if job == nil {
								worked <- false
								return
							}
							if job.Cmd == "new" {
								worked <- true
								return
							}
							worked <- false
						}()

						ok := make(chan bool)
						go func() {
							ticker := time.NewTicker(100 * time.Millisecond)
							ticks := 0
							for {
								select {
								case <-ticker.C:
									ticks++
									if ticks == 2 {
										jobs = append(jobs, &Job{Cmd: "new", Cwd: "/fake/cwd", ReqGroup: "add_group", Requirements: &jqs.Requirements{RAM: 1024, Time: 5 * time.Hour, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
										gojq, errc := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
										if errc != nil {
											fmt.Printf("Connect failed: %s\n", errc)
										}
										defer disconnect(gojq)
										_, _, erra := gojq.Add(jobs, envVars, true)
										if errc != nil {
											fmt.Printf("Add failed: %s\n", erra)
										}
									}
									continue
								case w := <-worked:
									ticker.Stop()
									if w && ticks <= 8 {
										ok <- true
									}
									ok <- false
									return
								}
							}
						}()

						<-time.After(2 * time.Second)
						So(<-ok, ShouldBeTrue)
					})
				})
			})

			if runtime.NumCPU() >= 2 {
				Convey("You can subsequently add more jobs", func() {
					for i := 10; i < 20; i++ {
						jobs = append(jobs, &Job{Cmd: fmt.Sprintf("test cmd %d", i), Cwd: "/fake/cwd", ReqGroup: "new_group", Requirements: &jqs.Requirements{RAM: 2048, Time: 1 * time.Hour, Cores: 2}, Retries: uint8(3), RepGroup: "manually_added"})
					}
					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 10)
					So(already, ShouldEqual, 10)

					Convey("You can reserve jobs for a particular scheduler group", func() {
						for i := 10; i < 20; i++ {
							job, err := jq.ReserveScheduled(20*time.Millisecond, "2048:60:2:0")
							So(err, ShouldBeNil)
							So(job, ShouldNotBeNil)
							So(job.Cmd, ShouldEqual, fmt.Sprintf("test cmd %d", i))
						}
						job, err := jq.ReserveScheduled(10*time.Millisecond, "2048:60:2:0")
						So(err, ShouldBeNil)
						So(job, ShouldBeNil)

						for i := 9; i >= 0; i-- {
							jid := i
							if i == 7 {
								jid = 4
							} else if i == 4 {
								jid = 7
							}
							job, err = jq.ReserveScheduled(10*time.Millisecond, "1024:240:1:0")
							So(err, ShouldBeNil)
							So(job, ShouldNotBeNil)
							So(job.Cmd, ShouldEqual, fmt.Sprintf("test cmd %d", jid))
						}
						job, err = jq.ReserveScheduled(10*time.Millisecond, "1024:240:1:0")
						So(err, ShouldBeNil)
						So(job, ShouldBeNil)
					})
				})
			}

			Convey("You can add more jobs, but without any environment variables", func() {
				server.racmutex.Lock()
				server.rc = ""
				server.racmutex.Unlock()
				os.Setenv("wr_jobqueue_test_no_envvar", "a")
				inserts, already, err := jq.Add([]*Job{{Cmd: "echo $wr_jobqueue_test_no_envvar && false", Cwd: "/tmp", ReqGroup: "new_group", Requirements: standardReqs, Priority: uint8(100), RepGroup: "noenvvar"}}, []string{}, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.RepGroup, ShouldEqual, "noenvvar")

				env, err := job.Env()
				So(err, ShouldBeNil)
				So(env, ShouldNotBeEmpty)

				os.Setenv("wr_jobqueue_test_no_envvar", "b")
				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldNotBeNil)
				So(job.FailReason, ShouldEqual, FailReasonExit)
				stdout, err := job.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "b")

				// make sure the stdout is actually stored in the database
				retrieved, err := jq.GetByEssence(job.ToEssense(), true, false)
				So(err, ShouldBeNil)
				stdout, err = retrieved.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "b")

				// by comparison, compare normal behaviour, where the initial
				// value of the envvar gets used for the job
				os.Setenv("wr_jobqueue_test_no_envvar", "a")
				inserts, already, err = jq.Add([]*Job{{Cmd: "echo $wr_jobqueue_test_no_envvar && false && false", Cwd: "/tmp", ReqGroup: "new_group", Requirements: standardReqs, Priority: uint8(101), RepGroup: "withenvvar"}}, os.Environ(), true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				job, err = jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.RepGroup, ShouldEqual, "withenvvar")

				env, err = job.Env()
				So(err, ShouldBeNil)
				So(env, ShouldNotBeEmpty)

				os.Setenv("wr_jobqueue_test_no_envvar", "b")
				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldNotBeNil)
				So(job.FailReason, ShouldEqual, FailReasonExit)
				stdout, err = job.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "a")
			})

			Convey("You can add more jobs, overriding certain environment variables", func() {
				server.racmutex.Lock()
				server.rc = ""
				server.racmutex.Unlock()
				os.Setenv("wr_jobqueue_test_no_envvar", "a")
				compressed, err := jq.CompressEnv([]string{"wr_jobqueue_test_no_envvar=c", "wr_jobqueue_test_no_envvar2=d"})
				So(err, ShouldBeNil)
				inserts, already, err := jq.Add([]*Job{{
					Cmd:          "echo $wr_jobqueue_test_no_envvar && echo $wr_jobqueue_test_no_envvar2 && false",
					Cwd:          "/tmp",
					RepGroup:     "noenvvar",
					ReqGroup:     "new_group",
					Requirements: standardReqs,
					Priority:     uint8(100),
					Retries:      uint8(0),
					EnvOverride:  compressed,
				}}, []string{}, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.RepGroup, ShouldEqual, "noenvvar")

				env, err := job.Env()
				So(err, ShouldBeNil)
				So(env, ShouldNotBeEmpty)

				os.Setenv("wr_jobqueue_test_no_envvar", "b")
				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldNotBeNil)
				So(job.FailReason, ShouldEqual, FailReasonExit)
				stdout, err := job.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "c\nd")
			})

			Convey("You can stop the server by sending it a SIGTERM or SIGINT", func() {
				err := jq.Disconnect()
				So(err, ShouldBeNil)

				errk := syscall.Kill(os.Getpid(), syscall.SIGTERM)
				if errk != nil {
					fmt.Printf("failed to send SIGTERM: %s\n", errk)
				}
				<-time.After(serverShutDownTime())
				_, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldNotBeNil)
				jqerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(jqerr.Err, ShouldEqual, ErrNoServer)

				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)

				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				err = jq.Disconnect()
				So(err, ShouldBeNil)

				errk = syscall.Kill(os.Getpid(), syscall.SIGINT)
				if errk != nil {
					fmt.Printf("failed to send SIGINT: %s\n", errk)
				}
				<-time.After(serverShutDownTime())
				_, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldNotBeNil)
				jqerr, ok = err.(Error)
				So(ok, ShouldBeTrue)
				So(jqerr.Err, ShouldEqual, ErrNoServer)
			})

			Convey("You get a nice error if you send the server junk", func() {
				_, err := jq.request(&clientRequest{Method: "junk"})
				So(err, ShouldNotBeNil)
				jqerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(jqerr.Err, ShouldEqual, ErrUnknownCommand)
				disconnect(jq)
			})
		})

		Reset(func() {
			server.Stop(ctx, true)
		})
	})

	if server != nil {
		server.Stop(ctx, true)
	}
}

func TestJobqueueMedium(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, standardReqs, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	// start these tests anew because I don't want to mess with the timings in
	// the above tests
	Convey("Once a new jobqueue server is up", t, func() {
		ServerItemTTR = 200 * time.Millisecond
		ClientTouchInterval = 50 * time.Millisecond
		server, _, token, errs := serve(ctx, serverConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		Convey("You can connect, and add some real jobs", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)
			jq2, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq2)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "sleep 0.1 && true", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(2), RepGroup: "manually_added"})
			jobs = append(jobs, &Job{Cmd: "sleep 0.1 && false", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(2), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			Convey("You can't execute a job without reserving it", func() {
				err := jq.Execute(ctx, jobs[0], config.RunnerExecShell)
				So(err, ShouldNotBeNil)
				jqerr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(jqerr.Err, ShouldEqual, ErrMustReserve)
				disconnect(jq)
			})

			Convey("Once reserved you can execute jobs, and other clients see the correct state on gets", func() {
				// job that succeeds, no std out
				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job.Cmd, ShouldEqual, "sleep 0.1 && true")
				So(job.State, ShouldEqual, JobStateReserved)
				So(job.Attempts, ShouldEqual, 0)
				So(job.UntilBuried, ShouldEqual, 3)

				job2, err := jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && true"}, false, false)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.Cmd, ShouldEqual, "sleep 0.1 && true")
				So(job2.State, ShouldEqual, JobStateReserved)

				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldBeNil)
				So(job.State, ShouldEqual, JobStateComplete)
				So(job.Exited, ShouldBeTrue)
				So(job.Exitcode, ShouldEqual, 0)
				So(job.PeakRAM, ShouldBeGreaterThan, 0)
				So(job.PeakDisk, ShouldEqual, 0)
				So(job.Pid, ShouldBeGreaterThan, 0)
				host, err := os.Hostname()
				So(err, ShouldBeNil)
				So(job.Host, ShouldEqual, host)
				So(job.WallTime(), ShouldBeGreaterThanOrEqualTo, 1*time.Millisecond)
				So(job.CPUtime, ShouldBeGreaterThanOrEqualTo, 0*time.Millisecond)
				So(job.Attempts, ShouldEqual, 1)
				So(job.UntilBuried, ShouldEqual, 3)
				stdout, err := job.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "")
				stderr, err := job.StdErr()
				So(err, ShouldBeNil)
				So(stderr, ShouldEqual, "")
				actualCwd := job.ActualCwd
				So(actualCwd, ShouldStartWith, filepath.Join("/tmp", "jobqueue_cwd", "7", "4", "7", "27e23009c78b126f274aa64416f30"))
				So(actualCwd, ShouldEndWith, "cwd")

				job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && true"}, false, false)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.State, ShouldEqual, JobStateComplete)
				So(job2.Exited, ShouldBeTrue)
				So(job2.Exitcode, ShouldEqual, 0)
				So(job2.PeakRAM, ShouldEqual, job.PeakRAM)
				So(job2.Pid, ShouldEqual, job.Pid)
				So(job2.Host, ShouldEqual, host)
				So(job2.WallTime(), ShouldBeLessThanOrEqualTo, job.WallTime())
				So(job2.WallTime(), ShouldBeGreaterThanOrEqualTo, 1*time.Millisecond)
				So(job2.CPUtime, ShouldEqual, job.CPUtime)
				So(job2.Attempts, ShouldEqual, 1)
				So(job2.ActualCwd, ShouldEqual, actualCwd)

				// job that fails, no std out
				job, err = jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job.Cmd, ShouldEqual, "sleep 0.1 && false")
				So(job.State, ShouldEqual, JobStateReserved)
				So(job.Attempts, ShouldEqual, 0)
				So(job.UntilBuried, ShouldEqual, 3)

				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "command [sleep 0.1 && false] exited with code 1, which may be a temporary issue, so it will be tried again")
				So(job.State, ShouldEqual, JobStateDelayed)
				So(job.Exited, ShouldBeTrue)
				So(job.Exitcode, ShouldEqual, 1)
				So(job.PeakRAM, ShouldBeGreaterThan, 0)
				So(job.Pid, ShouldBeGreaterThan, 0)
				So(job.Host, ShouldEqual, host)
				So(job.WallTime(), ShouldBeGreaterThanOrEqualTo, 1*time.Millisecond)
				So(job.CPUtime, ShouldBeGreaterThanOrEqualTo, 0*time.Millisecond)
				So(job.Attempts, ShouldEqual, 1)
				So(job.UntilBuried, ShouldEqual, 2)
				stdout, err = job.StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, "")
				stderr, err = job.StdErr()
				So(err, ShouldBeNil)
				So(stderr, ShouldEqual, "")

				job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.State, ShouldEqual, JobStateDelayed)
				So(job2.Exited, ShouldBeTrue)
				So(job2.Exitcode, ShouldEqual, 1)
				So(job2.PeakRAM, ShouldEqual, job.PeakRAM)
				So(job2.Pid, ShouldEqual, job.Pid)
				So(job2.Host, ShouldEqual, host)
				So(job2.WallTime(), ShouldBeLessThanOrEqualTo, job.WallTime())
				So(job2.WallTime(), ShouldBeGreaterThanOrEqualTo, 1*time.Millisecond)
				So(job2.CPUtime, ShouldEqual, job.CPUtime)
				So(job2.Attempts, ShouldEqual, 1)

				Convey("Both current and archived jobs can be retrieved with GetByRepGroup", func() {
					jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
					So(err, ShouldBeNil)
					So(len(jobs), ShouldEqual, 2)

					Convey("But only current jobs are retrieved with GetIncomplete", func() {
						jobs, err = jq.GetIncomplete(0, "", false, false)
						So(err, ShouldBeNil)
						So(len(jobs), ShouldEqual, 1)
						So(jobs[0].Cmd, ShouldEqual, "sleep 0.1 && false")
						//*** should probably have a better test, where there are incomplete jobs in each of the sub queues
					})
				})

				Convey("A temp failed job is reservable after a delay", func() {
					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job, ShouldBeNil)
					job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateDelayed)

					<-time.After(100 * time.Millisecond)
					job, err = jq.Reserve(20 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job, ShouldNotBeNil)
					So(job.Cmd, ShouldEqual, "sleep 0.1 && false")
					So(job.State, ShouldEqual, JobStateReserved)
					So(job.Attempts, ShouldEqual, 1)
					So(job.UntilBuried, ShouldEqual, 2)
					job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateReserved)

					Convey("After 2 retries (3 attempts) it gets buried", func() {
						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldNotBeNil)
						So(job.State, ShouldEqual, JobStateDelayed)
						So(job.Exited, ShouldBeTrue)
						So(job.Exitcode, ShouldEqual, 1)
						So(job.Attempts, ShouldEqual, 2)
						So(job.UntilBuried, ShouldEqual, 1)

						<-time.After(210 * time.Millisecond)
						job, err = jq.Reserve(5 * time.Millisecond)
						So(err, ShouldBeNil)
						So(job.Cmd, ShouldEqual, "sleep 0.1 && false")
						So(job.State, ShouldEqual, JobStateReserved)
						So(job.Attempts, ShouldEqual, 2)
						So(job.UntilBuried, ShouldEqual, 1)

						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldNotBeNil)
						So(job.State, ShouldEqual, JobStateBuried)
						So(job.Exited, ShouldBeTrue)
						So(job.Exitcode, ShouldEqual, 1)
						So(job.Attempts, ShouldEqual, 3)
						So(job.UntilBuried, ShouldEqual, 0)

						<-time.After(210 * time.Millisecond)
						job, err = jq.Reserve(5 * time.Millisecond)
						So(err, ShouldBeNil)
						So(job, ShouldBeNil)

						Convey("Once buried it can be kicked back to ready state and be reserved again", func() {
							job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
							So(err, ShouldBeNil)
							So(job2, ShouldNotBeNil)
							So(job2.State, ShouldEqual, JobStateBuried)

							kicked, err := jq.Kick([]*JobEssence{{Cmd: "sleep 0.1 && false"}})
							So(err, ShouldBeNil)
							So(kicked, ShouldEqual, 1)

							job, err = jq.Reserve(5 * time.Millisecond)
							So(err, ShouldBeNil)
							So(job, ShouldNotBeNil)
							So(job.Cmd, ShouldEqual, "sleep 0.1 && false")
							So(job.State, ShouldEqual, JobStateReserved)
							So(job.Attempts, ShouldEqual, 3)
							So(job.UntilBuried, ShouldEqual, 3)

							job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
							So(err, ShouldBeNil)
							So(job2, ShouldNotBeNil)
							So(job2.State, ShouldEqual, JobStateReserved)
							So(job2.Attempts, ShouldEqual, 3)
							So(job2.UntilBuried, ShouldEqual, 3)

							Convey("If you do nothing with a reserved job, it auto reverts back to delayed", func() {
								<-time.After(210 * time.Millisecond)
								job2, err = jq2.GetByEssence(&JobEssence{Cmd: "sleep 0.1 && false"}, false, false)
								So(err, ShouldBeNil)
								So(job2, ShouldNotBeNil)
								So(job2.State, ShouldEqual, JobStateDelayed)
								So(job2.Attempts, ShouldEqual, 3)
								So(job2.UntilBuried, ShouldEqual, 3)
							})
						})
					})
				})
			})

			Convey("Jobs can be deleted in any state except running", func() {
				for _, added := range jobs {
					job, err := jq.GetByEssence(&JobEssence{Cmd: added.Cmd}, false, false)
					So(err, ShouldBeNil)
					So(job, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateReady)

					deleted, err := jq.Delete([]*JobEssence{{Cmd: added.Cmd}})
					So(err, ShouldBeNil)
					So(deleted, ShouldEqual, 1)

					job, err = jq.GetByEssence(&JobEssence{Cmd: added.Cmd}, false, false)
					So(err, ShouldBeNil)
					So(job, ShouldBeNil)

					//*** add tests to show this doesn't work if running...
				}
				job, err := jq.Reserve(5 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldBeNil)

				Convey("Cmds with pipes in them are handled correctly", func() {
					jobs = nil
					jobs = append(jobs, &Job{Cmd: "sleep 0.1 && true | true", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "should_pass"})
					jobs = append(jobs, &Job{Cmd: "sleep 0.1 && true | false | true", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "should_fail"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 2)

					// pipe job that succeeds
					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "sleep 0.1 && true | true")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)
					So(job.FailReason, ShouldEqual, "")

					// pipe job that fails in the middle
					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "sleep 0.1 && true | false | true")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "command [sleep 0.1 && true | false | true] exited with code 1, which may be a temporary issue, so it will be tried again") //*** can fail with a receive time out; why?!
					So(job.State, ShouldEqual, JobStateDelayed)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					So(job.FailReason, ShouldEqual, FailReasonExit)
				})

				Convey("Invalid commands are immediately buried", func() {
					jobs = nil
					jobs = append(jobs, &Job{Cmd: "awesjnalakjf --foo", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "should_fail"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					// job that fails because of non-existent exe
					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "awesjnalakjf --foo")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "command [awesjnalakjf --foo] exited with code 127 (command not found), which seems permanent, so it has been buried")
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 127)
					So(job.FailReason, ShouldEqual, FailReasonCFound)

					job2, err := jq2.GetByEssence(&JobEssence{Cmd: "awesjnalakjf --foo"}, false, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateBuried)
					So(job2.FailReason, ShouldEqual, FailReasonCFound)

					//*** how to test the other bury cases of invalid exit code
					// and permission problems on the exe?
				})

				Convey("If a job uses more memory than expected it is not killed, but we recommend more next time", func() {
					jobs = nil
					cmd := "perl -e '@a; for (1..3) { push(@a, q[a] x 50000000); sleep(1) }'"
					jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "highmem", Requirements: standardReqs, Retries: uint8(0), RepGroup: "too_much_mem"})
					RecMBRound = 1
					defer func() {
						RecMBRound = 100 // revert back to normal
					}()
					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)
					So(already, ShouldEqual, 0)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd)
					So(job.State, ShouldEqual, JobStateReserved)
					So(job.Requirements.RAM, ShouldEqual, 10)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)

					cmd2 := "echo another high mem job"
					jobs = append(jobs, &Job{Cmd: cmd2, Cwd: "/tmp", ReqGroup: "highmem", Requirements: standardReqs, Retries: uint8(0), RepGroup: "too_much_mem"})
					inserts, already, err = jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)
					So(already, ShouldEqual, 1)

					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd2)
					So(job.State, ShouldEqual, JobStateReserved)
					So(job.Requirements.RAM, ShouldBeGreaterThanOrEqualTo, 100)
					err = jq.Release(job, &JobEndState{}, "")
					So(err, ShouldBeNil)

					deleted, errd := jq.Delete([]*JobEssence{{Cmd: cmd}})
					So(errd, ShouldBeNil)
					So(deleted, ShouldEqual, 0)
					deleted, errd = jq.Delete([]*JobEssence{{Cmd: cmd2}})
					So(errd, ShouldBeNil)
					So(deleted, ShouldEqual, 1)
				})

				Convey("Jobs that fork and change processgroup can still be fully killed", func() {
					jobs = nil
					tmpdir, err := os.MkdirTemp("", "wr_kill_test")
					So(err, ShouldBeNil)
					defer os.RemoveAll(tmpdir)

					cmd := fmt.Sprintf("perl -Mstrict -we 'open(OUT, qq[>%s/$$]); my $pid = fork; if ($pid == 0) { setpgrp; my $subpid = fork; if ($subpid == 0) { sleep(60); exit 0; } open(OUT, qq[>%s/$subpid]); waitpid $subpid, 0; exit 0; }  open(OUT, qq[>%s/$pid]); sleep(30); waitpid $pid, 0'", tmpdir, tmpdir, tmpdir)
					jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(0), RepGroup: "forker"})
					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)
					So(already, ShouldEqual, 0)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd)
					So(job.State, ShouldEqual, JobStateReserved)

					ich := make(chan int, 1)
					ech := make(chan error, 1)
					go func() {
						<-time.After(1 * time.Second)
						i, errk := jq.Kill([]*JobEssence{job.ToEssense()})
						ich <- i
						ech <- errk
					}()

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					var jqerr Error
					if !errors.As(err, &jqerr) {
						fmt.Printf("\ngot err %+v\n", err)
					}
					So(jqerr, ShouldNotBeNil)
					So(jqerr.Err, ShouldEqual, FailReasonKilled)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, -1)
					So(job.FailReason, ShouldEqual, FailReasonKilled)

					i := <-ich
					So(i, ShouldEqual, 1)
					err = <-ech
					So(err, ShouldBeNil)

					files, err := os.ReadDir(tmpdir)
					So(err, ShouldBeNil)
					count := 0
					for _, file := range files {
						if file.IsDir() {
							continue
						}
						count++
						pid, err := strconv.Atoi(file.Name())
						So(err, ShouldBeNil)
						process, err := os.FindProcess(pid)
						So(err, ShouldBeNil)
						err = process.Signal(syscall.Signal(0))
						So(err, ShouldNotBeNil)
						So(err.Error(), ShouldContainSubstring, "process already finished")
					}
					So(count, ShouldEqual, 3)

					deleted, errd := jq.Delete([]*JobEssence{{Cmd: cmd}})
					So(errd, ShouldBeNil)
					So(deleted, ShouldEqual, 1)
				})

				Convey("Jobs that fork and change processgroup have correct memory usage reported", func() {
					jobs = nil
					cmd := `perl -Mstrict -we 'my $pid = fork; if ($pid == 0) { setpgrp; my $subpid = fork; if ($subpid == 0) { my @a; for (1..100) { push(@a, q[a] x 10000000); } exit 0; } waitpid $subpid, 0; exit 0; } my @b; for (1..100) { push(@b, q[b] x 1000000); } waitpid $pid, 0'`
					jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(0), RepGroup: "forker"})
					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)
					So(already, ShouldEqual, 0)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd)
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)
					So(job.PeakRAM, ShouldBeGreaterThan, 500)
				})

				if runtime.NumCPU() >= 2 {
					Convey("Jobs that fork and change processgroup have correct CPU time reported", func() {
						jobs = nil
						cmd := `perl -Mstrict -we 'my $pid = fork; if ($pid == 0) { setpgrp; my $subpid = fork; if ($subpid == 0) { my $a = 2; for (1..10000000) { $a *= $a } exit 0; } waitpid $subpid, 0; exit 0; } my $b = 2; for (1..10000000) { $b *= $b } waitpid $pid, 0'`
						jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(0), RepGroup: "forker"})
						inserts, already, err := jq.Add(jobs, envVars, true)
						So(err, ShouldBeNil)
						So(inserts, ShouldEqual, 1)
						So(already, ShouldEqual, 0)

						job, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(job.Cmd, ShouldEqual, cmd)
						So(job.State, ShouldEqual, JobStateReserved)

						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldBeNil)
						So(job.State, ShouldEqual, JobStateComplete)
						So(job.Exited, ShouldBeTrue)
						So(job.Exitcode, ShouldEqual, 0)
						So(job.CPUtime, ShouldBeGreaterThanOrEqualTo, job.WallTime()+(job.WallTime()/4))
					})
				}

				Convey("The stdout/err of jobs is only kept for failed jobs, and cwd&TMPDIR&HOME get set appropriately", func() {
					jobs = nil
					baseDir, err := os.MkdirTemp("", "wr_jobqueue_test_runner_dir_")
					So(err, ShouldBeNil)
					defer os.RemoveAll(baseDir)
					tmpDir := filepath.Join(baseDir, "jobqueue tmpdir") // testing that it works with spaces in the name
					err = os.Mkdir(tmpDir, os.ModePerm)
					So(err, ShouldBeNil)
					jobs = append(jobs, &Job{Cmd: "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; warn File::Spec->tmpdir, qq[\\n]'", Cwd: tmpDir, CwdMatters: true, ChangeHome: true, ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_pass"})
					jobs = append(jobs, &Job{Cmd: "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; die File::Spec->tmpdir, qq[\\n]'", Cwd: tmpDir, CwdMatters: false, ChangeHome: true, ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_fail"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 2)

					// job that outputs to stdout and stderr but succeeds
					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; warn File::Spec->tmpdir, qq[\\n]'")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)
					stdout, err := job.StdOut()
					So(err, ShouldBeNil)
					home, herr := os.UserHomeDir()
					So(herr, ShouldBeNil)
					So(stdout, ShouldEqual, tmpDir+"-"+home)
					stderr, err := job.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, os.TempDir())

					job2, err := jq2.GetByEssence(&JobEssence{Cmd: "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; warn File::Spec->tmpdir, qq[\\n]'", Cwd: tmpDir}, true, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateComplete)
					stdout, err = job2.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, "")
					stderr, err = job2.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, "")

					// job that outputs to stdout and stderr and fails
					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; die File::Spec->tmpdir, qq[\\n]'")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 255)
					So(job.FailReason, ShouldEqual, FailReasonExit)
					stdout, err = job.StdOut()
					So(err, ShouldBeNil)
					actualCwd := job.ActualCwd
					So(actualCwd, ShouldStartWith, filepath.Join(tmpDir, "jobqueue_cwd", "d", "4", "1", "7364d743329da784e74f2d69d438d"))
					So(actualCwd, ShouldEndWith, "cwd")
					So(stdout, ShouldEqual, actualCwd+"-"+actualCwd)
					stderr, err = job.StdErr()
					So(err, ShouldBeNil)
					tmpDir = actualCwd[:len(actualCwd)-3] + "tmp"
					So(stderr, ShouldEqual, tmpDir)

					job2, err = jq2.GetByEssence(&JobEssence{Cmd: "perl -MCwd -MFile::Spec -e '$cwd = getcwd(); print $cwd, qq[-], $ENV{HOME}, qq[\\n]; die File::Spec->tmpdir, qq[\\n]'"}, true, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateBuried)
					So(job2.FailReason, ShouldEqual, FailReasonExit)
					stdout, err = job2.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, actualCwd+"-"+actualCwd)
					stderr, err = job2.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, tmpDir)
				})

				Convey("The stdout/err of jobs is limited in size", func() {
					jobs = nil
					jobs = append(jobs, &Job{Cmd: "perl -e 'for (1..60) { print $_ x 130, qq[p\\n]; warn $_ x 130, qq[w\\n] } die'", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_fail"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					// job that outputs tons of stdout and stderr and fails
					expectedout := ""
					expectederr := ""
					for i := 1; i <= 60; i++ {
						if i > 21 && i < 46 {
							continue
						}
						if i == 21 {
							expectedout += "21212121212121212121212121\n... omitting 6358 bytes ...\n45454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545p\n"
							expectederr += "21212121212121212121212121\n... omitting 6377 bytes ...\n5454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545454545w\n"
						} else {
							s := strconv.Itoa(i)
							for j := 1; j <= 130; j++ {
								expectedout += s
								expectederr += s
							}
							expectedout += "p\n"
							expectederr += "w\n"
						}
					}
					expectederr += "Died at -e line 1."
					expectedout = strings.TrimSpace(expectedout)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "perl -e 'for (1..60) { print $_ x 130, qq[p\\n]; warn $_ x 130, qq[w\\n] } die'")
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 255)
					stdout, err := job.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, expectedout)
					stderr, err := job.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, expectederr)

					job2, err := jq2.GetByEssence(&JobEssence{Cmd: "perl -e 'for (1..60) { print $_ x 130, qq[p\\n]; warn $_ x 130, qq[w\\n] } die'"}, true, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateBuried)
					stdout, err = job2.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, expectedout)
					stderr, err = job2.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, expectederr)

					Convey("If you don't ask for stdout, you don't get it", func() {
						job2, err = jq2.GetByEssence(&JobEssence{Cmd: "perl -e 'for (1..60) { print $_ x 130, qq[p\\n]; warn $_ x 130, qq[w\\n] } die'"}, false, false)
						So(err, ShouldBeNil)
						So(job2, ShouldNotBeNil)
						So(job2.State, ShouldEqual, JobStateBuried)
						stdout, err = job2.StdOut()
						So(err, ShouldBeNil)
						So(stdout, ShouldEqual, "")
						stderr, err = job2.StdErr()
						So(err, ShouldBeNil)
						So(stderr, ShouldEqual, "")
					})
				})

				Convey("The stdout/err of jobs is filtered for \\r blocks", func() {
					jobs = nil
					progressCmd := "perl -e '$|++; print qq[a\nb\n\nprogress: 98%\r]; for (99..100) { print qq[progress: $_%\r]; sleep(1); } print qq[\n\nc\n]; exit(1)'"
					jobs = append(jobs, &Job{Cmd: progressCmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_fail"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					expectedout := "a\nb\n\nprogress: 98%\n[...]\nprogress: 100%\n\nc\n"
					expectedout = strings.TrimSpace(expectedout)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, progressCmd)
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					stdout, err := job.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, expectedout)
					stderr, err := job.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, "")

					jobs = nil
					progressCmd = "perl -e '$|++; print qq[a\nb\n\n]; for (99..100) { print qq[progress: $_%\r]; sleep(1); } print qq[\n\nc\n]; exit(1)'"
					jobs = append(jobs, &Job{Cmd: progressCmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_fail"})
					inserts, _, err = jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					expectedout = "a\nb\n\nprogress: 99%\nprogress: 100%\n\nc\n"
					expectedout = strings.TrimSpace(expectedout)

					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, progressCmd)
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					stdout, err = job.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldEqual, expectedout)
					stderr, err = job.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldEqual, "")
				})

				Convey("Jobs with long lines of stderr do not cause execution to hang", func() {
					jobs = nil
					bigerrCmd := `perl -e 'for (1..10) { for (1..65536) { print STDERR qq[e] } print STDERR qq[\n] }' && false`
					jobs = append(jobs, &Job{Cmd: bigerrCmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "bigerr"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, bigerrCmd)
					So(job.State, ShouldEqual, JobStateReserved)

					// wait for the job to finish executing
					done := make(chan bool, 1)
					go func() {
						go execute(ctx, jq, job, config.RunnerExecShell, true)

						limit := time.After(10 * time.Second)
						ticker := time.NewTicker(500 * time.Millisecond)
						for {
							select {
							case <-ticker.C:
								jobs, err = jq.GetByRepGroup("bigerr", false, 0, JobStateBuried, false, false)
								if err != nil {
									continue
								}
								if len(jobs) == 1 {
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
					So(<-done, ShouldBeTrue)

					job, err = jq.GetByEssence(&JobEssence{Cmd: bigerrCmd}, true, false)
					So(err, ShouldBeNil)
					So(job, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					stdout, err := job.StdOut()
					So(err, ShouldBeNil)
					So(stdout, ShouldBeBlank)
					stderr, err := job.StdErr()
					So(err, ShouldBeNil)
					So(stderr, ShouldStartWith, "eeeeeeeeeeeeeeeeeeeeeeeeeeee")
					So(stderr, ShouldContainSubstring, "... omitting 647178 bytes ...")
					So(stderr, ShouldEndWith, "eeeeeeeeeeeeeeeeeeeeeeeeeeee")
				})

				Convey("Job behaviours trigger correctly", func() {
					jobs = nil
					cwd, err := os.MkdirTemp("", "wr_jobqueue_test_runner_dir_")
					So(err, ShouldBeNil)
					defer os.RemoveAll(cwd)
					b1 := &Behaviour{When: OnSuccess, Do: CleanupAll}
					b2 := &Behaviour{When: OnFailure, Do: Run, Arg: "touch foo"}
					bs := Behaviours{b1, b2}
					b3 := &Behaviour{When: OnFailure, Do: Remove}
					bs2 := Behaviours{b3}
					jobs = append(jobs, &Job{Cmd: "touch bar", Cwd: cwd, ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_pass", Behaviours: bs})
					jobs = append(jobs, &Job{Cmd: "touch bar && false", Cwd: cwd, ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_fail", Behaviours: bs})
					jobs = append(jobs, &Job{Cmd: "touch car && false", Cwd: cwd, ReqGroup: "fake_group", Requirements: standardReqs, RepGroup: "should_delete", Behaviours: bs2})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 3)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "touch bar")
					So(job.State, ShouldEqual, JobStateReserved)
					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)

					actualCwd := job.ActualCwd
					So(actualCwd, ShouldStartWith, filepath.Join(cwd, "jobqueue_cwd", "6", "0", "8", "3ab5943a1918a9774e4644acb36f6"))
					So(actualCwd, ShouldEndWith, "cwd")
					_, err = os.Stat(filepath.Join(actualCwd, "bar"))
					So(err, ShouldNotBeNil)
					_, err = os.Stat(filepath.Join(actualCwd, "foo"))
					So(err, ShouldNotBeNil)
					entries, err := os.ReadDir(cwd)
					So(err, ShouldBeNil)
					So(len(entries), ShouldEqual, 0)

					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "touch bar && false")
					So(job.State, ShouldEqual, JobStateReserved)
					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					So(job.FailReason, ShouldEqual, FailReasonExit)

					actualCwd = job.ActualCwd
					So(actualCwd, ShouldStartWith, filepath.Join(cwd, "jobqueue_cwd", "4", "4", "a", "758484033bddc46a51d3ec7517f2c"))
					So(actualCwd, ShouldEndWith, "cwd")
					_, err = os.Stat(filepath.Join(actualCwd, "bar"))
					So(err, ShouldBeNil)
					_, err = os.Stat(filepath.Join(actualCwd, "foo"))
					So(err, ShouldBeNil)
					entries, err = os.ReadDir(cwd)
					So(err, ShouldBeNil)
					So(len(entries), ShouldEqual, 1)
					So(entries[0].Name(), ShouldEqual, "jobqueue_cwd")

					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, "touch car && false")
					So(job.State, ShouldEqual, JobStateReserved)
					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldNotBeNil)
					So(job.State, ShouldEqual, JobStateBuried)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 1)
					So(job.FailReason, ShouldEqual, FailReasonExit)

					<-time.After(100 * time.Millisecond)
					jobs, err = jq.GetByRepGroup("should_fail", false, 0, JobStateBuried, false, false)
					So(err, ShouldBeNil)
					So(len(jobs), ShouldEqual, 1)
					jobs, err = jq.GetByRepGroup("should_delete", false, 0, JobStateBuried, false, false)
					So(err, ShouldBeNil)
					So(len(jobs), ShouldEqual, 0)
				})

				Convey("Jobs that take longer than the ttr can execute successfully, even if clienttouchinterval is > ttr", func() {
					jobs = nil
					cmd := "perl -e 'for (1..3) { sleep(1) }'"
					jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "should_pass"})
					inserts, _, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd)
					So(job.State, ShouldEqual, JobStateReserved)

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
					So(job.State, ShouldEqual, JobStateComplete)
					So(job.Exited, ShouldBeTrue)
					So(job.Exitcode, ShouldEqual, 0)

					job2, err := jq2.GetByEssence(&JobEssence{Cmd: cmd}, true, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)
					So(job2.State, ShouldEqual, JobStateComplete)

					// same again, but we'll alter the clienttouchinterval to be > ttr
					ClientTouchInterval = 500 * time.Millisecond
					inserts, _, err = jq.Add(jobs, envVars, false)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 1)

					job, err = jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job.Cmd, ShouldEqual, cmd)
					So(job.State, ShouldEqual, JobStateReserved)

					lostCh := make(chan bool)
					go func() {
						<-time.After(300 * time.Millisecond)
						// after ttr but before first touch, it becomes lost
						job2f, errf := jq2.GetByEssence(&JobEssence{Cmd: cmd}, true, false)
						if errf != nil || job2f == nil || job2f.State != JobStateLost || job2f.FailReason != FailReasonLost || job2f.Exited {
							lostCh <- false
						}

						<-time.After(250 * time.Millisecond)
						// after the first touch, it becomes running again
						job2f, errf = jq2.GetByEssence(&JobEssence{Cmd: cmd}, true, false)
						if errf != nil || job2f == nil || job2f.State != JobStateRunning || job2f.FailReason != FailReasonLost || job2f.Exited {
							lostCh <- false
						}
						lostCh <- true
					}()

					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)

					job2, err = jq2.GetByEssence(&JobEssence{Cmd: cmd}, true, false)
					So(err, ShouldBeNil)
					So(job2, ShouldNotBeNil)

					So(job2.State, ShouldEqual, JobStateComplete)
					So(job2.Exited, ShouldBeTrue)
					So(<-lostCh, ShouldBeTrue)
				})
			})
		})

		Convey("After connecting and adding some jobs under one RepGroup", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			for i := 0; i < 3; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo rgduptest %d", i), Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "rp1"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 3)
			So(already, ShouldEqual, 0)

			Convey("You can reserve and execute those", func() {
				for i := 0; i < 3; i++ {
					job, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					err = jq.Execute(ctx, job, config.RunnerExecShell)
					So(err, ShouldBeNil)
				}

				Convey("Then you can add dups and a new one under a new RepGroup and reserve/execute all of them", func() {
					jobs = nil
					for i := 0; i < 4; i++ {
						jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo rgduptest %d", i), Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "rp2"})
					}
					inserts, already, err := jq.Add(jobs, envVars, false)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 4)
					So(already, ShouldEqual, 0)

					for i := 0; i < 4; i++ {
						job, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldBeNil)
					}

					Convey("The jobs can be retrieved by either RepGroup and will have the expected RepGroup", func() {
						jobsg, err := jq.GetByRepGroup("rp1", false, 0, JobStateComplete, false, false)
						So(err, ShouldBeNil)
						So(len(jobsg), ShouldEqual, 3)
						So(jobsg[0].RepGroup, ShouldEqual, "rp1")

						jobsg, err = jq.GetByRepGroup("rp2", false, 0, JobStateComplete, false, false)
						So(err, ShouldBeNil)
						So(len(jobsg), ShouldEqual, 4)
						So(jobsg[0].RepGroup, ShouldEqual, "rp2")
					})
				})

				Convey("Previously complete jobs are rejected when adding with ignoreComplete", func() {
					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 0)
					So(already, ShouldEqual, 3)
				})
			})

			Convey("You can add dups and a new one under a new RepGroup", func() {
				jobs = nil
				for i := 0; i < 4; i++ {
					jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo rgduptest %d", i), Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "rp2"})
				}
				inserts, already, err := jq.Add(jobs, envVars, false)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 3)

				Convey("You can then reserve and execute the only 4 jobs", func() {
					for i := 0; i < 4; i++ {
						job, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldBeNil)
					}
					job, err := jq.Reserve(10 * time.Millisecond)
					So(err, ShouldBeNil)
					So(job, ShouldBeNil)

					Convey("The jobs can be retrieved by either RepGroup and will have the expected RepGroup", func() {
						jobs, err := jq.GetByRepGroup("rp1", false, 0, JobStateComplete, false, false)
						So(err, ShouldBeNil)
						So(len(jobs), ShouldEqual, 3)
						So(jobs[0].RepGroup, ShouldEqual, "rp1")

						jobs, err = jq.GetByRepGroup("rp2", false, 0, JobStateComplete, false, false)
						So(err, ShouldBeNil)
						So(len(jobs), ShouldEqual, 4)
						So(jobs[0].RepGroup, ShouldEqual, "rp2")
					})
				})
			})
		})

		Convey("After connecting and adding some jobs under some RepGroups", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo deptest1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep1"})
			jobs = append(jobs, &Job{Cmd: "echo deptest2", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep2"})
			jobs = append(jobs, &Job{Cmd: "echo deptest3", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep3"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 3)
			So(already, ShouldEqual, 0)

			Convey("You can search for the jobs using a common substring of their repgroups", func() {
				gottenJobs, err := jq.GetByRepGroup("dep", true, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(gottenJobs), ShouldEqual, 3)
				gottenJobs, err = jq.GetByRepGroup("2", true, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(gottenJobs), ShouldEqual, 1)
			})

			Convey("You can reserve and execute one of them", func() {
				j1, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(j1.RepGroup, ShouldEqual, "dep1")
				err = jq.Execute(ctx, j1, config.RunnerExecShell)
				So(err, ShouldBeNil)

				<-time.After(6 * time.Millisecond)

				gottenJobs, err := jq.GetByRepGroup("dep1", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(gottenJobs), ShouldEqual, 1)
				So(gottenJobs[0].State, ShouldEqual, JobStateComplete)

				Convey("You can then add jobs dependent on the initial jobs and themselves", func() {
					// https://i-msdn.sec.s-msft.com/dynimg/IC332764.gif
					jobs = nil
					d1 := NewEssenceDependency("echo deptest1", "")
					d2 := NewEssenceDependency("echo deptest2", "")
					d3 := NewEssenceDependency("echo deptest3", "")
					jobs = append(jobs, &Job{Cmd: "echo deptest4", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep4", Dependencies: Dependencies{d1}})
					d4 := NewEssenceDependency("echo deptest4", "")
					jobs = append(jobs, &Job{Cmd: "echo deptest5", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep5", Dependencies: Dependencies{d1, d2, d3}})
					d5 := NewEssenceDependency("echo deptest5", "")
					jobs = append(jobs, &Job{Cmd: "echo deptest6", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep6", Dependencies: Dependencies{d3, d4}})
					d6 := NewEssenceDependency("echo deptest6", "")
					jobs = append(jobs, &Job{Cmd: "echo deptest7", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep7", Dependencies: Dependencies{d5, d6}})
					jobs = append(jobs, &Job{Cmd: "echo deptest8", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep8", Dependencies: Dependencies{d5}})

					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 5)
					So(already, ShouldEqual, 0)

					// dep4 was added with a dependency on dep1, but after dep1
					// was already completed; it should start off in the ready
					// queue, not the dependent queue
					gottenJobs, err = jq.GetByRepGroup("dep4", false, 0, "", false, false)
					So(err, ShouldBeNil)
					So(len(gottenJobs), ShouldEqual, 1)
					So(gottenJobs[0].State, ShouldEqual, JobStateReady)

					Convey("They are then only reservable according to the dependency chain", func() {
						j2, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j2.RepGroup, ShouldEqual, "dep2")
						j3, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j3.RepGroup, ShouldEqual, "dep3")
						j4, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j4.RepGroup, ShouldEqual, "dep4")
						jNil, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(jNil, ShouldBeNil)

						// since things can take a while, we keep our reserved
						// jobs touched
						touchJ2 := true
						touchJ3 := true
						var touchLock sync.Mutex
						go func() {
							ticker := time.NewTicker(50 * time.Millisecond)
							for range ticker.C {
								touchLock.Lock()
								if touchJ2 {
									touch(jq, j2)
								}
								if touchJ3 {
									touch(jq, j3)
								}
								if !touchJ2 && !touchJ3 {
									ticker.Stop()
									touchLock.Unlock()
									return
								}
								touchLock.Unlock()
								continue
							}
						}()

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						err = jq.Execute(ctx, j4, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ3 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j3, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						gottenJobs, err = jq.GetByRepGroup("dep5", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ2 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j2, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep5", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						j5, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j5.RepGroup, ShouldEqual, "dep5")
						j6, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j6.RepGroup, ShouldEqual, "dep6")
						jNil, err = jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(jNil, ShouldBeNil)

						touchJ6 := true
						go func() {
							ticker := time.NewTicker(50 * time.Millisecond)
							for range ticker.C {
								touchLock.Lock()
								if touchJ6 {
									touch(jq, j6)
								} else {
									ticker.Stop()
									touchLock.Unlock()
									return
								}
								touchLock.Unlock()
								continue
							}
						}()

						gottenJobs, err = jq.GetByRepGroup("dep8", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						err = jq.Execute(ctx, j5, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep8", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						gottenJobs, err = jq.GetByRepGroup("dep7", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ6 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j6, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep7", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)
					})
				})

				Convey("You can add jobs with non-existent dependencies", func() {
					// first get rid of the jobs added earlier
					for i := 0; i < 2; i++ {
						job, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldBeNil)
					}

					jobs = nil
					d5 := NewEssenceDependency("echo deptest5", "")
					jobs = append(jobs, &Job{Cmd: "echo deptest4", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep4", Dependencies: Dependencies{d5}})
					jobs = append(jobs, &Job{Cmd: "echo deptest5", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep5"})

					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 2)
					So(already, ShouldEqual, 0)

					j5, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(j5.RepGroup, ShouldEqual, "dep5")
					jNil, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(jNil, ShouldBeNil)

					err = jq.Execute(ctx, j5, config.RunnerExecShell)
					So(err, ShouldBeNil)

					j4, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(j4.RepGroup, ShouldEqual, "dep4")

					err = jq.Release(j4, nil, "")
					So(err, ShouldBeNil)

					// *** we should implement rejection of dependency cycles
					// and test for that
				})
			})
		})

		Convey("After connecting you can add some jobs with DepGroups", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo deptest1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep1", DepGroups: []string{"dep1", "dep1+2+3"}})
			jobs = append(jobs, &Job{Cmd: "echo deptest2", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep2", DepGroups: []string{"dep2", "dep1+2+3"}})
			jobs = append(jobs, &Job{Cmd: "echo deptest3", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep3", DepGroups: []string{"dep3", "dep1+2+3"}})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 3)
			So(already, ShouldEqual, 0)

			Convey("You can reserve and execute one of them", func() {
				j1, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(j1.RepGroup, ShouldEqual, "dep1")
				err = jq.Execute(ctx, j1, config.RunnerExecShell)
				So(err, ShouldBeNil)

				<-time.After(6 * time.Millisecond)

				gottenJobs, err := jq.GetByRepGroup("dep1", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(gottenJobs), ShouldEqual, 1)
				So(gottenJobs[0].State, ShouldEqual, JobStateComplete)

				Convey("You can then add jobs dependent on the initial jobs and themselves", func() {
					// https://i-msdn.sec.s-msft.com/dynimg/IC332764.gif
					jobs = nil
					d1 := NewDepGroupDependency("dep1")
					d123 := NewDepGroupDependency("dep1+2+3")
					d3 := NewDepGroupDependency("dep3")
					jobs = append(jobs, &Job{Cmd: "echo deptest4", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep4", DepGroups: []string{"dep4"}, Dependencies: Dependencies{d1}})
					d4 := NewDepGroupDependency("dep4")
					jobs = append(jobs, &Job{Cmd: "echo deptest5", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep5", DepGroups: []string{"dep5"}, Dependencies: Dependencies{d123}})
					d5 := NewDepGroupDependency("dep5")
					jobs = append(jobs, &Job{Cmd: "echo deptest6", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep6", DepGroups: []string{"dep6"}, Dependencies: Dependencies{d3, d4}})
					d6 := NewDepGroupDependency("dep6")
					jobs = append(jobs, &Job{Cmd: "echo deptest7", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep7", DepGroups: []string{"final"}, Dependencies: Dependencies{d5, d6}})
					jobs = append(jobs, &Job{Cmd: "echo deptest8", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep8", DepGroups: []string{"final"}, Dependencies: Dependencies{d5}})

					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 5)
					So(already, ShouldEqual, 0)

					// dep4 was added with a dependency on dep1, but after dep1
					// was already completed; it should start off in the ready
					// queue, not the dependent queue
					gottenJobs, err = jq.GetByRepGroup("dep4", false, 0, "", false, false)
					So(err, ShouldBeNil)
					So(len(gottenJobs), ShouldEqual, 1)
					So(gottenJobs[0].State, ShouldEqual, JobStateReady)

					Convey("They are then only reservable according to the dependency chain", func() {
						j2, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j2.RepGroup, ShouldEqual, "dep2")
						j3, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j3.RepGroup, ShouldEqual, "dep3")
						j4, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j4.RepGroup, ShouldEqual, "dep4")
						jNil, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(jNil, ShouldBeNil)

						// since things can take a while, we keep our reserved
						// jobs touched
						touchJ2 := true
						touchJ3 := true
						var touchLock sync.Mutex
						go func() {
							ticker := time.NewTicker(50 * time.Millisecond)
							for range ticker.C {
								touchLock.Lock()
								if touchJ2 {
									touch(jq, j2)
								}
								if touchJ3 {
									touch(jq, j3)
								}
								if !touchJ2 && !touchJ3 {
									ticker.Stop()
									touchLock.Unlock()
									return
								}
								touchLock.Unlock()
								continue
							}
						}()

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						err = jq.Execute(ctx, j4, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ3 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j3, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep6", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						gottenJobs, err = jq.GetByRepGroup("dep5", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ2 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j2, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep5", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						j5, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j5.RepGroup, ShouldEqual, "dep5")
						j6, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(j6.RepGroup, ShouldEqual, "dep6")
						jNil, err = jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						So(jNil, ShouldBeNil)

						touchJ6 := true
						go func() {
							ticker := time.NewTicker(50 * time.Millisecond)
							for range ticker.C {
								touchLock.Lock()
								if touchJ6 {
									touch(jq, j6)
								} else {
									ticker.Stop()
									touchLock.Unlock()
									return
								}
								touchLock.Unlock()
								continue
							}
						}()

						gottenJobs, err = jq.GetByRepGroup("dep8", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						err = jq.Execute(ctx, j5, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep8", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						gottenJobs, err = jq.GetByRepGroup("dep7", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

						touchLock.Lock()
						touchJ6 = false
						touchLock.Unlock()
						err = jq.Execute(ctx, j6, config.RunnerExecShell)
						So(err, ShouldBeNil)

						gottenJobs, err = jq.GetByRepGroup("dep7", false, 0, "", false, false)
						So(err, ShouldBeNil)
						So(len(gottenJobs), ShouldEqual, 1)
						So(gottenJobs[0].State, ShouldEqual, JobStateReady)

						Convey("DepGroup dependencies are live, bringing back jobs if new jobs are added that match their dependencies", func() {
							jobs = nil
							dfinal := NewDepGroupDependency("final")
							jobs = append(jobs, &Job{Cmd: "echo after final", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "afterfinal", DepGroups: []string{"afterfinal"}, Dependencies: Dependencies{dfinal}})
							dafinal := NewDepGroupDependency("afterfinal")
							jobs = append(jobs, &Job{Cmd: "echo after after-final", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "after-afterfinal", Dependencies: Dependencies{dafinal}})
							inserts, already, err := jq.Add(jobs, envVars, true)
							So(err, ShouldBeNil)
							So(inserts, ShouldEqual, 2)
							So(already, ShouldEqual, 0)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							j7, err := jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(j7.RepGroup, ShouldEqual, "dep7")
							err = jq.Execute(ctx, j7, config.RunnerExecShell)
							So(err, ShouldBeNil)
							j8, err := jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(j8.RepGroup, ShouldEqual, "dep8")
							err = jq.Execute(ctx, j8, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateReady)

							jobs = nil
							jobs = append(jobs, &Job{Cmd: "echo deptest9", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep9", DepGroups: []string{"final"}})
							inserts, already, err = jq.Add(jobs, envVars, true)
							So(err, ShouldBeNil)
							So(inserts, ShouldEqual, 1)
							So(already, ShouldEqual, 0)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							j9, err := jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(j9.RepGroup, ShouldEqual, "dep9")
							err = jq.Execute(ctx, j9, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateReady)

							faf, err := jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(faf.RepGroup, ShouldEqual, "afterfinal")
							err = jq.Execute(ctx, faf, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateComplete)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateReady)

							inserts, already, err = jq.Add(jobs, envVars, false)
							So(err, ShouldBeNil)
							So(inserts, ShouldEqual, 2) // the job I added, and the resurrected afterfinal job
							So(already, ShouldEqual, 0)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							j9, err = jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(j9.RepGroup, ShouldEqual, "dep9")
							err = jq.Execute(ctx, j9, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateReady)

							faf, err = jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(faf.RepGroup, ShouldEqual, "afterfinal")
							err = jq.Execute(ctx, faf, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateComplete)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateReady)

							faaf, err := jq.Reserve(50 * time.Millisecond)
							So(err, ShouldBeNil)
							So(faaf.RepGroup, ShouldEqual, "after-afterfinal")
							err = jq.Execute(ctx, faaf, config.RunnerExecShell)
							So(err, ShouldBeNil)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateComplete)

							jobs = nil
							jobs = append(jobs, &Job{Cmd: "echo deptest10", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep10", DepGroups: []string{"final"}})
							inserts, already, err = jq.Add(jobs, envVars, true)
							So(err, ShouldBeNil)
							So(inserts, ShouldEqual, 3)
							So(already, ShouldEqual, 0)

							gottenJobs, err = jq.GetByRepGroup("afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)

							gottenJobs, err = jq.GetByRepGroup("after-afterfinal", false, 0, "", false, false)
							So(err, ShouldBeNil)
							So(len(gottenJobs), ShouldEqual, 1)
							So(gottenJobs[0].State, ShouldEqual, JobStateDependent)
						})
					})
				})

				Convey("You can add jobs with non-existent depgroup dependencies", func() {
					// first get rid of the jobs added earlier
					for i := 0; i < 2; i++ {
						job, err := jq.Reserve(50 * time.Millisecond)
						So(err, ShouldBeNil)
						err = jq.Execute(ctx, job, config.RunnerExecShell)
						So(err, ShouldBeNil)
					}

					jobs = nil
					d5 := NewDepGroupDependency("dep5")
					jobs = append(jobs, &Job{Cmd: "echo deptest4", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep4", Dependencies: Dependencies{d5}})
					jobs = append(jobs, &Job{Cmd: "echo deptest5", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(3), RepGroup: "dep5", DepGroups: []string{"dep5"}})

					inserts, already, err := jq.Add(jobs, envVars, true)
					So(err, ShouldBeNil)
					So(inserts, ShouldEqual, 2)
					So(already, ShouldEqual, 0)

					j5, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(j5.RepGroup, ShouldEqual, "dep5")
					jNil, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(jNil, ShouldBeNil)

					err = jq.Execute(ctx, j5, config.RunnerExecShell)
					So(err, ShouldBeNil)

					j4, err := jq.Reserve(50 * time.Millisecond)
					So(err, ShouldBeNil)
					So(j4.RepGroup, ShouldEqual, "dep4")

					err = jq.Release(j4, nil, "")
					So(err, ShouldBeNil)

					// *** we should implement rejection of dependency cycles
					// and test for that
				})
			})
		})

		Reset(func() {
			server.Stop(ctx, true)
		})
	})
}

func TestJobqueueLimitGroups(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, standardReqs, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	Convey("Once a new jobqueue server is up", t, func() {
		ServerItemTTR = 1 * time.Second
		ClientTouchInterval = 2500 * time.Millisecond
		server, _, token, errs := serve(ctx, serverConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		server.rc = serverRC

		Convey("You can connect, and add jobs with LimitGroups", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer func() {
				errd := jq.Disconnect()
				if errd != nil {
					fmt.Printf("Disconnect failed: %s\n", errd)
				}
			}()

			var addJobs []*Job
			for i := 1; i <= 5; i++ {
				addJobs = append(addJobs, &Job{Cmd: fmt.Sprintf("echo %d", i), Cwd: "/tmp", ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "ab", LimitGroups: []string{"b:2", "a:3"}})
			}
			inserts, already, err := jq.Add(addJobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 5)
			So(already, ShouldEqual, 0)

			reserveJobs := func() []*Job {
				var jobs []*Job
				for i := 1; i <= 5; i++ {
					job, errr := jq.ReserveScheduled(25*time.Millisecond, "110:30:1:0~a,b")
					So(errr, ShouldBeNil)
					if job != nil {
						jobs = append(jobs, job)
					}
				}
				return jobs
			}

			Convey("You can't reserve more than limit", func() {
				jobs := reserveJobs()
				So(len(jobs), ShouldEqual, 2)

				finalJob := jobs[1]

				stopTouching := make(chan bool, 1)
				go func() {
					// touch this periodically because it might take more than 1
					// second from reserving it to executing it later
					ticker := time.NewTicker(250 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							jq.Touch(finalJob)
						case <-stopTouching:
							return
						}
					}
				}()
				defer func() {
					stopTouching <- true
				}()

				for i := 1; i <= 3; i++ {
					err = jq.Execute(ctx, jobs[0], config.RunnerExecShell)
					So(err, ShouldBeNil)
					jobs = reserveJobs()
					So(len(jobs), ShouldEqual, 1)
				}

				err = jq.Execute(ctx, jobs[0], config.RunnerExecShell)
				So(err, ShouldBeNil)

				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 0)

				stopTouching <- true
				err = jq.Execute(ctx, finalJob, config.RunnerExecShell)
				So(err, ShouldBeNil)
				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 0)
			})

			Convey("You can change the limit by adding a new Job", func() {
				jobs := reserveJobs()
				So(len(jobs), ShouldEqual, 2)

				jobs = []*Job{}
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo %d", 6), Cwd: "/tmp", ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "ab", LimitGroups: []string{"a:3", "b:4"}})
				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 1)
			})

			Convey("You can get and change the limit using GetOrSetLimitGroup()", func() {
				l, err := jq.GetOrSetLimitGroup("b")
				So(err, ShouldBeNil)
				So(l, ShouldEqual, 2)

				jobs := reserveJobs()
				So(len(jobs), ShouldEqual, 2)

				l, err = jq.GetOrSetLimitGroup("b:4")
				So(err, ShouldBeNil)
				So(l, ShouldEqual, 4)

				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 1)

				l, err = jq.GetOrSetLimitGroup("b")
				So(err, ShouldBeNil)
				So(l, ShouldEqual, 4)
			})

			Convey("You can't add Jobs with bad LimitGroup names", func() {
				var jobs []*Job
				jobs = append(jobs, &Job{Cmd: "echo bad", Cwd: "/tmp", ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "ab", LimitGroups: []string{"b:2", "a:d3"}})
				_, _, err := jq.Add(jobs, envVars, true)
				So(err, ShouldNotBeNil)
				serr, ok := err.(Error)
				So(ok, ShouldBeTrue)
				So(serr.Err, ShouldEqual, ErrBadLimitGroup)
			})

			Convey("Failing to start a job after reserving it does not use up the limit", func() {
				jobs := reserveJobs()
				So(len(jobs), ShouldEqual, 2)

				<-time.After(2 * time.Second)
				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 2)
			})

			Convey("Burying jobs after reserving them does not use up the limit", func() {
				jobs := reserveJobs()
				So(len(jobs), ShouldEqual, 2)

				jq.Bury(jobs[0], nil, "foo")
				jq.Bury(jobs[1], nil, "foo")

				<-time.After(2 * time.Second)
				jobs = reserveJobs()
				So(len(jobs), ShouldEqual, 2)
			})
		})

		Reset(func() {
			server.Stop(ctx, true)
		})
	})
}

func jobsToJobEssenses(jobs []*Job) []*JobEssence {
	jes := make([]*JobEssence, 0, len(jobs))
	for _, job := range jobs {
		jes = append(jes, job.ToEssense())
	}
	return jes
}

func TestJobqueueModify(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, standardReqs, clientConnectTime := jobqueueTestInit(true)
	rtime := 50 * time.Millisecond
	rgroup := "110:30:1:0"
	learnedRgroup := "200:30:1:0"
	learnedRAMNormal := 100
	learnedRAMExtraRange := []int{200, 500}
	tmp := "/tmp"

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	Convey("Once a new jobqueue server is up and client is connected", t, func() {
		ServerItemTTR = 5 * time.Second
		ClientTouchInterval = 2500 * time.Millisecond
		ClientReleaseDelay = 0 * time.Second
		server, _, token, errs := serve(ctx, serverConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		server.rc = serverRC

		jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
		So(err, ShouldBeNil)
		defer func() {
			errd := jq.Disconnect()
			if errd != nil {
				fmt.Printf("Disconnect failed: %s\n", errd)
			}
		}()

		var addJobs []*Job
		jm := NewJobModifer()

		add := func(expected int) {
			inserts, already, err := jq.Add(addJobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, expected)
			So(already, ShouldEqual, 0)
		}

		reserve := func(schedStr, expected string, skip ...int) *Job {
			s := 1
			if len(skip) == 1 {
				s = skip[0]
			}
			_, _, line, _ := runtime.Caller(s)
			job, errr := jq.ReserveScheduled(rtime, schedStr)
			So(errr, ShouldBeNil)
			if job == nil && schedStr == learnedRgroup {
				// *** not sure why the memory is sometimes higher when running
				// under Travis or race...
				job, errr = jq.ReserveScheduled(rtime, "300:30:1:0")
				So(errr, ShouldBeNil)

				if job == nil {
					job, errr = jq.ReserveScheduled(rtime, "400:30:1:0")
					So(errr, ShouldBeNil)
				}
				if job == nil {
					job, errr = jq.ReserveScheduled(rtime, "500:30:1:0")
					So(errr, ShouldBeNil)
				}
				if job == nil {
					job, errr = jq.ReserveScheduled(rtime, "600:30:1:0")
					So(errr, ShouldBeNil)
				}
			}
			if job == nil {
				schedDetails := server.schedulerGroupDetails()
				if len(schedDetails) > 0 {
					fmt.Printf("\nschedgrp %s not found, we have:\n", schedStr)
					for _, val := range schedDetails {
						fmt.Printf(" - %s\n", val)
					}
				} else {
					fmt.Printf("\nschedgrp %s not found, and nothing in the scheduler.\n", schedStr)
				}
				fmt.Printf(" *** test from line %d failed\n", line)
			}
			So(job, ShouldNotBeNil)
			So(job.Cmd, ShouldEqual, expected)
			return job
		}

		modify := func(repgroup string, expected int) {
			jobs, err := jq.GetByRepGroup(repgroup, false, 0, "", false, false)
			So(err, ShouldBeNil)
			jes := jobsToJobEssenses(jobs)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, expected)
		}

		execute := func(job *Job, shouldWork bool, expectedStdout string) *Job {
			err := jq.Execute(ctx, job, config.RunnerExecShell)
			if shouldWork {
				So(err, ShouldBeNil)
			} else {
				So(err, ShouldNotBeNil)
			}

			jobs, err := jq.GetByRepGroup(job.RepGroup, false, 0, "", true, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)
			if !shouldWork && expectedStdout != "" {
				stdout, err := jobs[0].StdOut()
				So(err, ShouldBeNil)
				So(stdout, ShouldEqual, expectedStdout)
			}
			return jobs[0]
		}

		kick := func(repgroup string, schedStr, expectedCmd string, expectedStdout string) *Job {
			jobs, err := jq.GetByRepGroup(repgroup, false, 0, "", false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)
			kicked, err := jq.Kick(jobsToJobEssenses(jobs))
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			job := reserve(schedStr, expectedCmd, 2)
			return execute(job, false, expectedStdout)
		}

		release := func(job *Job) {
			err := jq.Release(job, &JobEndState{}, "")
			So(err, ShouldBeNil)
		}

		groupsToDeps := func(groups string) (deps Dependencies) {
			for _, depgroup := range strings.Split(groups, ",") {
				deps = append(deps, NewDepGroupDependency(depgroup))
			}
			return
		}

		Convey("You can modify the priority and limit of jobs", func() {
			for i := 1; i <= 3; i++ {
				addJobs = append(addJobs, &Job{Cmd: fmt.Sprintf("echo %d", i), Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a", Priority: uint8(5)})
			}
			for i := 4; i <= 7; i++ {
				addJobs = append(addJobs, &Job{Cmd: fmt.Sprintf("echo %d", i), Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "b", Priority: uint8(6)})
			}

			add(7)

			<-time.After(1000 * time.Millisecond) // wait for the jobs to be ready and assiged sched groups

			reserve(rgroup, "echo 4")

			jm.SetPriority(uint8(4))
			modify("b", 3)
			reserve(rgroup, "echo 1")

			jm = NewJobModifer()
			jm.SetLimitGroups([]string{"foo:0"})
			modify("a", 2)
			reserve(rgroup, "echo 5")
		})

		Convey("You can modify the command line of a job", func() {
			addJobs = append(addJobs, &Job{Cmd: "echo a && false", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, "echo a && false")
			job = execute(job, false, "a")
			So(job.Attempts, ShouldEqual, 1)

			jm.SetCmd("echo b && false")
			modify("a", 1)

			job = kick("a", rgroup, "echo b && false", "b")
			So(job.Attempts, ShouldEqual, 2)
		})

		Convey("You can't modify the command line of a job to match another job", func() {
			addJobs = append(addJobs, &Job{Cmd: "echo a && false", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			addJobs = append(addJobs, &Job{Cmd: "echo b && false", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "b"})
			add(2)

			jm.SetCmd("echo b && false")
			modify("a", 0)

			jm.SetCmd("true")
			modify("a", 1)
			modify("b", 0)
		})

		Convey("You can modify the cwd of a job, with and without cwd_matters", func() {
			dir, err := os.Getwd()
			So(err, ShouldBeNil)
			cmd := "pwd && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: dir, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, cmd)
			job = execute(job, false, "")
			stdout, err := job.StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldNotEqual, dir)
			So(stdout, ShouldStartWith, dir)

			jm.SetCwd(tmp)
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			stdout, err = job.StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldNotEqual, tmp)
			So(stdout, ShouldStartWith, tmp)
			So(job.ActualCwd, ShouldNotEqual, tmp)
			So(job.ActualCwd, ShouldStartWith, tmp)

			cmd = "pwd && true && false"
			addJobs = []*Job{{Cmd: cmd, Cwd: dir, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "b", CwdMatters: true}}
			add(1)

			job = reserve(rgroup, cmd)
			execute(job, false, dir)

			jm.SetCwd(tmp)
			modify("b", 1)

			job = kick("b", rgroup, cmd, tmp)
			So(job.Cwd, ShouldEqual, tmp)
		})

		Convey("You can modify the req_group of a job", func() {
			cmd := "echo a"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "initial", Requirements: standardReqs, Override: uint8(0), Retries: uint8(0), RepGroup: "a"})
			add(1)

			jm.SetReqGroup("modified")
			modify("a", 1)

			job := reserve(rgroup, cmd)
			execute(job, true, "a")

			cmd = "echo b"
			addJobs = []*Job{{Cmd: cmd, Cwd: tmp, ReqGroup: "modified", Requirements: &jqs.Requirements{RAM: 300, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}, Override: uint8(0), Retries: uint8(0), RepGroup: "b"}}
			add(1)

			// if the modify of initial didn't work, we'd have no learning of
			// the modified reqgroup, so it would get 400:30:1:0 as its scheduler
			// group. But due to learning, the RAM is 100
			job = reserve(learnedRgroup, cmd)
			if job.Requirements.RAM != learnedRAMNormal {
				So(job.Requirements.RAM, ShouldBeBetweenOrEqual, learnedRAMExtraRange[0], learnedRAMExtraRange[1])
			} else {
				So(job.Requirements.RAM, ShouldEqual, learnedRAMNormal)
			}
		})

		Convey("You can modify the requirements of a job", func() {
			// not actually using a scheduler to determine when and if jobs are
			// allowed to run, so we're just doing a basic test that the reqs
			// of the job change appropriately
			cmd := "echo a"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			jm.SetRequirements(&jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: 0, CoresSet: true, Disk: 0, Other: make(map[string]string)})
			modify("a", 1)

			job := reserve("200:30:0:0", cmd)
			So(job.Requirements.RAM, ShouldEqual, 100)
			So(job.Requirements.Cores, ShouldEqual, 0)
			So(job.Requirements.Disk, ShouldEqual, 0)
			So(len(job.Requirements.Other), ShouldEqual, 0)
			release(job)

			other := make(map[string]string)
			other["foo"] = "bar"
			jm = NewJobModifer()
			jm.SetRequirements(&jqs.Requirements{RAM: 600, Time: 40 * time.Minute, Cores: 0, Disk: 5, DiskSet: true, Other: other, OtherSet: true})
			modify("a", 1)

			job = reserve("700:60:0:5:cfd399e4a9dba25ac14a2454ce3e8d24", cmd)
			So(job.Requirements.RAM, ShouldEqual, 600)
			So(job.Requirements.Cores, ShouldEqual, 0)
			So(job.Requirements.Disk, ShouldEqual, 5)
			So(len(job.Requirements.Other), ShouldEqual, 1)
			release(job)

			jm = NewJobModifer()
			jm.SetRequirements(&jqs.Requirements{Cores: 0.5, CoresSet: true, Disk: 0, DiskSet: true, Other: make(map[string]string), OtherSet: true})
			modify("a", 1)

			job = reserve("700:60:0.5:0", cmd)
			So(job.Requirements.RAM, ShouldEqual, 600)
			So(job.Requirements.Cores, ShouldEqual, 0.5)
			So(job.Requirements.Disk, ShouldEqual, 0)
			So(len(job.Requirements.Other), ShouldEqual, 0)
			release(job)
		})

		Convey("You can modify the override of a job", func() {
			addJobs = append(addJobs, &Job{Cmd: "echo pre", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(0), Retries: uint8(0), RepGroup: "pre"})
			cmd := "echo a && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(2)

			job := reserve(rgroup, "echo pre")
			execute(job, true, "")
			job = reserve(rgroup, cmd)
			job = execute(job, false, "a")
			So(job.Requirements.Time, ShouldEqual, 10*time.Second)

			jm.SetOverride(uint8(0))
			modify("a", 1)

			// by turning off override, we enable the learned values

			job = kick("a", learnedRgroup, cmd, "a")
			So(job.Requirements.Time, ShouldEqual, 1*time.Second)
			stats := server.GetServerStats()
			So(stats.ETC, ShouldEqual, 0*time.Second)

			_, err := jq.Kick(jobsToJobEssenses([]*Job{job}))
			So(err, ShouldBeNil)
			job = reserve(learnedRgroup, cmd)
			jq.Started(job, 1)
			stats = server.GetServerStats()
			So(stats.ETC, ShouldEqual, 1*time.Second)
		})

		Convey("You can modify the retries of a job", func() {
			cmd := "false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, cmd)
			job = execute(job, false, "")
			So(job.State, ShouldEqual, JobStateBuried)
			So(job.Retries, ShouldEqual, 0)

			jm.SetRetries(uint8(1))
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			So(job.State, ShouldEqual, JobStateReady)
			So(job.Retries, ShouldEqual, 1)
		})

		Convey("You can modify the dependencies of a job", func() {
			addJobs = append(addJobs, &Job{Cmd: "echo a", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a", DepGroups: []string{"a"}})
			addJobs = append(addJobs, &Job{Cmd: "echo b", Cwd: tmp, ReqGroup: "rgroup", Requirements: &jqs.Requirements{RAM: 400, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}, Override: uint8(2), Retries: uint8(0), RepGroup: "b", DepGroups: []string{"b"}})
			addJobs = append(addJobs, &Job{Cmd: "echo c", Cwd: tmp, ReqGroup: "rgroup", Requirements: &jqs.Requirements{RAM: 800, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}, Override: uint8(2), Retries: uint8(0), RepGroup: "c", Dependencies: groupsToDeps("a,b")})
			add(3)

			jobA := reserve(rgroup, "echo a")
			reserve("500:30:1:0", "echo b")

			jobC, err := jq.ReserveScheduled(rtime, "900:30:1:0")
			So(err, ShouldBeNil)
			So(jobC, ShouldBeNil)

			jm.SetDependencies(groupsToDeps("a"))
			modify("c", 1)

			execute(jobA, true, "")

			// without the modification, we'd also need to execute job b before
			// the following reserve would work

			reserve("900:30:1:0", "echo c")
		})

		Convey("You can modify the command line of a job that other jobs depend on", func() {
			addJobs = append(addJobs, &Job{Cmd: "echo a && false", Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a", DepGroups: []string{"a"}})
			addJobs = append(addJobs, &Job{Cmd: "echo b && true", Cwd: tmp, ReqGroup: "rgroup", Requirements: &jqs.Requirements{RAM: 400, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}, Override: uint8(2), Retries: uint8(0), RepGroup: "b", Dependencies: groupsToDeps("a")})
			add(2)

			job := reserve(rgroup, "echo a && false")
			job = execute(job, false, "a")
			So(job.State, ShouldEqual, JobStateBuried)

			job, err := jq.ReserveScheduled(rtime, "700:30:1:0")
			So(err, ShouldBeNil)
			So(job, ShouldBeNil)

			jm.SetCmd("echo a && true")
			modify("a", 1)

			// (kick() assumes the command will fail again)
			jobs, err := jq.GetByRepGroup("a", false, 0, "", false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)
			kicked, err := jq.Kick(jobsToJobEssenses(jobs))
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)
			job = reserve(rgroup, "echo a && true")
			execute(job, true, "")

			reserve("500:30:1:0", "echo b && true")
		})

		Convey("You can modify the behaviours of a job", func() {
			dir, err := os.MkdirTemp("", "wr_jobqueue_mod_test")
			So(err, ShouldBeNil)
			defer os.RemoveAll(dir)

			cmd := "touch a && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: dir, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a", Behaviours: []*Behaviour{{When: OnExit, Do: Cleanup}}})
			add(1)

			job := reserve(rgroup, cmd)
			job = execute(job, false, "")
			path := filepath.Join(job.ActualCwd, "a")
			_, err = os.Stat(path)
			So(err, ShouldNotBeNil)

			jm.SetBehaviours([]*Behaviour{{When: OnExit, Do: Nothing}})
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			path = filepath.Join(job.ActualCwd, "a")
			_, err = os.Stat(path)
			So(err, ShouldBeNil)

			jm = NewJobModifer()
			cpPath := filepath.Join(dir, "copied")
			jm.SetBehaviours([]*Behaviour{{When: OnExit, Do: Cleanup}, {When: OnFailure, Do: Run, Arg: fmt.Sprintf("cp a %s", cpPath)}})
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			path = filepath.Join(job.ActualCwd, "a")
			_, err = os.Stat(path)
			So(err, ShouldNotBeNil)
			_, err = os.Stat(cpPath)
			So(err, ShouldBeNil)
		})

		Convey("You can modify the env of a job", func() {
			cmd := "echo $wrmodtestfoo && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, cmd)
			execute(job, false, "")

			errs := jm.SetEnvOverride("wrmodtestfoo=bar")
			So(errs, ShouldBeNil)
			modify("a", 1)

			kick("a", rgroup, cmd, "bar")

			jm = NewJobModifer()
			errs = jm.SetEnvOverride("")
			So(errs, ShouldBeNil)
			modify("a", 1)

			kick("a", rgroup, cmd, "")
		})

		Convey("You can modify the cwd_matters of a job", func() {
			cmd := "pwd && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, cmd)
			job = execute(job, false, "")
			stdout, err := job.StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldNotEqual, tmp)
			So(stdout, ShouldStartWith, tmp)
			So(job.ActualCwd, ShouldNotEqual, tmp)
			So(job.ActualCwd, ShouldStartWith, tmp)
			So(job.Cwd, ShouldEqual, tmp)

			jm.SetCwdMatters(true)
			modify("a", 1)

			job = kick("a", rgroup, cmd, tmp)
			So(job.ActualCwd, ShouldEqual, tmp)
			So(job.Cwd, ShouldEqual, tmp)

			jm = NewJobModifer()
			jm.SetCwdMatters(false)
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			stdout, err = job.StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldNotEqual, tmp)
			So(stdout, ShouldStartWith, tmp)
			So(job.ActualCwd, ShouldNotEqual, tmp)
			So(job.ActualCwd, ShouldStartWith, tmp)
			So(job.Cwd, ShouldEqual, tmp)
		})

		Convey("You can modify the change_home of a job", func() {
			home := os.Getenv("HOME")
			cmd := "echo $HOME && false"
			addJobs = append(addJobs, &Job{Cmd: cmd, Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a"})
			add(1)

			job := reserve(rgroup, cmd)
			execute(job, false, home)

			jm.SetChangeHome(true)
			modify("a", 1)

			job = kick("a", rgroup, cmd, "")
			stdout, err := job.StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldNotEqual, tmp)
			So(stdout, ShouldStartWith, tmp)
			So(stdout, ShouldEqual, job.ActualCwd)

			jm = NewJobModifer()
			jm.SetChangeHome(false)
			modify("a", 1)

			kick("a", rgroup, cmd, home)
		})

		Convey("Modifying does not resume a paused server", func() {
			for i := 1; i <= 3; i++ {
				addJobs = append(addJobs, &Job{Cmd: fmt.Sprintf("echo %d", i), Cwd: tmp, ReqGroup: "rgroup", Requirements: standardReqs, Override: uint8(2), Retries: uint8(0), RepGroup: "a", Priority: uint8(5)})
			}

			add(3)

			<-time.After(1000 * time.Millisecond)

			reserve(rgroup, "echo 1")

			_, _, errp := jq.PauseServer()
			So(errp, ShouldBeNil)
			job, errr := jq.ReserveScheduled(rtime, rgroup)
			So(job, ShouldBeNil)
			So(errr, ShouldBeNil)

			jm = NewJobModifer()
			jm.SetPriority(uint8(4))
			modify("a", 2)

			job, errr = jq.ReserveScheduled(rtime, rgroup)
			So(job, ShouldBeNil)
			So(errr, ShouldBeNil)

			errr = jq.ResumeServer()
			So(errr, ShouldBeNil)

			reserve(rgroup, "echo 2")

			// *** want an inverse test that ResumeServer() in the middle of
			// carriying out a Modify() does not cause issues, but ~impossible
			// without mocks
		})

		// *** untested: SetDepGroups(), SetBsubMode(). These are not yet fully
		// implemented.

		// *** want to test that modifications survive a server crash and
		// restart

		Reset(func() {
			server.Stop(ctx, true)
		})
	})
}

func TestJobqueueHighMem(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, standardReqs, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	// start these tests anew because they need a long TTR
	maxRAM, errp := internal.ProcMeminfoMBs()
	if errp == nil && maxRAM > 80000 { // authors high memory system
		Convey("If a job uses close to all memory on machine it is killed and we recommend more next time", t, func() {
			ServerItemTTR = 200 * time.Second
			ClientTouchInterval = 50 * time.Millisecond
			server, _, token, errs := serve(ctx, serverConfig)
			So(errs, ShouldBeNil)
			defer func() {
				server.Stop(ctx, true)
			}()

			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)
			jq2, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq2)

			var jobs []*Job
			cmd := "perl -e '@a; for (1..1000) { push(@a, q[a] x 800000000) }'"
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: standardReqs, Retries: uint8(0), RepGroup: "run_out_of_mem"})
			RecMBRound = 1
			defer func() {
				RecMBRound = 100 // revert back to normal
			}()
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateReserved)

			ClientPercentMemoryKill = 1
			err = jq.Execute(ctx, job, config.RunnerExecShell)
			ClientPercentMemoryKill = 90
			So(err, ShouldNotBeNil)
			jqerr, ok := err.(Error)
			So(ok, ShouldBeTrue)
			So(jqerr.Err, ShouldEqual, FailReasonRAM)
			So(job.State, ShouldEqual, JobStateBuried)
			So(job.Exited, ShouldBeTrue)
			So(job.Exitcode, ShouldEqual, -1)
			So(job.FailReason, ShouldEqual, FailReasonRAM)
			So(job.Requirements.RAM, ShouldEqual, 10)

			// requirements only change on becoming ready
			kicked, err := jq.Kick([]*JobEssence{job.ToEssense()})
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)
			job, err = jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Cmd, ShouldEqual, cmd)
			So(job.State, ShouldEqual, JobStateReserved)
			So(job.Requirements.RAM, ShouldBeGreaterThanOrEqualTo, 1000)

			errr := jq.Release(job, &JobEndState{}, "")
			So(errr, ShouldBeNil)
			deleted, errd := jq.Delete([]*JobEssence{{Cmd: cmd}})
			So(errd, ShouldBeNil)
			So(deleted, ShouldEqual, 1)
		})
	} else {
		SkipConvey("Skipping test that uses most of machine memory", t, func() {})
	}
}

func TestJobqueueProduction(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}
	config, serverConfig, addr, _, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	managerDBBkFile := serverConfig.DBFileBackup

	// start these tests anew because I need to disable dev-mode wiping of the
	// db to test some behaviours
	Convey("Once a new jobqueue server is up it creates a db file", t, func() {
		ServerItemTTR = 2 * time.Second
		forceBackups = true
		defer func() {
			forceBackups = false
		}()
		server, _, token, errs := serve(ctx, serverConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		_, err := os.Stat(config.ManagerDBFile)
		So(err, ShouldBeNil)
		_, err = os.Stat(managerDBBkFile)
		So(err, ShouldNotBeNil)

		Convey("You can connect, and add 2 jobs, which creates a db backup", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			// do this in 2 separate Add() calls to better test how backups
			// work
			server.db.slowBackups = true
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo 1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			wait := make(chan bool)
			go func() {
				<-time.After(150 * time.Millisecond)
				wait <- true
				<-time.After(300 * time.Millisecond)
				wait <- true
			}()
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)
			jobs = append(jobs, &Job{Cmd: "echo 2", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
			inserts, already, err = jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 1)

			tmpPath := managerDBBkFile + ".tmp"
			<-wait
			_, err = os.Stat(tmpPath)
			So(err, ShouldBeNil)

			<-wait

			info, err := os.Stat(config.ManagerDBFile)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 65536) // don't know if this will be consistent across platforms and versions...
			info2, err := os.Stat(managerDBBkFile)
			So(err, ShouldBeNil)
			So(info2.Size(), ShouldEqual, 32768) // *** don't know why it's so much smaller...
			_, err = os.Stat(tmpPath)
			So(err, ShouldNotBeNil)

			Convey("You can create manual backups that work correctly", func() {
				manualBackup := managerDBBkFile + ".manual"
				err = jq.BackupDB(manualBackup)
				So(err, ShouldBeNil)
				info3, err := os.Stat(manualBackup)
				So(err, ShouldBeNil)
				So(info3.Size(), ShouldEqual, 32768)

				server.Stop(ctx, true)
				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err := jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 0)

				server.Stop(ctx, true)
				err = os.Rename(manualBackup, config.ManagerDBFile)
				So(err, ShouldBeNil)
				wipeDevDBOnInit = false

				defer func() {
					wipeDevDBOnInit = true
				}()
				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 2)
			})

			Convey("You can stop the server, delete or corrupt the database, and it will be restored from backup", func() {
				jobsByRepGroup, err := jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 2)

				server.Stop(ctx, true)
				wipeDevDBOnInit = false
				defer func() {
					wipeDevDBOnInit = true
				}()
				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 2)

				server.Stop(ctx, true)
				os.Remove(config.ManagerDBFile)
				_, err = os.Stat(config.ManagerDBFile)
				So(err, ShouldNotBeNil)
				_, err = os.Stat(managerDBBkFile)
				So(err, ShouldBeNil)

				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)

				info, err = os.Stat(config.ManagerDBFile)
				So(err, ShouldBeNil)
				So(info.Size(), ShouldEqual, 32768)
				info2, err = os.Stat(managerDBBkFile)
				So(err, ShouldBeNil)
				So(info2.Size(), ShouldEqual, 32768)

				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 2)

				server.Stop(ctx, true)
				f, err := os.OpenFile(config.ManagerDBFile, os.O_TRUNC|os.O_RDWR, dbFilePermission)
				So(err, ShouldBeNil)
				_, err = f.WriteString("corrupt!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
				So(err, ShouldBeNil)
				err = f.Sync()
				So(err, ShouldBeNil)
				err = f.Close()
				So(err, ShouldBeNil)

				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)

				info, err = os.Stat(config.ManagerDBFile)
				So(err, ShouldBeNil)
				So(info.Size(), ShouldEqual, 32768)
				info2, err = os.Stat(managerDBBkFile)
				So(err, ShouldBeNil)
				So(info2.Size(), ShouldEqual, 32768)

				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 2)
			})

			Convey("You can reserve & execute just 1 of the jobs, stop the server, restart it, and then reserve & execute the other", func() {
				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job.Cmd, ShouldEqual, "echo 1")
				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldBeNil)
				So(job.State, ShouldEqual, JobStateComplete)
				So(job.Exited, ShouldBeTrue)
				So(job.Exitcode, ShouldEqual, 0)

				server.Stop(ctx, true)
				wipeDevDBOnInit = false
				server, _, token, errs = serve(ctx, serverConfig)
				wipeDevDBOnInit = true
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				job, err = jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.Cmd, ShouldEqual, "echo 2")
				err = jq.Execute(ctx, job, config.RunnerExecShell)
				So(err, ShouldBeNil)
				So(job.State, ShouldEqual, JobStateComplete)
				So(job.Exited, ShouldBeTrue)
				So(job.Exitcode, ShouldEqual, 0)
			})
		})

		Convey("You can connect, add a job, then immediately shutdown, and the db backup still completes", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			server.db.slowBackups = true
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo 1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)
			server.Stop(ctx, true)

			info, err := os.Stat(config.ManagerDBFile)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 32768)
			info2, err := os.Stat(managerDBBkFile)
			So(err, ShouldBeNil)
			So(info2.Size(), ShouldEqual, 28672)

			Convey("You can restart the server with that existing job, delete it, and it stays deleted when restoring from backup", func() {
				wipeDevDBOnInit = false
				defer func() {
					wipeDevDBOnInit = true
				}()
				errr := os.Remove(config.ManagerDBFile)
				So(errr, ShouldBeNil)
				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				job, err := jq.Reserve(15 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				errb := jq.Bury(job, nil, "")
				So(errb, ShouldBeNil)
				deleted, err := jq.Delete([]*JobEssence{{JobKey: job.Key()}})
				server.Stop(ctx, true)
				So(deleted, ShouldEqual, 1)
				So(err, ShouldBeNil)

				errr = os.Remove(config.ManagerDBFile)
				So(errr, ShouldBeNil)
				server, _, token, errs = serve(ctx, serverConfig)
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				job, err = jq.Reserve(15 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job, ShouldBeNil)
			})
		})

		Convey("You can connect and add a non-instant job", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			job1Cmd := "sleep 1 && echo noninstant"
			jobs = append(jobs, &Job{Cmd: job1Cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "nij"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			Convey("You can reserve & execute the job, drain the server, add a new job while draining, restart it, and then reserve & execute the new one", func() {
				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job.Cmd, ShouldEqual, job1Cmd)
				go execute(ctx, jq, job, config.RunnerExecShell)
				So(job.Exited, ShouldBeFalse)

				running, etc, err := jq.DrainServer()
				So(err, ShouldBeNil)
				So(running, ShouldEqual, 1)
				So(etc.Minutes(), ShouldBeLessThanOrEqualTo, 30)

				jobs = append(jobs, &Job{Cmd: "echo added", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "nij"})
				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 1)

				job2, err := jq.Reserve(10 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job2, ShouldBeNil)

				<-time.After(3 * time.Second)

				_, err = jq.Ping(10 * time.Millisecond)
				So(err, ShouldNotBeNil)

				wipeDevDBOnInit = false
				server, _, token, errs = serve(ctx, serverConfig)
				wipeDevDBOnInit = true
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				job, err = jq.GetByEssence(&JobEssence{Cmd: job1Cmd}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.Exited, ShouldBeTrue)
				So(job.Exitcode, ShouldEqual, 0)

				job2, err = jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job2, ShouldNotBeNil)
				So(job2.Cmd, ShouldEqual, "echo added")
				err = jq.Execute(ctx, job2, config.RunnerExecShell)
				So(err, ShouldBeNil)
				So(job2.State, ShouldEqual, JobStateComplete)
				So(job2.Exited, ShouldBeTrue)
				So(job2.Exitcode, ShouldEqual, 0)
			})

			Convey("You can reserve & execute the job, shut down the server and then can't add new jobs and the started job gets lost and eventually completes", func() {
				job, err := jq.Reserve(50 * time.Millisecond)
				So(err, ShouldBeNil)
				So(job.Cmd, ShouldEqual, job1Cmd)
				started := make(chan bool)
				done := make(chan error)
				go func() {
					started <- true
					erre := jq.Execute(ctx, job, config.RunnerExecShell)
					done <- erre
				}()
				So(job.Exited, ShouldBeFalse)

				<-started
				<-time.After(200 * time.Millisecond)
				ok := jq.ShutdownServer()
				So(ok, ShouldBeTrue)

				jobs = append(jobs, &Job{Cmd: "echo added", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "nij"})
				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldNotBeNil)

				_, err = jq.Ping(10 * time.Millisecond)
				So(err, ShouldNotBeNil)

				err = jq.Disconnect()
				if err != nil {
					So(err.Error(), ShouldEqual, "connection closed")
				}

				wipeDevDBOnInit = false
				server, _, token, errs = serve(ctx, serverConfig)
				startedAt := time.Now()
				wipeDevDBOnInit = true
				So(errs, ShouldBeNil)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				job, err = jq.GetByEssence(&JobEssence{Cmd: job1Cmd}, false, false)
				So(err, ShouldBeNil)

				shouldBeLost := false
				if time.Since(startedAt) > ServerItemTTR {
					shouldBeLost = true
				}

				job.RLock()
				notLost := false
				if job.Exited {
					// sometimes the existing runner manages to reconnect to the
					// new server before this test
					So(job.Lost, ShouldEqual, shouldBeLost)
					if !job.Lost {
						notLost = true
					}
				} else {
					So(job.Exited, ShouldBeFalse)
				}
				job.RUnlock()

				erre := <-done
				So(erre, ShouldNotBeNil)
				So(erre.Error(), ShouldContainSubstring, "recovered on a new server") // or "receive time out"?

				job, err = jq.GetByEssence(&JobEssence{Cmd: job1Cmd}, false, false)
				So(err, ShouldBeNil)
				job.RLock()
				So(job.Exited, ShouldBeTrue)

				shouldBeLost = false
				if !notLost && time.Since(startedAt) > ServerItemTTR {
					shouldBeLost = true
				}
				So(job.Lost, ShouldEqual, shouldBeLost)
				job.RUnlock()
			})
		})

		Convey("You can connect and add a failing job that stays buried after a restart", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			job1Cmd := "false"
			jobs = append(jobs, &Job{Cmd: job1Cmd, Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), RepGroup: "false"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job.Cmd, ShouldEqual, job1Cmd)
			err = jq.Execute(ctx, job, config.RunnerExecShell)
			So(err, ShouldNotBeNil)
			So(job.Exited, ShouldBeTrue)

			job, err = jq.GetByEssence(&JobEssence{Cmd: job1Cmd}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Exited, ShouldBeTrue)
			So(job.State, ShouldEqual, JobStateBuried)

			ok := jq.ShutdownServer()
			So(ok, ShouldBeTrue)

			err = jq.Disconnect()
			So(err, ShouldBeNil)

			wipeDevDBOnInit = false
			server, _, token, errs = serve(ctx, serverConfig)
			wipeDevDBOnInit = true
			So(errs, ShouldBeNil)
			jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)

			job, err = jq.GetByEssence(&JobEssence{Cmd: job1Cmd}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.Exited, ShouldBeTrue)
			So(job.State, ShouldEqual, JobStateBuried)
		})

		Reset(func() {
			server.Stop(ctx, true)
		})
	})
}

func TestJobqueueRunners(t *testing.T) {
	ctx := context.Background()

	if servermode {
		return
	}
	runtime.GOMAXPROCS(runtime.NumCPU())
	if runnermode {
		// we have a full test of Serve() below that needs a client executable;
		// we say this test script is that exe, and when --runnermode is passed
		// to us we skip all tests and just act like a runner
		runner(ctx)
		return
	}
	config, serverConfig, addr, _, clientConnectTime := jobqueueTestInit(true)

	defer os.RemoveAll(filepath.Join(os.TempDir(), AppName+"_cwd"))

	// start these tests anew because these tests have the server spawn runners
	Convey("Once a new jobqueue server is up", t, func() {
		ServerItemTTR = 10 * time.Second
		ServerCheckRunnerTime = 2 * time.Second
		ClientTouchInterval = 50 * time.Millisecond
		pwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		runnertmpdir, err := os.MkdirTemp(pwd, "wr_jobqueue_test_runner_dir_")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(runnertmpdir)

		// our runnerCmd will be running ourselves in --runnermode, so first
		// we'll compile ourselves to the tmpdir
		runnerCmd := filepath.Join(runnertmpdir, "runner")
		cmd := exec.Command("go", "test", "-tags", "netgo", "-run", "TestJobqueue", "-c", "-o", runnerCmd)
		err = cmd.Run()
		if err != nil {
			log.Fatal(err)
		}

		runningConfig := serverConfig
		runningConfig.RunnerCmd = runnerCmd + " --runnermode --schedgrp '%s' --rdeployment %s --rserver '%s' --rdomain %s --rtimeout %d --maxmins %d --tmpdir " + runnertmpdir
		server, _, token, errs := serve(ctx, runningConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		maxCPU := runtime.NumCPU()
		runtime.GOMAXPROCS(maxCPU)

		Convey("You can connect, and add a job and then manually kill both the runner and process", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			cmd := "perl -e 'for (1..20) { sleep(1) }'"
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "sleep", Requirements: &jqs.Requirements{RAM: 1, Time: 20 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for the job to start running
			started := make(chan bool, 1)
			go func() {
				limit := time.After(10 * time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateRunning, false, false)
						if err != nil {
							continue
						}
						if len(jobs) == 1 {
							ticker.Stop()
							started <- true

							return
						}

						continue
					case <-limit:
						ticker.Stop()
						started <- false

						return
					}
				}
			}()
			So(<-started, ShouldBeTrue)

			jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateRunning, false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)

			// initially, we force us to fail to be able to check if the job
			// is really dead or not, so that we can test this scenario
			ServerLostJobCheckTimeout = 1 * time.Nanosecond
			ServerLostJobCheckRetryTime = 2 * time.Second

			defer func() {
				ServerLostJobCheckTimeout = 5 * time.Second
				ServerLostJobCheckRetryTime = 1 * time.Hour
			}()

			ec := exec.Command("bash", "-c", fmt.Sprintf("ps -o 'pgid' -p %d | tail -n 1", jobs[0].Pid))
			out, err := ec.CombinedOutput()
			So(err, ShouldBeNil)
			pgid, err := strconv.Atoi(strings.TrimSpace(string(out)))
			So(err, ShouldBeNil)
			syscall.Kill(-pgid, syscall.SIGKILL)

			// wait for the job to become lost and then buried
			killed := make(chan bool, 1)
			checkLost := true
			var timeToBury time.Duration
			go func() {
				var lostTime time.Time
				limit := time.After(ServerItemTTR + 5*time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						if checkLost {
							jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateLost, false, false)
							if err != nil {
								continue
							}

							if len(jobs) == 1 {
								checkLost = false
								lostTime = time.Now()

								// re-enable our ability to check the job is
								// really dead
								ServerLostJobCheckTimeout = 5 * time.Second
							} else {
								continue
							}
						}

						jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
						if err != nil {
							continue
						}
						if len(jobs) == 1 {
							ticker.Stop()
							timeToBury = time.Since(lostTime)
							killed <- true

							return
						}

						continue
					case <-limit:
						ticker.Stop()
						jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", true, false)
						timelimitDebug(jobs, err)
						killed <- false

						return
					}
				}
			}()
			So(<-killed, ShouldBeTrue)

			jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)
			So(jobs[0].State, ShouldEqual, JobStateBuried)
			So(jobs[0].FailReason, ShouldEqual, FailReasonLost)
			So(jobs[0].Exitcode, ShouldEqual, -1)
			So(timeToBury, ShouldBeGreaterThan, ServerLostJobCheckRetryTime)
		})

		Convey("You can connect, and add jobs with limits, and they run without delays", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			count := 3
			for i := 1; i <= count; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo %d && sleep 1", i), Cwd: "/tmp", CwdMatters: true, ReqGroup: "limitedA", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0}, Retries: uint8(0), Override: uint8(2), RepGroup: "limited", LimitGroups: []string{"a:5", "b:1"}})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, count)
			So(already, ShouldEqual, 0)

			waitForCompletion := func(n int) bool {
				limit := time.After(10 * time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						jobs, err = jq.GetByRepGroup("limited", false, 0, JobStateComplete, false, false)
						if err != nil {
							continue
						}
						if len(jobs) == n {
							ticker.Stop()
							return true
						}
						continue
					case <-limit:
						ticker.Stop()
						return false
					}
				}
			}

			// wait for 1 job to complete, then add a job with overlapping
			// limitgroups and different requirements
			waitForCompletion(1)

			jobs = []*Job{}
			jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo %d && sleep 1", count+1), Cwd: "/tmp", CwdMatters: true, ReqGroup: "limitedB", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "limited", LimitGroups: []string{"c:5", "b:1"}})
			inserts, already, err = jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// the remaining jobs should complete in about count seconds, ie. no
			// delay between finishing the 0 cpu jobs, and starting the 1 cpu
			// job. If this is not working due to a bug, it takes
			// ServerCheckRunnerTime longer. When working, it takes an
			// additional runner timeout (1s) due to fact the first runner uses
			// up the limit for that long while waiting to reserve from the now
			// empty queue for its group. There's also a little overhead.
			t := time.Now()
			waitForCompletion(count + 1)
			So(time.Since(t), ShouldBeLessThan, time.Duration((count*1100)+1000)*time.Millisecond)
		})

		Convey("You can connect, and add some jobs where reserved resources depend on override", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			zeroReq := &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0}
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "fallocate -l 200M foo && echo 1", Cwd: tmpdir, ReqGroup: "fallocate", Requirements: zeroReq, Retries: uint8(0), Override: uint8(2), RepGroup: "fallocate"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// run the first job by itself, so learning occurs (even when disk
			// is 0 and override is 2)
			waitToFinish := func() bool {
				done := make(chan bool, 1)
				go func() {
					limit := time.After(10 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
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
				return <-done
			}

			waitToFinish()
			complete, errj := jq.GetByRepGroup("fallocate", false, 0, JobStateComplete, false, false)
			So(errj, ShouldBeNil)
			So(len(complete), ShouldEqual, 1)
			So(complete[0].Requirements, ShouldResemble, zeroReq)
			So(complete[0].PeakDisk, ShouldEqual, 200)

			// add 3 similar jobs that only really differ in override behaviour
			jobs = append(jobs, &Job{Cmd: "fallocate -l 200M foo && echo 2", Cwd: tmpdir, ReqGroup: "fallocate", Requirements: zeroReq, Retries: uint8(0), Override: uint8(0), RepGroup: "learns"})
			jobs = append(jobs, &Job{Cmd: "fallocate -l 200M foo && echo 3", Cwd: tmpdir, ReqGroup: "fallocate", Requirements: zeroReq, Retries: uint8(0), Override: uint8(2), RepGroup: "learnsDiskNotMem"})
			// following is the main test: specifying Disk of 0 and override 2
			// should result in 0 overriding learned value, even though its a
			// zero value, if DiskSet is true
			notOverrideReq := &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0, Disk: 0}
			overrideReq := &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0, Disk: 0, DiskSet: true}
			jobs = append(jobs, &Job{Cmd: "fallocate -l 200M foo && echo 4", Cwd: tmpdir, ReqGroup: "fallocate", Requirements: notOverrideReq, Retries: uint8(0), Override: uint8(2), RepGroup: "learnsDiskNotMem2"})
			jobs = append(jobs, &Job{Cmd: "fallocate -l 200M foo && echo 5", Cwd: tmpdir, ReqGroup: "fallocate", Requirements: overrideReq, Retries: uint8(0), Override: uint8(2), RepGroup: "nolearning"})

			inserts, already, err = jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 4)
			So(already, ShouldEqual, 1)

			waitToFinish()

			complete, errj = jq.GetByRepGroup("learns", false, 0, JobStateComplete, false, false)
			So(errj, ShouldBeNil)
			So(len(complete), ShouldEqual, 1)
			So(complete[0].Requirements, ShouldNotResemble, zeroReq)
			So(complete[0].Requirements.Disk, ShouldEqual, 1)
			So(complete[0].Requirements.RAM, ShouldEqual, 100)
			So(complete[0].PeakDisk, ShouldEqual, 200)

			complete, errj = jq.GetByRepGroup("learnsDiskNotMem", false, 0, JobStateComplete, false, false)
			So(errj, ShouldBeNil)
			So(len(complete), ShouldEqual, 1)
			So(complete[0].Requirements, ShouldNotResemble, zeroReq)
			So(complete[0].Requirements.Disk, ShouldEqual, 1)
			So(complete[0].Requirements.RAM, ShouldEqual, 1)
			So(complete[0].PeakDisk, ShouldEqual, 200)

			complete, errj = jq.GetByRepGroup("learnsDiskNotMem2", false, 0, JobStateComplete, false, false)
			So(errj, ShouldBeNil)
			So(len(complete), ShouldEqual, 1)
			So(complete[0].Requirements, ShouldNotResemble, zeroReq)
			So(complete[0].Requirements.Disk, ShouldEqual, 1)
			So(complete[0].Requirements.RAM, ShouldEqual, 1)
			So(complete[0].PeakDisk, ShouldEqual, 200)

			complete, errj = jq.GetByRepGroup("nolearning", false, 0, JobStateComplete, false, false)
			So(errj, ShouldBeNil)
			So(len(complete), ShouldEqual, 1)
			So(complete[0].Requirements, ShouldResemble, overrideReq)
			So(complete[0].PeakDisk, ShouldEqual, 200)
		})

		Convey("You can connect, and add a job that you can kill while it's running", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			cmd := "perl -e 'for (1..20) { sleep(1) }'"
			jobs = append(jobs, &Job{Cmd: cmd, Cwd: "/tmp", ReqGroup: "sleep", Requirements: &jqs.Requirements{RAM: 1, Time: 20 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for the job to start running
			started := make(chan bool, 1)
			go func() {
				limit := time.After(10 * time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateRunning, false, false)
						if err != nil {
							continue
						}
						if len(jobs) == 1 {
							ticker.Stop()
							started <- true
							return
						}
						continue
					case <-limit:
						ticker.Stop()
						started <- false
						return
					}
				}
			}()
			So(<-started, ShouldBeTrue)

			killCount, err := jq.Kill([]*JobEssence{{Cmd: cmd}})
			So(err, ShouldBeNil)
			So(killCount, ShouldEqual, 1)

			// wait for the job to get killed
			killed := make(chan bool, 1)
			go func() {
				limit := time.After(25 * time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
						if err != nil {
							continue
						}
						if len(jobs) == 1 {
							ticker.Stop()
							killed <- true
							return
						}
						continue
					case <-limit:
						ticker.Stop()
						jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", true, false)
						timelimitDebug(jobs, err)
						killed <- false
						return
					}
				}
			}()
			So(<-killed, ShouldBeTrue)

			jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 1)
			So(jobs[0].State, ShouldEqual, JobStateBuried)
			So(jobs[0].FailReason, ShouldEqual, FailReasonKilled)
			So(jobs[0].Exitcode, ShouldEqual, -1)
		})

		Convey("You can connect, and add some real jobs", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			var jobs []*Job
			count := maxCPU * 2
			for i := 0; i < count; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("perl -e 'open($fh, q[>%d]); print $fh q[foo]; close($fh)'", i), Cwd: tmpdir, ReqGroup: "perl", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), Override: 2, RepGroup: "manually_added"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, count)
			So(already, ShouldEqual, 0)

			Convey("After some time the jobs get automatically run", func() {
				// wait for the jobs to get run
				done := make(chan bool, 1)
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
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
				So(<-done, ShouldBeTrue) // we shouldn't have hit our time limit

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, count)
				ran := 0
				for _, job := range jobs {
					files, err := os.ReadDir(job.ActualCwd)
					if err != nil {
						log.Fatal(err)
					}
					for range files {
						ran++
					}
				}
				So(ran, ShouldEqual, count)

				// we shouldn't have executed any unnecessary runners, and those
				// we did run should have exited without error, even if there
				// were no more jobs left
				files, err := os.ReadDir(runnertmpdir)
				if err != nil {
					log.Fatal(err)
				}
				ranClean := 0
				for range files {
					ranClean++
				}
				So(ranClean, ShouldEqual, maxCPU+1) // +1 for the runner exe
			})
		})

		Convey("You can connect, and add some jobs with fractional CPU requirements", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			var jobs []*Job
			count := maxCPU * 2
			for i := 0; i < count; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("sleep 2 && perl -e 'open($fh, q[>%d]); print $fh q[foo]; close($fh)'", i), Cwd: tmpdir, ReqGroup: "perl", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0.5}, Retries: uint8(0), RepGroup: "manually_added"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, count)
			So(already, ShouldEqual, 0)

			Convey("After some time the jobs get automatically run", func() {
				// wait for the jobs to get run
				done := make(chan bool, 1)
				var simultaneous int
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
								ticker.Stop()
								done <- true
								return
							}
							running, errj := jq.GetByRepGroup("manually_added", false, 0, JobStateRunning, false, false)
							if errj == nil && len(running) > simultaneous {
								simultaneous = len(running)
							}
							continue
						case <-limit:
							ticker.Stop()
							done <- false
							return
						}
					}
				}()
				So(<-done, ShouldBeTrue) // we shouldn't have hit our time limit
				So(simultaneous, ShouldBeGreaterThan, maxCPU)

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, count)
				ran := 0
				for _, job := range jobs {
					files, err := os.ReadDir(job.ActualCwd)
					if err != nil {
						log.Fatal(err)
					}
					for range files {
						ran++
					}
				}
				So(ran, ShouldEqual, count)

				files, err := os.ReadDir(runnertmpdir)
				if err != nil {
					log.Fatal(err)
				}
				ranClean := 0
				for range files {
					ranClean++
				}
				So(ranClean, ShouldEqual, count+1) // +1 for the runner exe
			})
		})

		Convey("You can connect, and add some 0 CPU jobs, which are limited by memory", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			maxMem, errp := internal.ProcMeminfoMBs()
			So(errp, ShouldBeNil)

			var jobs []*Job
			jobMB := int(math.Floor(float64(maxMem) / float64(maxCPU*2)))
			count := maxCPU * 3
			for i := 0; i < count; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("sleep 2 && perl -e 'open($fh, q[>%d]); print $fh q[foo]; close($fh)'", i), Cwd: tmpdir, ReqGroup: "perl", Requirements: &jqs.Requirements{RAM: jobMB, Time: 1 * time.Second, Cores: 0}, Retries: uint8(0), Override: 2, RepGroup: "manually_added"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, count)
			So(already, ShouldEqual, 0)

			Convey("After some time the jobs get automatically run", func() {
				// wait for the jobs to get run
				done := make(chan bool, 1)
				var simultaneous int
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
								ticker.Stop()
								done <- true
								return
							}
							running, errj := jq.GetByRepGroup("manually_added", false, 0, JobStateRunning, false, false)
							if errj == nil && len(running) > simultaneous {
								simultaneous = len(running)
							}
							continue
						case <-limit:
							ticker.Stop()
							gjobs, errj := jq.GetByRepGroup("manually_added", false, 0, "", true, false)
							timelimitDebug(gjobs, errj)
							done <- false
							return
						}
					}
				}()
				So(<-done, ShouldBeTrue) // we shouldn't have hit our time limit
				So(simultaneous, ShouldBeBetweenOrEqual, maxCPU, maxCPU*2)

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, count)
				ran := 0
				for _, job := range jobs {
					files, err := os.ReadDir(job.ActualCwd)
					if err != nil {
						log.Fatal(err)
					}
					for range files {
						ran++
					}
				}
				So(ran, ShouldEqual, count)
			})
		})

		Convey("You can connect, and add a job that buries with no retries", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo 1 && false", Cwd: tmpdir, ReqGroup: "echo", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			Convey("After some time the jobs get automatically run without excess runners", func() {
				// wait for the jobs to get run
				done := make(chan bool, 1)
				waitForBury := func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(50 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
							if err != nil {
								continue
							}
							if len(jobs) == 1 {
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
				}
				go waitForBury()
				So(<-done, ShouldBeTrue)

				jobs = nil
				jobs = append(jobs, &Job{Cmd: "echo 2 && true", Cwd: tmpdir, ReqGroup: "echo", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "manually_added"})
				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				done2 := make(chan bool, 1)
				waitForNoRunners := func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
								ticker.Stop()
								done2 <- true
								return
							}
							continue
						case <-limit:
							ticker.Stop()
							done2 <- false
							return
						}
					}
				}
				go waitForNoRunners()
				So(<-done2, ShouldBeTrue)

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateBuried, false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 1)
				buriedKey := jobs[0].Key()
				jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateComplete, false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 1)

				// we shouldn't have executed any unnecessary runners
				files, err := os.ReadDir(runnertmpdir)
				if err != nil {
					log.Fatal(err)
				}
				ranClean := 0
				for _, file := range files {
					if !strings.HasPrefix(file.Name(), "ok") {
						continue
					}
					ranClean++
				}
				So(ranClean, ShouldEqual, 1)

				Convey("Retrying buried jobs works multiple times in a row", func() {
					kicked, err := jq.Kick([]*JobEssence{{JobKey: buriedKey}})
					So(err, ShouldBeNil)
					So(kicked, ShouldEqual, 1)

					go waitForBury()
					So(<-done, ShouldBeTrue)

					kicked, err = jq.Kick([]*JobEssence{{JobKey: buriedKey}})
					So(err, ShouldBeNil)
					So(kicked, ShouldEqual, 1)

					go waitForBury()
					So(<-done, ShouldBeTrue)

					go waitForNoRunners()
					So(<-done2, ShouldBeTrue)
				})
			})
		})

		if maxCPU > 2 {
			Convey("You can connect and add jobs in alternating scheduler groups and they don't pend", func() {
				jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				defer disconnect(jq)

				req1 := &jqs.Requirements{RAM: 10, Time: 4 * time.Second, Cores: 1}
				jobs := []*Job{{Cmd: "echo 1 && sleep 4", Cwd: "/tmp", ReqGroup: "req1", Requirements: req1, RepGroup: "a"}}
				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				<-time.After(1 * time.Second)

				job, err := jq.GetByEssence(&JobEssence{Cmd: "echo 1 && sleep 4"}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.State, ShouldEqual, JobStateRunning)

				jobs = []*Job{{Cmd: "echo 2 && sleep 3", Cwd: "/tmp", ReqGroup: "req2", Requirements: &jqs.Requirements{RAM: 10, Time: 4 * time.Hour, Cores: 1}, RepGroup: "a"}}
				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				<-time.After(1 * time.Second)

				job, err = jq.GetByEssence(&JobEssence{Cmd: "echo 2 && sleep 3"}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.State, ShouldEqual, JobStateRunning)

				jobs = []*Job{{Cmd: "echo 3 && sleep 2", Cwd: "/tmp", ReqGroup: "req1", Requirements: req1, RepGroup: "a"}}
				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				<-time.After(1 * time.Second)

				job, err = jq.GetByEssence(&JobEssence{Cmd: "echo 3 && sleep 2"}, false, false)
				So(err, ShouldBeNil)
				So(job, ShouldNotBeNil)
				So(job.State, ShouldEqual, JobStateRunning)

				// let them all complete
				done := make(chan bool, 1)
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if !server.HasRunners(ctx) {
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
				So(<-done, ShouldBeTrue)
			})
		} else {
			SkipConvey("Skipping a test that needs at least 3 cores", func() {})
		}

		if runtime.NumCPU() >= 2 {
			Convey("You can connect, and add 2 real jobs with the same reqs sequentially that run simultaneously", func() {
				jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				defer disconnect(jq)

				jobs := []*Job{{Cmd: fmt.Sprintf("perl -e 'print q[%s2sim%d]; sleep(5);'", runnertmpdir, 1), Cwd: runnertmpdir, ReqGroup: "perl2sim", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"}}

				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				// wait for this first command to start running
				running := make(chan bool, 1)
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							pids, errf := process.Pids()
							if errf == nil {
								for _, pid := range pids {
									p, errf := process.NewProcess(pid)
									if errf == nil {
										cmd, errf := p.Cmdline()
										if errf == nil {
											if strings.Contains(cmd, runnertmpdir+"2sim") {
												status, errf := p.Status()
												if errf == nil && status == "S" {
													ticker.Stop()
													running <- true
													return
												}
											}
										}
									}
								}
							}
							if !server.HasRunners(ctx) {
								ticker.Stop()
								running <- false
								return
							}
							continue
						case <-limit:
							ticker.Stop()
							running <- false
							return
						}
					}
				}()
				So(<-running, ShouldBeTrue)

				jobs = []*Job{{Cmd: fmt.Sprintf("perl -e 'print q[%s2sim%d]; sleep(5);'", runnertmpdir, 2), Cwd: runnertmpdir, ReqGroup: "perl2sim", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"}}

				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				// wait for the jobs to get run, and while waiting we'll check to
				// see if we get both of our commands running at once
				numRanSimultaneously := make(chan int, 1)
				go func() {
					limit := time.After(30 * time.Second)
					ticker := time.NewTicker(500 * time.Millisecond)
					maxSimultaneous := 0
					for {
						select {
						case <-ticker.C:
							pids, err := process.Pids()
							if err == nil {
								simultaneous := 0
								for _, pid := range pids {
									p, err := process.NewProcess(pid)
									if err == nil {
										cmd, err := p.Cmdline()
										if err == nil {
											if strings.Contains(cmd, runnertmpdir+"2sim") {
												status, err := p.Status()
												if err == nil && status == "S" {
													simultaneous++
												}
											}
										}
									}
								}
								if simultaneous > maxSimultaneous {
									maxSimultaneous = simultaneous
								}
							}
							if !server.HasRunners(ctx) {
								ticker.Stop()
								numRanSimultaneously <- maxSimultaneous
								return
							}
							continue
						case <-limit:
							ticker.Stop()
							numRanSimultaneously <- maxSimultaneous
							return
						}
					}
				}()
				So(<-numRanSimultaneously, ShouldEqual, 2)
			})
		}

		Convey("You can connect, and add 2 large batches of jobs sequentially", func() {
			lsfMode := false
			count := 1000
			count2 := 100

			batchtest := func() {
				clientConnectTime = 20 * time.Second // it takes a long time with -race to add 10000 jobs...
				jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)
				defer disconnect(jq)

				tmpdir, err := os.MkdirTemp(pwd, "wr_jobqueue_test_output_dir_")
				if err != nil {
					log.Fatal(err)
				}
				defer os.RemoveAll(tmpdir)

				req := &jqs.Requirements{RAM: 300, Time: 1 * time.Second, Cores: 1}
				var jobs []*Job
				for i := 0; i < count; i++ {
					jobs = append(jobs, &Job{Cmd: fmt.Sprintf("perl -e 'open($fh, q[>batch1.%d]); print $fh q[foo]; close($fh)'", i), Cwd: tmpdir, ReqGroup: "perl", Requirements: req, Retries: uint8(3), RepGroup: "manually_added"})
				}
				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, count)
				So(already, ShouldEqual, 0)

				// wait for 101 of them to complete
				done := make(chan bool, 1)
				fourHundredCount := 0
				go func() {
					limit := time.After(45 * time.Second)
					ticker := time.NewTicker(50 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateComplete, false, false)
							if err != nil {
								continue
							}
							ran := 0
							for _, job := range jobs {
								files, errf := os.ReadDir(job.ActualCwd)
								if errf != nil {
									log.Fatalf("job [%s] had actual cwd %s: %s\n", job.Cmd, job.ActualCwd, errf)
								}
								for range files {
									ran++
								}
							}
							if ran > 100 {
								ticker.Stop()
								done <- true
								return
							} else if fourHundredCount == 0 {
								server.psgmutex.RLock()
								if group, existed := server.previouslyScheduledGroups["400:30:1:0"]; existed {
									fourHundredCount = group.count
								}
								server.psgmutex.RUnlock()
							}
							continue
						case <-limit:
							ticker.Stop()
							done <- false
							return
						}
					}
				}()
				So(<-done, ShouldBeTrue)
				So(fourHundredCount, ShouldBeBetweenOrEqual, count/2, count)

				// now add a new batch of jobs with the same reqs and reqgroup
				jobs = nil
				for i := 0; i < count2; i++ {
					jobs = append(jobs, &Job{Cmd: fmt.Sprintf("perl -e 'open($fh, q[>batch2.%d]); print $fh q[foo]; close($fh)'", i), Cwd: tmpdir, ReqGroup: "perl", Requirements: req, Retries: uint8(3), RepGroup: "manually_added"})
				}
				inserts, already, err = jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, count2)
				So(already, ShouldEqual, 0)

				// wait for all the jobs to get run
				done = make(chan bool, 1)
				twoHundredCount := 0
				go func() {
					limit := time.After(360 * time.Second)
					ticker := time.NewTicker(50 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if twoHundredCount > 0 && !server.HasRunners(ctx) {
								// check they're really all complete, since the
								// switch to a new job array could leave us with no
								// runners temporarily
								jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateComplete, false, false)
								if err == nil && len(jobs) == count+count2 {
									ticker.Stop()
									done <- true
									return
								}
							} else if twoHundredCount == 0 {
								server.psgmutex.RLock()
								if group, existed := server.previouslyScheduledGroups["200:30:1:0"]; existed {
									twoHundredCount = group.count
								}
								server.psgmutex.RUnlock()
							}
							continue
						case <-limit:
							ticker.Stop()
							done <- false
							return
						}
					}
				}()
				So(<-done, ShouldBeTrue)
				So(twoHundredCount, ShouldBeBetween, fourHundredCount/2, count+count2)

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, count+count2)
				ran := 0
				for _, job := range jobs {
					files, err := os.ReadDir(job.ActualCwd)
					if err != nil {
						continue
					}
					for range files {
						ran++
					}
				}
				So(ran, ShouldEqual, count+count2)

				if !lsfMode {
					// we should end up running maxCPU*2 runners, because the first set
					// will be for our given reqs, and the second set will be for when
					// the system learns actual memory usage
					files, err := os.ReadDir(runnertmpdir)
					if err != nil {
						log.Fatal(err)
					}
					So(len(files), ShouldBeBetweenOrEqual, (maxCPU * 2), (maxCPU*2)+2) // *** we can get up to 2 more due to timing issues, but I don't think this is a significant bug...
				} // *** else under LSF we want to test that we never request more than count+count2 runners...
			}

			batchtest()

			// if possible, we want to repeat these tests with the LSF
			// scheduler, which reveals more issues
			_, err := exec.LookPath("lsadmin")
			if err == nil {
				_, err = exec.LookPath("bqueues")
			}

			privateKeyPath := os.Getenv("WR_LSF_TEST_KEY")
			if err == nil && privateKeyPath != "" && os.Getenv("WR_DISABLE_UNRELIABLE_LSF_TESTS") != "true" {
				lsfMode = true
				count = 10000
				count2 = 1000
				lsfConfig := runningConfig
				lsfConfig.SchedulerName = "lsf"
				lsfConfig.SchedulerConfig = &jqs.ConfigLSF{Shell: config.RunnerExecShell, Deployment: "testing", PrivateKeyPath: privateKeyPath}
				server.Stop(ctx, true)
				server, _, token, errs = serve(ctx, lsfConfig)
				So(errs, ShouldBeNil)

				batchtest()
			}
		})

		Reset(func() {
			if server != nil {
				server.Stop(ctx, true)
			}
		})
	})

	// start these tests anew because these tests have the server spawn runners
	// that fail, simulating some network issue
	Convey("Once a new jobqueue server is up with bad runners", t, func() {
		ServerItemTTR = 1 * time.Second
		ServerCheckRunnerTime = 2 * time.Second
		ClientTouchInterval = 50 * time.Millisecond
		pwd, err := os.Getwd()
		if err != nil {
			log.Fatal(err)
		}
		runnertmpdir, err := os.MkdirTemp(pwd, "wr_jobqueue_test_runner_dir_")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(runnertmpdir)

		// our runnerCmd will be running ourselves in --runnermode, so first
		// we'll compile ourselves to the tmpdir
		runnerCmd := filepath.Join(runnertmpdir, "runner")
		cmd := exec.Command("go", "test", "-tags", "netgo", "-run", "TestJobqueue", "-c", "-o", runnerCmd)
		err = cmd.Run()
		if err != nil {
			log.Fatal(err)
		}

		runningConfig := serverConfig
		runningConfig.RunnerCmd = runnerCmd + " --runnermode --runnerfail --schedgrp '%s' --rdeployment %s --rserver '%s' --rdomain %s --rtimeout %d --maxmins %d --tmpdir " + runnertmpdir
		server, _, token, errs := serve(ctx, runningConfig)
		So(errs, ShouldBeNil)
		defer func() {
			server.Stop(ctx, true)
		}()

		Convey("You can connect, and add a job", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "true", Cwd: tmpdir, ReqGroup: "true", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}, Retries: uint8(0), Override: uint8(2), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			Convey("After some time the manager will have tried to spawn runners more than once", func() {
				runnerCheck := func() (runners int) {
					files, errf := os.ReadDir(runnertmpdir)
					if errf != nil {
						log.Fatal(errf)
					}
					ranFailed := 0
					for _, file := range files {
						if !strings.HasPrefix(file.Name(), "fail") {
							continue
						}
						ranFailed++
					}
					return ranFailed
				}

				So(runnerCheck(), ShouldEqual, 0)

				hadRunner := make(chan bool, 1)
				go func() {
					limit := time.After(3 * time.Second)
					ticker := time.NewTicker(100 * time.Millisecond)
					for {
						select {
						case <-ticker.C:
							if server.HasRunners(ctx) {
								ticker.Stop()
								hadRunner <- true
								return
							}
							continue
						case <-limit:
							ticker.Stop()
							hadRunner <- false
							return
						}
					}
				}()
				So(<-hadRunner, ShouldBeTrue)
				<-time.After(1 * time.Second)

				jobs, err = jq.GetByRepGroup("manually_added", false, 0, JobStateReady, false, false)
				So(err, ShouldBeNil)
				So(len(jobs), ShouldEqual, 1)

				So(runnerCheck(), ShouldEqual, 1)

				<-time.After(3 * time.Second)

				So(runnerCheck(), ShouldEqual, 2)

				err = server.Drain(ctx)
				So(err, ShouldBeNil)
				<-time.After(4 * time.Second)
				So(server.HasRunners(ctx), ShouldBeFalse)
			})
		})

		Reset(func() {
			if server != nil {
				server.Stop(ctx, true)
			}
		})
	})
}

func TestJobqueueWithOpenStack(t *testing.T) {
	if runnermode || servermode {
		return
	}

	ctx := context.Background()

	osPrefix := os.Getenv("OS_OS_PREFIX")
	osUser := os.Getenv("OS_OS_USERNAME")
	localUser := os.Getenv("OS_LOCAL_USERNAME")
	flavorRegex := os.Getenv("OS_FLAVOR_REGEX")
	host, err := os.Hostname()
	if err != nil || !strings.HasPrefix(host, "wr-dev-"+localUser) || osPrefix == "" || osUser == "" || flavorRegex == "" {
		SkipConvey("Skipping the OpenStack tests", t, func() {})
		return
	}

	ServerInterruptTime = 10 * time.Millisecond
	ServerReserveTicker = 10 * time.Millisecond
	ClientReleaseDelay = 100 * time.Millisecond
	clientConnectTime := 10 * time.Second
	ServerItemTTR = 1 * time.Second
	ClientTouchInterval = 50 * time.Millisecond

	var server *Server
	var token []byte
	var errs error
	config := internal.ConfigLoadFromParentDir(ctx, internal.Development)

	addr := "localhost:" + config.ManagerPort

	setDomainIP(config.ManagerCertDomain)

	runnertmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_runner_dir_")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(runnertmpdir)

	// our runnerCmd will be running ourselves in --runnermode, so first
	// we'll compile ourselves to the tmpdir
	runnerCmd := filepath.Join(runnertmpdir, "runner")
	cmd := exec.Command("go", "test", "-tags", "netgo", "-run", "TestJobqueue", "-c", "-o", runnerCmd)
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	resourceName := "wr-testing-" + localUser
	cloudConfig := &jqs.ConfigOpenStack{
		ResourceName:         resourceName,
		OSPrefix:             osPrefix,
		OSUser:               osUser,
		OSRAM:                2048,
		FlavorRegex:          flavorRegex,
		FlavorSets:           os.Getenv("OS_FLAVOR_SETS"),
		ServerPorts:          []int{22},
		ServerKeepTime:       3 * time.Second,
		StateUpdateFrequency: 1 * time.Second,
		Shell:                "bash",
		MaxInstances:         -1,
		Umask:                config.ManagerUmask,
	}
	cloudConfig.AddConfigFile(config.ManagerTokenFile + ":~/.wr_" + config.Deployment + "/client.token")
	if config.ManagerCAFile != "" {
		cloudConfig.AddConfigFile(config.ManagerCAFile + ":~/.wr_" + config.Deployment + "/ca.pem")
	}

	osConfig := ServerConfig{
		Port:            config.ManagerPort,
		WebPort:         config.ManagerWeb,
		SchedulerName:   "openstack",
		SchedulerConfig: cloudConfig,
		UploadDir:       config.ManagerUploadDir,
		DBFile:          config.ManagerDBFile,
		DBFileBackup:    config.ManagerDBBkFile,
		TokenFile:       config.ManagerTokenFile,
		CAFile:          config.ManagerCAFile,
		CertFile:        config.ManagerCertFile,
		CertDomain:      config.ManagerCertDomain,
		KeyFile:         config.ManagerKeyFile,
		Deployment:      config.Deployment,
		RunnerCmd:       runnerCmd + " --runnermode --schedgrp '%s' --rdeployment %s --rserver '%s' --rdomain %s --rtimeout %d --maxmins %d --tmpdir " + runnertmpdir,
	}

	dockerInstallScript := `sudo mkdir -p /etc/docker/
sudo bash -c "echo '{ \"bip\": \"192.168.3.3/24\", \"dns\": [\"8.8.8.8\",\"8.8.4.4\"], \"mtu\": 1380 }' > /etc/docker/daemon.json"
sudo DEBIAN_FRONTEND=noninteractive apt-get -yq update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y install apt-transport-https ca-certificates curl gnupg lsb-release && >&2 echo installed deps
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo DEBIAN_FRONTEND=noninteractive apt-get -yq update
sudo >&2 apt-get -y install docker-ce docker-ce-cli containerd.io && >&2 echo installed docker
sudo usermod -aG docker ` + osUser

	Convey("You can connect with an OpenStack scheduler", t, func() {
		server, _, token, errs = serve(ctx, osConfig)
		So(errs, ShouldBeNil)
		defer func() {
			<-time.After(1 * time.Second) // give runners a chance to exit to avoid extraneous warnings
			server.Stop(ctx, true)
		}()

		jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
		So(err, ShouldBeNil)
		defer disconnect(jq)

		waitRun := func(done chan bool) {
			limit := time.After(maxSpawnTime)
			ticker := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-ticker.C:
					got, errg := jq.GetIncomplete(0, JobStateBuried, false, false)
					if errg != nil {
						fmt.Printf("GetIncomplete failed: %s\n", errg)
					}
					if len(got) == 1 {
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
		}

		Convey("You can add a chain of jobs that run quickly one after the other", func() {
			tmpdir, err := os.MkdirTemp("", "wr_jobqueue_test_output_dir_")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(tmpdir)

			zeroReq := &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 0}
			oneReq := &jqs.Requirements{RAM: 1, Time: 1 * time.Second, Cores: 1}
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo 1", Cwd: tmpdir, ReqGroup: "test1", Requirements: zeroReq, Retries: uint8(0), Override: uint8(2), RepGroup: "chain", DepGroups: []string{"1"}})
			d1 := NewDepGroupDependency("1")
			jobs = append(jobs, &Job{Cmd: "echo 2", Cwd: tmpdir, ReqGroup: "test2", Requirements: oneReq, Retries: uint8(0), Override: uint8(2), RepGroup: "chain", DepGroups: []string{"2"}, Dependencies: Dependencies{d1}})
			d2 := NewDepGroupDependency("2")
			jobs = append(jobs, &Job{Cmd: "echo 3", Cwd: tmpdir, ReqGroup: "test3", Requirements: zeroReq, Retries: uint8(0), Override: uint8(2), RepGroup: "chain", DepGroups: []string{"3"}, Dependencies: Dependencies{d2}})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 3)
			So(already, ShouldEqual, 0)

			// wait for the jobs to get run
			done := make(chan bool, 1)
			go func() {
				limit := time.After(30 * time.Second)
				ticker := time.NewTicker(500 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						if !server.HasRunners(ctx) {
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
			So(<-done, ShouldBeTrue)

			jobs, err = jq.GetByRepGroup("chain", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, 3)
			var e1, s2, e2, s3 time.Time
			for _, job := range jobs {
				So(job.State, ShouldEqual, JobStateComplete)
				switch job.Cmd {
				case "echo 1":
					e1 = job.EndTime
				case "echo 2":
					s2 = job.StartTime
					e2 = job.EndTime
				case "echo 3":
					s3 = job.StartTime
				}
			}
			// (the below used to be over a second ; these tests show we
			//  improved the behaviour and now react instantly)
			So(s2.Sub(e1), ShouldBeLessThan, 150*time.Millisecond)
			So(s3.Sub(e2), ShouldBeLessThan, 150*time.Millisecond)
		})

		Convey("You can modify cloud_config_files of a job", func() {
			var jobs []*Job
			other := make(map[string]string)

			rg := "ccfmod"
			ccfmodPath := "/tmp/ccfmod"
			_, erro := os.OpenFile(ccfmodPath, os.O_RDONLY|os.O_CREATE, 0666)
			So(erro, ShouldBeNil)
			defer func() {
				errr := os.Remove(ccfmodPath)
				So(errr, ShouldBeNil)
			}()
			cores := float64(runtime.NumCPU() + 1) // ensure the job doesn't run on this instance
			jobs = append(jobs, &Job{Cmd: "ls " + ccfmodPath, Cwd: "/tmp", ReqGroup: "rg", Requirements: &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: cores, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: rg})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			done := make(chan bool, 1)
			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err := jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)

			jm := NewJobModifer()
			other = make(map[string]string)
			other["cloud_config_files"] = ccfmodPath
			jm.SetRequirements(&jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: cores, Other: other, OtherSet: true})
			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes := jobsToJobEssenses(got)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			kicked, err := jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			// now that the "config" file is copied to where we're trying to
			// ls, the job should complete
			go func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						got, errg := jq.GetByRepGroup(rg, false, 0, JobStateComplete, false, false)
						if errg != nil {
							fmt.Printf("GetIncomplete failed: %s\n", errg)
						}
						if len(got) == 1 {
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
			So(<-done, ShouldBeTrue)
		})

		Convey("You can modify cloud_script of a job", func() {
			var jobs []*Job
			other := make(map[string]string)

			rg := "scmod"
			csmodPath := "/tmp/csmod"
			jobs = append(jobs, &Job{Cmd: "ls " + csmodPath, Cwd: "/tmp", ReqGroup: "rg", Requirements: &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: float64(1), Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: rg})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			done := make(chan bool, 1)
			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err := jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)

			jm := NewJobModifer()
			other = make(map[string]string)
			other["cloud_script"] = "touch " + csmodPath
			jm.SetRequirements(&jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: float64(1), Other: other, OtherSet: true})
			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes := jobsToJobEssenses(got)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			kicked, err := jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			// now that the cloud script touches the file we're trying to
			// ls, the job should complete
			go func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						got, errg := jq.GetByRepGroup(rg, false, 0, JobStateComplete, false, false)
						if errg != nil {
							fmt.Printf("GetIncomplete failed: %s\n", errg)
						}
						if len(got) == 1 {
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
			So(<-done, ShouldBeTrue)
		})

		Convey("You can modify cloud_flavor of a job", func() {
			var jobs []*Job
			other := make(map[string]string)

			cores := runtime.NumCPU()
			p, err := cloud.New(ctx, "openstack", resourceName, filepath.Join(runnertmpdir, "os_resources"))
			So(err, ShouldBeNil)
			flavor, err := p.CheapestServerFlavor(ctx, cores, 2048, flavorRegex)
			So(err, ShouldBeNil)
			flavor, err = p.CheapestServerFlavor(ctx, flavor.Cores+1, 2048, flavorRegex)
			So(err, ShouldBeNil)
			coresMore := flavor.Cores

			rg := "rg"
			jobs = append(jobs, &Job{Cmd: "getconf _NPROCESSORS_ONLN && false", Cwd: "/tmp", ReqGroup: "rg", Requirements: &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: float64(cores), Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: rg})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for the job to run
			done := make(chan bool, 1)
			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err := jq.GetByRepGroup(rg, false, 0, JobStateBuried, true, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			stdout, err := got[0].StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldEqual, fmt.Sprintf("%d", cores))

			jm := NewJobModifer()
			other = make(map[string]string)
			other["cloud_flavor"] = flavor.Name
			jm.SetRequirements(&jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: float64(cores), Other: other, OtherSet: true})
			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes := jobsToJobEssenses(got)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			kicked, err := jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, true, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			stdout, err = got[0].StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldEqual, fmt.Sprintf("%d", coresMore))

			jm = NewJobModifer()
			other = make(map[string]string)
			jm.SetRequirements(&jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: float64(cores), Other: other, OtherSet: true})
			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes = jobsToJobEssenses(got)
			modified, err = jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			<-time.After(4 * time.Second) // wait for the flavor.Name node to terminate, or the job in next test might run on it randomly

			kicked, err = jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, true, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			stdout, err = got[0].StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldEqual, fmt.Sprintf("%d", cores))
		})

		Convey("You can modify MonitorDocker of a job", func() {
			var jobs []*Job
			other := make(map[string]string)
			other["cloud_script"] = dockerInstallScript

			rg := "first_docker"
			jobs = append(jobs, &Job{Cmd: "docker run sendu/usememory:v1 && false", Cwd: "/tmp", ReqGroup: "docker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: rg})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for the job to run
			done := make(chan bool, 1)
			waitRun(done)
			So(<-done, ShouldBeTrue)

			expectedRAM := 2000
			got, err := jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeBetweenOrEqual, 1, 500)

			jm := NewJobModifer()
			jm.SetMonitorDocker("?")
			jes := jobsToJobEssenses(got)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			kicked, err := jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeGreaterThanOrEqualTo, expectedRAM)

			jm = NewJobModifer()
			jm.SetMonitorDocker("")
			modified, err = jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			kicked, err = jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			waitRun(done)
			So(<-done, ShouldBeTrue)

			got, err = jq.GetByRepGroup(rg, false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeBetweenOrEqual, 1, 500)
		})

		Convey("You can run cmds that have fractional or 0 CPU requirements simultaneously on 1 CPU", func() {
			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "sleep 4 && echo 1", Cwd: "/tmp", ReqGroup: "sleep", Requirements: &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 0.9, Disk: 0}, Retries: uint8(0), RepGroup: "fraction"})
			jobs = append(jobs, &Job{Cmd: "sleep 4 && echo 2", Cwd: "/tmp", ReqGroup: "sleep", Requirements: &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 0.1, Disk: 0}, Retries: uint8(0), RepGroup: "fraction"})
			jobs = append(jobs, &Job{Cmd: "sleep 4 && echo 3", Cwd: "/tmp", ReqGroup: "sleep", Requirements: &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 0, Disk: 0}, Retries: uint8(0), RepGroup: "fraction"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 3)
			So(already, ShouldEqual, 0)

			// wait for the jobs to get run
			done := make(chan bool, 1)
			var simultaneous int
			go func() {
				limit := time.After(10 * time.Second)
				ticker := time.NewTicker(50 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						if !server.HasRunners(ctx) {
							ticker.Stop()
							done <- true
							return
						}
						running, errj := jq.GetByRepGroup("fraction", false, 0, JobStateRunning, false, false)
						if errj == nil && len(running) > simultaneous {
							simultaneous = len(running)
						}
						continue
					case <-limit:
						ticker.Stop()
						done <- false
						return
					}
				}
			}()
			So(<-done, ShouldBeTrue)
			So(simultaneous, ShouldEqual, 3)
		})

		Convey("You can run cmds that start docker containers and get correct memory and cpu usage", func() {
			var jobs []*Job
			other := make(map[string]string)
			other["cloud_script"] = dockerInstallScript + "\necho 1"

			jobs = append(jobs, &Job{Cmd: "docker run sendu/usememory:v1", Cwd: "/tmp", ReqGroup: "docker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "first_docker", MonitorDocker: "?"})

			other = make(map[string]string)
			other["cloud_script"] = dockerInstallScript + "\necho 2"
			dockerName := "jobqueue_test." + internal.RandomString()
			jobs = append(jobs, &Job{Cmd: "docker run --name " + dockerName + " sendu/usememory:v1", Cwd: "/tmp", ReqGroup: "docker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "named_docker", MonitorDocker: dockerName})

			other = make(map[string]string)
			other["cloud_script"] = dockerInstallScript + "\necho 3"
			dockerCidFile := "jobqueue_test.cidfile"
			jobs = append(jobs, &Job{Cmd: "docker run --cidfile " + dockerCidFile + " sendu/usecpu:v1 && rm " + dockerCidFile, Cwd: "/tmp", ReqGroup: "docker2", Requirements: &jqs.Requirements{RAM: 1, Time: 5 * time.Second, Cores: 2, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "cidfile_docker", MonitorDocker: dockerCidFile})

			other = make(map[string]string)
			other["cloud_script"] = dockerInstallScript + "\necho 4"
			dockerCidFile = "uuid-20181127.cidfile"
			jobs = append(jobs, &Job{Cmd: "docker run --cidfile " + dockerCidFile + " sendu/usecpu:v1 && rm " + dockerCidFile, Cwd: "/tmp", ReqGroup: "docker2", Requirements: &jqs.Requirements{RAM: 1, Time: 5 * time.Second, Cores: 2, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "cidglob_docker", MonitorDocker: "uuid-*.cidfile"})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 4)
			So(already, ShouldEqual, 0)

			// wait for the jobs to get run
			done := make(chan bool, 1)
			go func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						if !server.HasRunners(ctx) {
							got, errg := jq.GetIncomplete(0, "", false, false)
							if errg != nil {
								fmt.Printf("GetIncomplete failed: %s\n", errg)
							}
							if len(got) == 0 {
								ticker.Stop()
								done <- true
								return
							}
						}
						continue
					case <-limit:
						ticker.Stop()
						done <- false
						return
					}
				}
			}()
			So(<-done, ShouldBeTrue)

			expectedRAM := 2000
			got, err := jq.GetByRepGroup("first_docker", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeGreaterThanOrEqualTo, expectedRAM)
			So(got[0].WallTime(), ShouldBeBetweenOrEqual, 5*time.Second, 25*time.Second)
			So(got[0].CPUtime, ShouldBeLessThan, 5*time.Second)

			got, err = jq.GetByRepGroup("named_docker", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeGreaterThanOrEqualTo, expectedRAM)

			got, err = jq.GetByRepGroup("cidfile_docker", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeLessThan, 100)
			So(got[0].WallTime(), ShouldBeBetweenOrEqual, 5*time.Second, 25*time.Second)
			So(got[0].CPUtime, ShouldBeGreaterThan, 5*time.Second)

			got, err = jq.GetByRepGroup("cidglob_docker", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			So(got[0].PeakRAM, ShouldBeLessThan, 100)
			So(got[0].WallTime(), ShouldBeBetweenOrEqual, 5*time.Second, 25*time.Second)
			So(got[0].CPUtime, ShouldBeGreaterThan, 5*time.Second)

			// *** want to test that when we kill a running job, its docker
			// is also immediately killed...
		})

		Convey("You can run a cmd to get the memory and cpu usage when no docker containers are running", func() {
			var jobs []*Job

			Convey("when docker is not installed", func() {
				jobs = append(jobs, &Job{Cmd: "docker run sendu/usememory:v1", Cwd: "/tmp", ReqGroup: "docker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1}, Override: uint8(2), Retries: uint8(0), RepGroup: "noDocker", MonitorDocker: "?"})

				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 1)
				So(already, ShouldEqual, 0)

				// wait for the jobs to get run
				done := make(chan bool, 1)
				waitRun(done)

				got, err := jq.GetByRepGroup("noDocker", false, 0, JobStateComplete, false, false)
				So(len(got), ShouldBeZeroValue)
				So(err, ShouldBeNil)
			})

			Convey("when no relevant containers are running", func() {
				other := make(map[string]string)
				other["cloud_script"] = dockerInstallScript + "\necho 1"
				jobs = append(jobs, &Job{Cmd: "sleep 30", Cwd: "/tmp", ReqGroup: "nodocker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "no_docker", MonitorDocker: "?"})

				other = make(map[string]string)
				other["cloud_script"] = dockerInstallScript + "\necho 2"
				dockerName := "jobqueue_test." + internal.RandomString()
				wrongDockerName := internal.RandomString()
				jobs = append(jobs, &Job{Cmd: "docker run --name " + dockerName + " sendu/usememory:v1", Cwd: "/tmp", ReqGroup: "docker", Requirements: &jqs.Requirements{RAM: 3, Time: 5 * time.Second, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "wrongnamed_docker", MonitorDocker: wrongDockerName})

				other = make(map[string]string)
				other["cloud_script"] = dockerInstallScript + "\necho 3"
				dockerCidFile := "jobqueue_test.cidfile"
				wrongDockerCidFile := "jobqueue_wrong.cidfile"
				jobs = append(jobs, &Job{Cmd: "docker run --cidfile " + dockerCidFile + " sendu/usecpu:v1 && rm " + dockerCidFile, Cwd: "/tmp", ReqGroup: "docker2", Requirements: &jqs.Requirements{RAM: 1, Time: 5 * time.Second, Cores: 2, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "wrongcidfile_docker", MonitorDocker: wrongDockerCidFile})

				other = make(map[string]string)
				other["cloud_script"] = dockerInstallScript + "\necho 4"
				dockerCidFile = "uuid-20181127.cidfile"
				wrongDockerUUID := internal.RandomString() + "*" + internal.RandomString()
				jobs = append(jobs, &Job{Cmd: "docker run --cidfile " + dockerCidFile + " sendu/usecpu:v1 && rm " + dockerCidFile, Cwd: "/tmp", ReqGroup: "docker2", Requirements: &jqs.Requirements{RAM: 1, Time: 5 * time.Second, Cores: 2, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "wrongcidglob_docker", MonitorDocker: wrongDockerUUID})

				inserts, already, err := jq.Add(jobs, envVars, true)
				So(err, ShouldBeNil)
				So(inserts, ShouldEqual, 4)
				So(already, ShouldEqual, 0)

				// wait for the jobs to get run
				done := make(chan bool, 1)
				waitRun(done)

				usedMinRAM := 100
				got, err := jq.GetByRepGroup("no_docker", false, 0, JobStateComplete, false, false)
				So(err, ShouldBeNil)
				So(len(got), ShouldEqual, 1)
				So(got[0].PeakRAM, ShouldBeLessThanOrEqualTo, usedMinRAM)
				So(got[0].CPUtime, ShouldBeLessThan, 5*time.Millisecond)

				got, err = jq.GetByRepGroup("wrongnamed_docker", false, 0, JobStateComplete, false, false)
				So(err, ShouldBeNil)
				So(len(got), ShouldEqual, 1)
				So(got[0].PeakRAM, ShouldBeLessThanOrEqualTo, usedMinRAM)
				So(got[0].CPUtime, ShouldBeLessThan, 100*time.Millisecond)

				got, err = jq.GetByRepGroup("wrongcidfile_docker", false, 0, JobStateComplete, false, false)
				So(err, ShouldBeNil)
				So(len(got), ShouldEqual, 1)
				So(got[0].PeakRAM, ShouldBeLessThanOrEqualTo, usedMinRAM)
				So(got[0].CPUtime, ShouldBeLessThan, 100*time.Millisecond)

				got, err = jq.GetByRepGroup("wrongcidglob_docker", false, 0, JobStateComplete, false, false)
				So(err, ShouldBeNil)
				So(len(got), ShouldEqual, 1)
				So(got[0].PeakRAM, ShouldBeLessThanOrEqualTo, usedMinRAM)
				So(got[0].CPUtime, ShouldBeLessThan, 100*time.Millisecond)
			})
		})

		Convey("You can run a cmd with a per-cmd set of config files", func() {
			// create a config file locally
			localConfigPath := filepath.Join(runnertmpdir, "test.config")
			configContent := []byte("myconfig\n")
			err := os.WriteFile(localConfigPath, configContent, 0600)
			So(err, ShouldBeNil)

			// pretend the server is remote to us, and upload our config
			// file first
			remoteConfigPath, err := jq.UploadFile(localConfigPath, "")
			So(err, ShouldBeNil)
			home, herr := os.UserHomeDir()
			So(herr, ShouldBeNil)
			So(remoteConfigPath, ShouldEqual, filepath.Join(home, ".wr_development", "uploads", "4", "2", "5", "a65424cddbee3271f937530c6efc6"))

			// check the remote config file was saved properly
			content, err := os.ReadFile(remoteConfigPath)
			So(err, ShouldBeNil)
			So(content, ShouldResemble, configContent)

			defer func() {
				err = os.RemoveAll(filepath.Join(home, ".wr_development", "uploads"))
				So(err, ShouldBeNil)
			}()

			// create a job that cats a config file that should only exist
			// if the supplied cloud_config_files option worked. It then
			// fails so we can check the stdout afterwards.
			var jobs []*Job
			other := make(map[string]string)
			configPath := "~/.wr_test.config"
			other["cloud_config_files"] = remoteConfigPath + ":" + configPath
			jobs = append(jobs, &Job{Cmd: "cat " + configPath + " && false", Cwd: "/tmp", ReqGroup: "cat", Requirements: &jqs.Requirements{RAM: 1, Time: 1 * time.Hour, Cores: 1, Other: other}, Override: uint8(2), Retries: uint8(0), RepGroup: "with_config_file"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			// wait for the job to get run
			done := make(chan bool, 1)
			go func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						if !server.HasRunners(ctx) {
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
			So(<-done, ShouldBeTrue)

			got, err := jq.GetByRepGroup("with_config_file", false, 0, JobStateBuried, true, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
			stderr, err := got[0].StdErr()
			So(err, ShouldBeNil)
			So(stderr, ShouldEqual, "")
			stdout, err := got[0].StdOut()
			So(err, ShouldBeNil)
			So(stdout, ShouldEqual, strings.TrimSuffix(string(configContent), "\n"))
		})

		Convey("You can run commands with different hardware requirements while dropping the count", func() {
			var jobs []*Job
			dropReq := &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 1, Disk: 0}
			jobs = append(jobs, &Job{Cmd: "sleep 1", Cwd: "/tmp", ReqGroup: "sleep", Requirements: dropReq, Retries: uint8(3), RepGroup: "manually_added"})
			jobs = append(jobs, &Job{Cmd: "echo 2", Cwd: "/tmp", ReqGroup: "echo", Requirements: &jqs.Requirements{RAM: 2048, Time: 1 * time.Hour, Cores: 1}, Override: uint8(2), Retries: uint8(3), RepGroup: "manually_added"})
			jobs = append(jobs, &Job{Cmd: "echo 3", Cwd: "/tmp", ReqGroup: "echo", Requirements: &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 2, Disk: 0}, Retries: uint8(3), RepGroup: "manually_added"})
			jobs = append(jobs, &Job{Cmd: "echo 4", Cwd: "/tmp", ReqGroup: "echo", Requirements: dropReq, Priority: uint8(255), Retries: uint8(3), RepGroup: "manually_added"})
			jobs = append(jobs, &Job{Cmd: "echo 5", Cwd: "/tmp", ReqGroup: "echo", Requirements: &jqs.Requirements{RAM: 1024, Time: 1 * time.Hour, Cores: 1, Disk: 20}, Retries: uint8(3), RepGroup: "manually_added"})
			count := 100
			for i := 6; i <= count; i++ {
				jobs = append(jobs, &Job{Cmd: fmt.Sprintf("echo %d", i), Cwd: "/tmp", ReqGroup: "sleep", Requirements: dropReq, Retries: uint8(3), RepGroup: "manually_added"})
			}
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, count)
			So(already, ShouldEqual, 0)

			// wait for the jobs to get run
			done := make(chan bool, 1)
			go func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						if !server.HasRunners(ctx) {
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
			So(<-done, ShouldBeTrue)
		})

		Convey("The manager reacts correctly to spawned servers going down", func() {
			p, err := cloud.New(ctx, "openstack", resourceName, filepath.Join(runnertmpdir, "os_resources"))
			So(err, ShouldBeNil)

			// for this test to work, we need 1 job to run on another
			// server, so we need to use all the cores of this server per
			// job
			cores := runtime.NumCPU()

			flavor, err := p.CheapestServerFlavor(ctx, cores, 2048, flavorRegex)
			So(err, ShouldBeNil)

			destroyedBadServer := 0
			var dbsMutex sync.Mutex
			badServerCB := func(server *cloud.Server) {
				errf := server.Destroy(ctx)
				if errf == nil {
					dbsMutex.Lock()
					destroyedBadServer++
					dbsMutex.Unlock()
				}
			}

			server.scheduler.SetBadServerCallBack(ctx, badServerCB)

			var jobs []*Job
			req := &jqs.Requirements{RAM: 2048, Time: 1 * time.Hour, Cores: float64(cores), Disk: 0}
			schedGrp := fmt.Sprintf("%d:60:%f:0", flavor.RAM, float64(cores))
			jobs = append(jobs, &Job{Cmd: "sleep 300", Cwd: "/tmp", ReqGroup: "sleep", Requirements: req, Retries: uint8(1), Override: uint8(2), RepGroup: "sleep"})
			jobs = append(jobs, &Job{Cmd: "sleep 301", Cwd: "/tmp", ReqGroup: "sleep", Requirements: req, Retries: uint8(1), Override: uint8(2), RepGroup: "sleep"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			// wait for the jobs to start running
			started := make(chan bool, 1)
			waitForBothRunning := func() {
				limit := time.After(maxSpawnTime)
				ticker := time.NewTicker(1 * time.Second)
				for {
					select {
					case <-ticker.C:
						if server.HasRunners(ctx) {
							running, errf := jq.GetByRepGroup("sleep", false, 0, JobStateRunning, false, false)
							if errf != nil {
								ticker.Stop()
								started <- false
								return
							}
							complete, errf := jq.GetByRepGroup("sleep", false, 0, JobStateComplete, false, false)
							if errf != nil {
								ticker.Stop()
								started <- false
								return
							}
							if len(running)+len(complete) == 2 {
								ticker.Stop()
								started <- true
								return
							}
						}
						continue
					case <-limit:
						ticker.Stop()
						started <- false
						return
					}
				}
			}
			go waitForBothRunning()
			So(<-started, ShouldBeTrue)

			// pretend a server went down by manually terminating one of
			// them, while monitoring that we never request more than 2
			// runners, and that we eventually spawn exactly 1 new server
			// to get the killed job running again
			got, err := jq.GetByRepGroup("sleep", false, 0, JobStateRunning, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 2)

			moreThan2 := make(chan bool, 1)
			stopChecking := make(chan bool, 1)
			go func() {
				ticker := time.NewTicker(10 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						server.psgmutex.RLock()
						group, exists := server.previouslyScheduledGroups[schedGrp]
						if exists && group.count > 2 {
							ticker.Stop()
							moreThan2 <- true
							server.psgmutex.RUnlock()
							return
						}
						server.psgmutex.RUnlock()
					case <-stopChecking:
						ticker.Stop()
						moreThan2 <- false
						return
					}
				}
			}()

			destroyed := false
			var killedJobEssence *JobEssence
			for _, job := range got {
				if job.Host != host {
					So(job.HostID, ShouldNotBeBlank)
					So(job.HostIP, ShouldNotBeBlank)
					err = p.DestroyServer(ctx, job.HostID)
					So(err, ShouldBeNil)
					destroyed = true
					killedJobEssence = &JobEssence{JobKey: job.Key()}
					break
				}
			}
			So(destroyed, ShouldBeTrue)

			// wait for the killed job to be marked as lost and then release
			// it
			gotLost := make(chan bool, 1)
			go func() {
				limit := time.After(20 * time.Second)
				ticker := time.NewTicker(10 * time.Millisecond)
				for {
					select {
					case <-ticker.C:
						job, err := jq.GetByEssence(killedJobEssence, false, false)
						if err != nil {
							ticker.Stop()
							gotLost <- false
							return
						}
						if job.State == JobStateLost {
							ticker.Stop()
							e, err := server.killJob(ctx, killedJobEssence.JobKey)
							if !e || err != nil {
								gotLost <- false
							}
							gotLost <- true

							return
						}
					case <-limit:
						ticker.Stop()
						gotLost <- false

						return
					}
				}
			}()
			So(<-gotLost, ShouldBeTrue)

			// wait until they both start running again
			go waitForBothRunning()
			So(<-started, ShouldBeTrue)
			stopChecking <- true
			So(<-moreThan2, ShouldBeFalse)
			dbsMutex.Lock()
			So(destroyedBadServer, ShouldEqual, 1)
			dbsMutex.Unlock()
		})

		Reset(func() {
			if server != nil {
				<-time.After(1 * time.Second)
				server.Stop(ctx, true)
			}
		})
	})
}

func TestJobqueueWithMounts(t *testing.T) {
	ctx := context.Background()

	if runnermode || servermode {
		return
	}

	if runtime.NumCPU() == 1 {
		// we lock up with only 1 proc
		runtime.GOMAXPROCS(2)
	}

	// for these tests to work, JOBQUEUE_REMOTES3_PATH must be the bucket name
	// and path to an S3 directory set up as per TestS3RemoteIntegration in
	// github.com/VertebrateResequencing/muxfys/s3_test.go. You must also have
	// an ~/.s3cfg file with a default section containing your s3 configuration.

	s3Path := os.Getenv("JOBQUEUE_REMOTES3_PATH")
	home, herr := os.UserHomeDir()
	if herr != nil {
		SkipConvey("home directory not known, so can't run tests")
		return
	}
	_, s3cfgErr := os.Stat(filepath.Join(home, ".s3cfg"))

	if s3Path == "" || s3cfgErr != nil {
		SkipConvey("Without the JOBQUEUE_REMOTES3_PATH environment variable and an ~/.s3cfg file, we'll skip jobqueue S3 tests", t, func() {})
		return
	}

	ServerInterruptTime = 10 * time.Millisecond
	ServerReserveTicker = 10 * time.Millisecond
	ClientReleaseDelay = 100 * time.Millisecond
	clientConnectTime := 10 * time.Second
	ServerItemTTR = 10 * time.Second
	ClientTouchInterval = 50 * time.Millisecond

	config := internal.ConfigLoadFromParentDir(ctx, internal.Development)
	addr := "localhost:" + config.ManagerPort
	serverConfig := ServerConfig{
		Port:            config.ManagerPort,
		WebPort:         config.ManagerWeb,
		SchedulerName:   "local",
		SchedulerConfig: &jqs.ConfigLocal{Shell: config.RunnerExecShell},
		DBFile:          config.ManagerDBFile,
		DBFileBackup:    config.ManagerDBBkFile,
		TokenFile:       config.ManagerTokenFile,
		CAFile:          config.ManagerCAFile,
		CertFile:        config.ManagerCertFile,
		CertDomain:      config.ManagerCertDomain,
		KeyFile:         config.ManagerKeyFile,
		Deployment:      config.Deployment,
	}

	Convey("You can bring up a server configured with an S3 db backup", t, func() {
		s3ServerConfig := serverConfig
		s3ServerConfig.DBFileBackup = fmt.Sprintf("s3://default@%s/db.bk", s3Path)
		localBkPath := filepath.Join(filepath.Dir(config.ManagerDBFile), ".db_bk_mount", s3Path, "db.bk.development")
		os.Remove(config.ManagerDBFile)
		forceBackups = true
		defer func() {
			forceBackups = false
		}()
		server, _, token, errs := serve(ctx, s3ServerConfig)
		So(errs, ShouldBeNil)

		defer func() {
			// stop the server
			server.Stop(ctx, true)

			// and delete the db.bk file in s3, which means we need to mount the
			// thing ourselves
			accessorConfig, err := muxfys.S3ConfigFromEnvironment("default", s3Path)
			if err != nil {
				return
			}
			accessor, err := muxfys.NewS3Accessor(accessorConfig)
			if err != nil {
				return
			}
			remoteConfig := &muxfys.RemoteConfig{
				Accessor: accessor,
				Write:    true,
			}
			cfg := &muxfys.Config{
				Mount:   filepath.Dir(localBkPath),
				Retries: 10,
			}
			fs, err := muxfys.New(cfg)
			if err != nil {
				return
			}
			err = fs.Mount(remoteConfig)
			if err != nil {
				return
			}
			fs.UnmountOnDeath()
			os.Remove(localBkPath)
			err = fs.Unmount()
			if err != nil {
				fmt.Printf("fs.Unmount failed: %s", err)
			}
		}()

		_, err := os.Stat(config.ManagerDBFile)
		So(err, ShouldBeNil)
		_, err = os.Stat(localBkPath)
		So(err, ShouldNotBeNil)

		Convey("You can connect and add a job, which creates a db backup", func() {
			jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			So(err, ShouldBeNil)
			defer disconnect(jq)

			var jobs []*Job
			jobs = append(jobs, &Job{Cmd: "echo 1", Cwd: "/tmp", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 10, Time: 1 * time.Second, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			<-time.After(8 * time.Second)

			info, err := os.Stat(config.ManagerDBFile)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 32768)
			info2, err := os.Stat(localBkPath)
			So(err, ShouldBeNil)
			So(info2.Size(), ShouldEqual, 28672)

			Convey("You can stop the server, delete the database, and it will be restored from S3 backup", func() {
				jobsByRepGroup, err := jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 1)

				server.Stop(ctx, true)
				wipeDevDBOnInit = false
				defer func() {
					wipeDevDBOnInit = true
				}()
				server, _, token, errs = serve(ctx, s3ServerConfig)
				So(errs, ShouldBeNil)
				defer server.Stop(ctx, true)
				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 1)

				server.Stop(ctx, true)
				os.Remove(config.ManagerDBFile)
				_, err = os.Stat(config.ManagerDBFile)
				So(err, ShouldNotBeNil)
				_, err = os.Stat(localBkPath)
				So(err, ShouldNotBeNil)

				server, _, token, errs = serve(ctx, s3ServerConfig)
				So(errs, ShouldBeNil)
				defer server.Stop(ctx, true)

				info, err = os.Stat(config.ManagerDBFile)
				So(err, ShouldBeNil)
				So(info.Size(), ShouldEqual, 28672)
				info2, err = os.Stat(localBkPath)
				So(err, ShouldBeNil)
				So(info2.Size(), ShouldEqual, 28672)

				jq, err = Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
				So(err, ShouldBeNil)

				jobsByRepGroup, err = jq.GetByRepGroup("manually_added", false, 0, "", false, false)
				So(err, ShouldBeNil)
				So(len(jobsByRepGroup), ShouldEqual, 1)
			})
		})
	})

	Convey("You can connect and run commands that rely on files in a remote S3 object store", t, func() {
		// pwd, err := os.Getwd()
		// if err != nil {
		// 	log.Fatal(err)
		// }
		cwd, err := os.MkdirTemp("", "wr_jobqueue_test_s3_dir_")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(cwd)

		server, _, token, err := serve(ctx, serverConfig)
		So(err, ShouldBeNil)

		standardReqs := &jqs.Requirements{RAM: 10, Time: 10 * time.Second, Cores: 1, Disk: 0, Other: make(map[string]string)}

		jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
		So(err, ShouldBeNil)
		defer disconnect(jq)

		var jobs []*Job
		mcs := MountConfigs{
			{Targets: []MountTarget{
				{Path: s3Path, Cache: true},
				{Path: s3Path + "/sub/deep", Cache: true},
			}, Verbose: true},
		}
		b := &Behaviour{When: OnExit, Do: CleanupAll}
		bs := Behaviours{b}

		Convey("Commands can read remote data and the cache gets deleted afterwards", func() {
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt && cat bar", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "s3", MountConfigs: mcs, Behaviours: bs})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.RepGroup, ShouldEqual, "s3")

			// muxfys.SetLogHandler(log15.StderrHandler)
			jeerr := jq.Execute(ctx, job, config.RunnerExecShell)
			So(jeerr, ShouldBeNil)

			job, err = jq.GetByEssence(&JobEssence{Cmd: "cat numalphanum.txt && cat bar"}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldBeNil)

			job, err = jq.GetByEssence(&JobEssence{Cmd: "cat numalphanum.txt && cat bar", MountConfigs: mcs}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.State, ShouldEqual, JobStateComplete)

			// test that the cache dirs get deleted; this test fails if cwd is based
			// in an nfs mount, the problem confirmed upstream in muxfys but with no
			// solution apparent...
			So(job.ActualCwd, ShouldNotBeEmpty)
			_, err = os.Stat(job.ActualCwd)
			So(err, ShouldNotBeNil)
			So(os.IsNotExist(err), ShouldBeTrue)
			f, err := os.Open(job.Cwd)
			So(err, ShouldBeNil)
			defer f.Close()
			_, err = f.Readdirnames(100)
			So(err, ShouldEqual, io.EOF) // ie. the whole created working dir got wiped
		})

		t1 := MountTarget{Path: s3Path + "/sub", Cache: true}
		t2 := MountTarget{Path: s3Path, Cache: true}
		Convey("You can add identical commands with different mounts", func() {
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "a", MountConfigs: mcs, Behaviours: bs})

			mcs2 := MountConfigs{
				{Targets: []MountTarget{t1}, Verbose: true},
			}

			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "b", MountConfigs: mcs2, Behaviours: bs})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			mcs3 := MountConfigs{
				{Targets: []MountTarget{t1, t2}},
			}
			mcs4 := MountConfigs{
				{Targets: []MountTarget{t2, t1}},
			}

			jobs = nil
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "c", MountConfigs: mcs3})
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "d", MountConfigs: mcs4})
			inserts, already, err = jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 2)
			So(already, ShouldEqual, 0)

			job, err := jq.GetByEssence(&JobEssence{Cmd: "cat numalphanum.txt", MountConfigs: mcs3}, false, false)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.RepGroup, ShouldEqual, "c")
		})

		Convey("You can't add identical commands with the same mounts", func() {
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "a", MountConfigs: mcs, Behaviours: bs})
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "b", MountConfigs: mcs})
			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 1)

			mcs5 := MountConfigs{
				{Targets: []MountTarget{t1}, Verbose: true, Mount: "a"},
				{Targets: []MountTarget{t2}, Verbose: true, Mount: "b"},
			}
			mcs6 := MountConfigs{
				{Targets: []MountTarget{t2}, Verbose: true, Mount: "b"},
				{Targets: []MountTarget{t1}, Verbose: true, Mount: "a"},
			}

			jobs = nil
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "c", MountConfigs: mcs5})
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "d", MountConfigs: mcs6})
			inserts, already, err = jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 1)
		})

		Convey("You can modify the mounts", func() {
			jobs = append(jobs, &Job{Cmd: "cat numalphanum.txt", Cwd: cwd, ReqGroup: "cat", Requirements: standardReqs, RepGroup: "s3"})

			inserts, already, err := jq.Add(jobs, envVars, true)
			So(err, ShouldBeNil)
			So(inserts, ShouldEqual, 1)
			So(already, ShouldEqual, 0)

			job, err := jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.RepGroup, ShouldEqual, "s3")

			jeerr := jq.Execute(ctx, job, config.RunnerExecShell)
			So(jeerr, ShouldNotBeNil)

			got, err := jq.GetByRepGroup("s3", false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)

			jm := NewJobModifer()
			jm.SetMountConfigs(MountConfigs{
				{Targets: []MountTarget{
					{Path: s3Path},
				}, Verbose: true},
			})
			got, err = jq.GetByRepGroup("s3", false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes := jobsToJobEssenses(got)
			modified, err := jq.Modify(jes, jm)
			So(err, ShouldBeNil)
			So(len(modified), ShouldEqual, 1)

			got, err = jq.GetByRepGroup("s3", false, 0, JobStateBuried, false, false)
			So(err, ShouldBeNil)
			jes = jobsToJobEssenses(got)
			kicked, err := jq.Kick(jes)
			So(err, ShouldBeNil)
			So(kicked, ShouldEqual, 1)

			job, err = jq.Reserve(50 * time.Millisecond)
			So(err, ShouldBeNil)
			So(job, ShouldNotBeNil)
			So(job.RepGroup, ShouldEqual, "s3")

			jeerr = jq.Execute(ctx, job, config.RunnerExecShell)
			So(jeerr, ShouldBeNil)

			got, err = jq.GetByRepGroup("s3", false, 0, JobStateComplete, false, false)
			So(err, ShouldBeNil)
			So(len(got), ShouldEqual, 1)
		})

		Reset(func() {
			if server != nil {
				server.Stop(ctx, true)
			}
		})
	})
}

func TestJobqueueSpeed(t *testing.T) {
	if runnermode || servermode {
		return
	}

	if true { // testing.Short()
		t.Skip("skipping speed test")
	}

	ctx := context.Background()
	config := internal.ConfigLoadFromParentDir(ctx, "development")
	serverConfig := ServerConfig{
		Port:            config.ManagerPort,
		WebPort:         config.ManagerWeb,
		SchedulerName:   "local",
		SchedulerConfig: &jqs.ConfigLocal{Shell: config.RunnerExecShell},
		DBFile:          config.ManagerDBFile,
		DBFileBackup:    config.ManagerDBBkFile,
		TokenFile:       config.ManagerTokenFile,
		CAFile:          config.ManagerCAFile,
		CertFile:        config.ManagerCertFile,
		CertDomain:      config.ManagerCertDomain,
		KeyFile:         config.ManagerKeyFile,
		Deployment:      config.Deployment,
	}
	addr := "localhost:" + config.ManagerPort

	// some manual speed tests (don't like the way the benchmarking feature
	// works)
	runtime.GOMAXPROCS(runtime.NumCPU())
	n := 50000

	server, _, token, err := serve(ctx, serverConfig)
	if err != nil {
		log.Fatal(err)
	}

	clientConnectTime := 10 * time.Second
	jq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
	if err != nil {
		log.Fatal(err)
	}
	defer disconnect(jq)

	before := time.Now()
	jobs := make([]*Job, 0, n)
	for i := 0; i < n; i++ {
		jobs = append(jobs, &Job{Cmd: fmt.Sprintf("test cmd %d", i), Cwd: "/fake/cwd", ReqGroup: "fake_group", Requirements: &jqs.Requirements{RAM: 1024, Time: 4 * time.Hour, Cores: 1}, Retries: uint8(3), RepGroup: "manually_added"})
	}
	inserts, already, err := jq.Add(jobs, envVars, true)
	if err != nil {
		log.Fatal(err)
	}
	e := time.Since(before)
	per := e.Nanoseconds() / int64(n)
	log.Printf("Added %d jobqueue jobs (%d inserts, %d dups) in %s == %d per\n", n, inserts, already, e, per)

	err = jq.Disconnect()
	if err != nil {
		log.Fatal(err)
	}

	reserves := make(chan int, n)
	beginat := time.Now().Add(1 * time.Second)
	o := runtime.NumCPU() // from here up to 1650 the time taken is around 6-7s, but beyond 1675 it suddenly drops to 14s, and seems to just hang forever at much higher values
	m := int(math.Ceil(float64(n) / float64(o)))
	for i := 1; i <= o; i++ {
		go func(i int) {
			start := time.After(time.Until(beginat))
			gjq, err := Connect(addr, config.ManagerCAFile, config.ManagerCertDomain, token, clientConnectTime)
			if err != nil {
				log.Fatal(err)
			}
			defer disconnect(gjq)
			<-start
			reserved := 0
			na := 0
			for j := 0; j < m; j++ {
				job, err := gjq.Reserve(5 * time.Second)
				if err != nil || job == nil {
					for k := j; k < m; k++ {
						na++
						reserves <- -i
					}
					break
				} else {
					reserved++
					reserves <- i
				}
			}
		}(i)
	}

	r := 0
	na := 0
	for i := 0; i < n; i++ {
		res := <-reserves
		if res >= 0 {
			r++
		} else {
			na++
		}
	}

	e = time.Since(beginat)
	per = e.Nanoseconds() / int64(n)
	log.Printf("Reserved %d jobqueue jobs (%d not available) in %s == %d per\n", r, na, e, per)

	// q := queue.New("myqueue")
	// before = time.Now()
	// for i := 0; i < n; i++ {
	// 	_, err := q.Add(fmt.Sprintf("test job %d", i), "data", 0, 0*time.Second, 30*time.Second)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// }
	// e = time.Since(before)
	// per = int64(e.Nanoseconds() / int64(n))
	// log.Printf("Added %d items to queue in %s == %d per\n", n, e, per)

	server.Stop(ctx, true)

	/* test speed of bolt db when there are lots of jobs already stored
	n := 10000000 // num jobs to start with
	b := 10000    // jobs per identifier

	server, _, err := serve(serverConfig)
	if err != nil {
		log.Fatal(err)
	}

	jq, err := Connect(addr, "cmds", 60*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	defer disconnect(jq)

	// get timings when the bolt db is empty
	total := 0
	batchNum := 1
	timeDealingWithBatch(addr, jq, batchNum, b)
	batchNum++
	total += b

	// add n jobs in b batches to the completed bolt db bucket to simulate
	// a well used database
	before := time.Now()
	q := server.getOrCreateQueue("cmds")
	for total < n {
		var jobs []*Job
		for i := 0; i < b; i++ {
			jobs = append(jobs, &Job{Cmd: fmt.Sprintf("test cmd %d", i+((batchNum-1)*b)), Cwd: "/fake/cwd", ReqGroup: "reqgroup", Requirements: &jqs.Requirements{RAM: 1024, Time: 4*time.Hour, Cores: 1}, Retries: uint8(3), RepGroup: fmt.Sprintf("batch_%d", batchNum)})
		}
		_, _, err := jq.Add(jobs, envVars, true)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nadded batch %d", batchNum)

		// it's too slow to reserve and archive things properly in this
		// test; this is not a real-world performance concern though, since
		// normally you wouldn't archive so many jobs in a row in a single
		// process...
		// for {
		// 	job, _ := jq.Reserve(1 * time.Millisecond)
		// 	if job == nil {
		// 		break
		// 	}
		// 	jq.Started(job, 123)
		// 	jq.Ended(job, 0, 5, 1*time.Second, []byte{}, []byte{})
		// 	err = jq.Archive(job)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 	}
		// 	fmt.Print(".")
		// }

		// ... Instead we bypass the client interface and directly add to
		// bolt db
		err = server.db.bolt.Batch(func(tx *bolt.Tx) error {
			bl := tx.Bucket(bucketJobsLive)
			b := tx.Bucket(bucketJobsComplete)

			var puterr error
			for _, job := range jobs {
				key := job.key()
				var encoded []byte
				enc := codec.NewEncoderBytes(&encoded, server.db.ch)
				enc.Encode(job)

				bl.Delete([]byte(key))
				q.Remove(key)

				puterr = b.Put([]byte(key), encoded)
				if puterr != nil {
					break
				}
			}
			return puterr
		})
		if err != nil {
			log.Fatal(err)
		}

		batchNum++
		total += b
	}
	e := time.Since(before)
	log.Printf("Archived %d jobqueue jobs in %d sized groups in %s\n", n, b, e)

	// now re-time how long it takes to deal with a single new batch
	timeDealingWithBatch(addr, jq, batchNum, b)

	server.Stop()
	*/
}

/* this func is used by the commented out test above
func timeDealingWithBatch(addr string, jq *Client, batchNum int, b int) {
	before := time.Now()
	var jobs []*Job
	batchName := fmt.Sprintf("batch_%d", batchNum)
	for i := 0; i < b; i++ {
        jobs = append(jobs, &Job{Cmd: fmt.Sprintf("test cmd %d", i+((batchNum-1)*b)), Cwd: "/fake/cwd", ReqGroup: "reqgroup", Requirements: &jqs.Requirements{RAM: 1024, Time: 4*time.Hour, Cores: 1}, Retries: uint8(3), RepGroup: batchName})
	}
	_, _, err := jq.Add(jobs, envVars, true)
	if err != nil {
		log.Fatal(err)
	}
	e := time.Since(before)
	log.Printf("\nAdded a new batch of %d jobs in %s\n", b, e)

	before = time.Now()
	runtime.GOMAXPROCS(runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(runtime.NumCPU())
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			defer wg.Done()
			gojq, _ := Connect(addr, "cmds", 10*time.Second)
            defer gojq.Disconnect()
			for {
				job, _ := gojq.Reserve(1 * time.Millisecond)
				if job == nil {
					break
				}
				gojq.Started(job, 123)
				gojq.Ended(job, 0, 5, 1*time.Second, []byte{}, []byte{})
				err = gojq.Archive(job)
				if err != nil {
					log.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()
	e = time.Since(before)
	log.Printf("Reserved and Archived that batch of jobs in %s\n", e)

	before = time.Now()
	jobs, err = jq.GetByRepGroup(batchName, 1, JobStateComplete, false, false) // without a limit this takes longer than 60s, so would time out
	if err != nil {
		log.Fatal(err)
	}
	e = time.Since(before)
	log.Printf("Was able to get all %d jobs in that batch in %s\n", 1+jobs[0].Similar, e)
}
*/

func timelimitDebug(jobs []*Job, err error) {
	if err != nil {
		fmt.Printf("\ntime limit reached, but err getting jobs: %s\n", err)
	} else {
		fmt.Printf("\ntime limit reached, jobs:\n")
		for _, job := range jobs {
			stderr, errs := job.StdErr()
			if errs != nil {
				fmt.Printf(" problem getting stderr: %s\n", errs)
			}
			fmt.Printf(" [%s]: %s (%s)\n", job.Cmd, job.State, stderr)
		}
	}
}

func disconnect(client *Client) {
	err := client.Disconnect()
	if err != nil && !strings.Contains(err.Error(), "connection closed") {
		fmt.Printf("client.Disconnect() failed: %s", err)
	}
}

func execute(ctx context.Context, client *Client, job *Job, shell string, failExpected ...bool) {
	err := client.Execute(ctx, job, shell)
	if err != nil && !(len(failExpected) == 1 && failExpected[0]) {
		fmt.Printf("client.Execute() failed: %s", err)
	}
}

func touch(client *Client, job *Job) {
	_, err := client.Touch(job)
	if err != nil {
		fmt.Printf("client.Touch() failed: %s", err)
	}
}

func runner(ctx context.Context) {
	if runnerfail && runnermodetmpdir != "" {
		// simulate loss of network connectivity between a spawned runner and
		// the manager by just exiting without reserving any job
		<-time.After(250 * time.Millisecond)
		tmpfile, err := os.CreateTemp(runnermodetmpdir, "fail")
		if err == nil {
			tmpfile.Close()
		}
		return
	}

	ServerItemTTR = 10 * time.Second
	ClientTouchInterval = 50 * time.Millisecond

	if runnerdebug {
		logfile, errlog := os.CreateTemp("", "wrrunnerlog")
		if errlog == nil {
			defer logfile.Close()
			log.SetOutput(logfile)
		}
	}

	if schedgrp == "" {
		log.Fatal("schedgrp missing")
	}
	log.Printf("runner working on schedgrp %s\n", schedgrp)

	config := internal.ConfigLoadFromParentDir(ctx, rdeployment)

	token, err := os.ReadFile(config.ManagerTokenFile)
	if err != nil {
		log.Fatalf("token read err: %s\n", err)
	}

	timeout := 6 * time.Second
	rtimeoutd := time.Duration(rtimeout) * time.Second
	// (we don't bother doing anything with maxmins in this test, but in a real
	//  runner client it would be used to end the below for loop before hitting
	//  this limit)

	jq, err := Connect(rserver, config.ManagerCAFile, rdomain, token, timeout)

	if err != nil {
		log.Fatalf("connect err: %s\n", err)
	}
	defer disconnect(jq)

	clean := true
	n := 0
	i := 0
	for {
		i++
		job, err := jq.ReserveScheduled(rtimeoutd, schedgrp)
		if err != nil {
			log.Fatalf("reserve err: %s\n", err)
		}
		if job == nil {
			log.Printf("reserve gave no job after %s\n", rtimeoutd)
			break
		}

		log.Printf("working on job %s\n", job.Cmd)
		n++

		// actually run the cmd
		err = jq.Execute(ctx, job, config.RunnerExecShell)
		if err != nil {
			if jqerr, ok := err.(Error); ok && jqerr.Err == FailReasonSignal {
				break
			} else {
				log.Printf("execute err: %s\n", err) // make this a Fatalf if you want to see these in the test output
				clean = false
				break
			}
		} else {
			err := jq.Archive(job, nil)
			if err != nil {
				log.Printf("archive err: %s\n", err)
			}
		}
	}

	log.Printf("ran %d jobs in %d loops\n", n, i)

	// if everything ran cleanly, create a tmpfile in our tmp dir
	if clean && runnermodetmpdir != "" {
		log.Printf("creating ok file in %s\n", runnermodetmpdir)
		tmpfile, err := os.CreateTemp(runnermodetmpdir, "ok")
		if err == nil {
			tmpfile.Close()
		}
	}
}

// setDomainIP is an author-only func to ensure that domain points to localhost
func setDomainIP(domain string) {
	if domain == "localhost" {
		return
	}
	host, err := os.Hostname()
	if err != nil {
		fmt.Printf("failed to get Hostname: %s", err)
		return
	}
	if host == "vr-2-2-02" {
		ip, err := internal.CurrentIP("")
		if err != nil {
			fmt.Printf("failed to get CurrentIP: %s", err)
			return
		}
		err = internal.InfobloxSetDomainIP(domain, ip)
		if err != nil {
			fmt.Printf("InfobloxSetDomainIP failed: %s", err)
		}
	}
}
