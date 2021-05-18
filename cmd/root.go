// Copyright © 2016-2018, 2021 Genome Research Limited
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

package cmd

// this is the cobra file that enables subcommands and handles command-line args

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/sevlyar/go-daemon"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wr/clog"
)

// maxCloudResourceUsernameLength is the maximum length that cloud username can
// be. It is limited because it will form part of cloudResourceName(), which
// will in turn form hostnames, which have max length 63. cloudResourceName()
// has a fixed prefix of length up to 8, and host names will include a UUID of
// length 36 and a prefix length 1, leaving 18 characters for the username.
const maxCloudResourceUsernameLength = 18

// these variables are accessible by all subcommands.
var deployment string
var config *internal.Config

// these are shared by some of the subcommands.
var addr string
var caFile string
var timeoutint int
var cmdCwd string

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "wr",
	Short: "wr is a software workflow management system.",
	Long: `wr is a software workflow management system and command runner.

You use it to run the same sequence of commands (a "workflow") on many different
input files (which comprise a "datasource").

Initially, you start the management system, which maintains a queue of the
commands you want to run:
$ wr manager start

Then you add commands you want to run to the queue:
$ wr add

At this point your commands should be running, and you can monitor their
progress with:
$ wr status`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die(err.Error())
	}
}

// ExecuteLSF is for treating a call to wr as if `wr lsf xxx` was called, for
// the LSF emulation to work.
func ExecuteLSF(cmd string) {
	args := append([]string{"lsf", cmd}, os.Args[1:]...)
	command, _, err := RootCmd.Find(args)
	if err != nil {
		die(err.Error())
	}
	RootCmd.SetArgs(args)
	if err := command.Execute(); err != nil {
		die(err.Error())
	}
}

func init() {
	ctx := context.Background()
	// set up logging to stderr
	clog.ToDefaultAtLevel("info")

	// global flags
	RootCmd.PersistentFlags().StringVar(&deployment, "deployment", internal.DefaultDeployment(ctx),
		"use production or development config")

	cobra.OnInitialize(initConfig)
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	ctx := context.Background()
	config = internal.ConfigLoadFromCurrentDir(ctx, deployment)
	addr = config.ManagerHost + ":" + config.ManagerPort
	caFile = config.ManagerCAFile
}

// token reads and returns the token from the file created when the manager
// starts.
func token() ([]byte, error) {
	token, err := os.ReadFile(config.ManagerTokenFile)
	if err != nil {
		return nil, err
	}
	return token, nil
}

// realUsername returns the username of the current user.
func realUsername() string {
	username, err := internal.Username()
	if err != nil {
		die("could not get username: %s", err)
	}
	return username
}

// cloudResourceName returns a user and deployment specific string that can be
// used to name cloud resources so they can be identified as having been created
// by wr. username arg defaults to the real username of the user running wr.
func cloudResourceName(username string) string {
	if username == "" {
		username = realUsername()
	}
	var dep string
	if config.Deployment == internal.Production {
		dep = "prod"
	} else {
		dep = "dev"
	}
	return "wr-" + dep + "-" + username
}

// info is a convenience to log a message at the Info level.
func info(msg string, a ...interface{}) {
	ctx := context.Background()
	clog.Info(ctx, fmt.Sprintf(msg), a...)
}

// warn is a convenience to log a message at the Warn level.
func warn(msg string, a ...interface{}) {
	ctx := context.Background()
	clog.Warn(ctx, fmt.Sprintf(msg), a...)
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(msg string, a ...interface{}) {
	ctx := context.Background()
	clog.Fatal(ctx, fmt.Sprintf(msg), a...)
}

// createWorkingDir ensures the main working directory is available
func createWorkingDir() {
	_, err := os.Stat(config.ManagerDir)
	if err != nil {
		if os.IsNotExist(err) {
			// try and create the directory
			err = os.MkdirAll(config.ManagerDir, os.ModePerm)
			if err != nil {
				die("could not create the working directory '%s': %v", config.ManagerDir, err)
			}
		} else {
			die("could not access or create the working directory '%s': %v", config.ManagerDir, err)
		}
	}
}

// daemonize spawns a child copy of ourselves with the correct deployment (we
// need to be careful because the default deployment depends on current dir, and
// the child is forced to run from /). Supplying extraArgs can override earlier
// args (to eg. re-specify an option with a relative path with an absolute
// path).
func daemonize(pidFile string, umask int, extraArgs ...string) (*os.Process, *daemon.Context) {
	args := os.Args
	hadDeployment := false
	for _, arg := range args {
		if arg == "--deployment" {
			hadDeployment = true
			break
		}
	}
	if !hadDeployment {
		args = append(args, "--deployment")
		args = append(args, config.Deployment)
	}

	args = append(args, extraArgs...)

	context := &daemon.Context{
		PidFileName: pidFile,
		PidFilePerm: 0644,
		WorkDir:     "/",
		Args:        args,
		Umask:       umask,
	}

	child, err := context.Reborn()
	if err != nil {
		// try again, deleting the pidFile first
		errr := os.Remove(pidFile)
		if errr != nil && !os.IsNotExist(errr) {
			warn("failed to delete existing pid file: %s", errr)
		}

		child, err = context.Reborn()
		if err != nil {
			die("failed to daemonize: %s", err)
		}
	}
	return child, context
}

// stopdaemon stops the daemon created by daemonize() by sending it SIGTERM and
// checking it really exited
func stopdaemon(pid int, source string) bool {
	err := syscall.Kill(pid, syscall.SIGTERM)
	if err != nil {
		warn("wr manager is running with pid %d according to %s, but failed to send it SIGTERM: %s", pid, source, err)
		return false
	}

	// wait a while for the daemon to gracefully close down
	giveupseconds := 120
	giveup := time.After(time.Duration(giveupseconds) * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	stopped := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-ticker.C:
				err = syscall.Kill(pid, syscall.Signal(0))
				if err == nil {
					// pid is still running
					continue
				}
				// assume the error was "no such process" *** should I do a string comparison to confirm?
				ticker.Stop()
				stopped <- true
				return
			case <-giveup:
				ticker.Stop()
				stopped <- false
				return
			}
		}
	}()
	ok := <-stopped

	// if it didn't stop, offer to force kill it? That's a bit dangerous...
	// just warn for now
	if !ok {
		warn("wr manager, running with pid %d according to %s, is still running %ds after I sent it a SIGTERM", pid, source, giveupseconds)
	}

	return ok
}

// sAddr gets a nice manager address to report in logs, preferring hostname,
// falling back on the ip address if that wasn't set
func sAddr(s *jobqueue.ServerInfo) string {
	saddr := s.Host
	if saddr == "localhost" {
		saddr = s.Addr
	} else {
		saddr += ":" + s.Port
	}
	return saddr
}

// connect gives you a connected client. Dies on error. Dies if there is no
// token file. Does not die or report any kind of error if an optional bool is
// supplied true.
func connect(wait time.Duration, expectedToBeDown ...bool) *jobqueue.Client {
	token, err := token()
	if err != nil && !(len(expectedToBeDown) == 1 && expectedToBeDown[0]) {
		die("could not read token file; has the manager been started? [%s]", err)
	}

	jq, err := jobqueue.Connect(config.ManagerHost+":"+config.ManagerPort, caFile, config.ManagerCertDomain, token, wait)
	if err != nil && !(len(expectedToBeDown) == 1 && expectedToBeDown[0]) {
		die("%s", err)
	}
	return jq
}

// setupLogging is a function to provide a new logger who's logging depends on
// debug.
func setupLogging(debug bool) {
	// Set up logging for both commands
	// for debug purposes, set up logging to STDERR
	clog.ToDefault()
	if debug {
		clog.ToDefaultAtLevel("debug")
	}
}
