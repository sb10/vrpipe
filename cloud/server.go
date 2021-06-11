// Copyright © 2016-2021 Genome Research Limited
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

package cloud

// This file contains the code for the Server struct.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	sync "github.com/sasha-s/go-deadlock"
	"github.com/wtsi-ssg/wr/clog"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

const sharePath = "/shared" // mount point for the *SharedDisk methods
const sshShortTimeOut = 15 * time.Second

// maxSSHSessions is the maximum number of sessions we will try and multiplex on
// each ssh client we make for a server. It doesn't matter if this is lower than
// the server is configured for in /etc/ssh/sshd_config (MaxSessions); we create
// more clients than stricly needed, but this is harmless? If it's higher than
// the configured MaxSessions, there doesn't seem to be much we can do about it.
// MaxSessions can't be queried ahead of time, and we can't discover the correct
// value because if we go over it, then all sessions fail, not just the ones
// over the max (and all failures appear at the ~same time if sessions were
// made at the ~same time). The best we can do for now is to set this to sshd's
// default MaxSessions, which is 10. *** can we do better?
const maxSSHSessions = 10

// Flavor describes a "flavor" of server, which is a certain (virtual) hardware
// configuration
type Flavor struct {
	ID    string
	Name  string
	Cores int
	RAM   int // MB
	Disk  int // GB
}

// HasSpaceFor takes the cpu, ram and disk requirements of a command and tells
// you how many of those commands could run simultaneously on a server of our
// flavor. Returns 0 if not even 1 command could fit on a server with this
// flavor.
func (f *Flavor) HasSpaceFor(cores float64, ramMB, diskGB int) int {
	if internal.FloatLessThan(float64(f.Cores), cores) || (f.RAM < ramMB) || (f.Disk < diskGB) {
		return 0
	}

	var canDo int
	if cores == 0 {
		// rather than allow an infinite or very large number of cmds to run on
		// a server, because there are still real limits on the number of
		// processes we can run at once before things start falling over, we
		// only allow double the actual core count of zero core things to run
		canDo = f.Cores * internal.ZeroCoreMultiplier
	} else {
		canDo = int(math.Floor(float64(f.Cores) / cores))
	}
	if canDo > 1 {
		var n int
		if ramMB > 0 {
			n = f.RAM / ramMB
			if n < canDo {
				canDo = n
			}
		}
		if diskGB > 0 {
			n = f.Disk / diskGB
			if n < canDo {
				canDo = n
			}
		}
	}
	return canDo
}

// Server provides details of the server that Spawn() created for you, and some
// methods that let you keep track of how you use that server.
type Server struct {
	Script            []byte // the content of a start-up script run on the server
	sshClients        []*ssh.Client
	sshClientSessions []int
	AdminPass         string
	PrivateKey        string // PEM format string of a private key that can be used to SSH to the server
	ID                string
	IP                string // ip address that you could SSH to
	Name              string // ought to correspond to the hostname
	OS                string // the name of the Operating System image
	ConfigFiles       string // files that you will CopyOver() and require to be on this Server, in CopyOver() format
	UserName          string // the username needed to log in to the server
	permanentProblem  string
	homeDir           string
	Flavor            *Flavor
	Disk              int           // GB of available disk space
	TTD               time.Duration // amount of idle time allowed before destruction
	goneBad           time.Time
	cancelDestruction chan bool
	cancelID          int
	cancelRunCmd      map[int]chan bool
	provider          *Provider
	sshClientConfig   *ssh.ClientConfig
	usedCores         float64
	cancels           int
	usedZeroCores     int // we keep track of how many zero core things are allocated
	usedDisk          int
	usedRAM           int
	mutex             sync.RWMutex
	hmutex            sync.Mutex
	csmutex           sync.Mutex
	IsHeadNode        bool
	SharedDisk        bool // the server will mount /shared
	created           bool // to distinguish instances we discovered or spawned
	toBeDestroyed     bool
	destroyed         bool
	onDeathrow        bool
	sshStarted        bool
	createdShare      bool
	used              bool
}

// NewServer returns a Server with the minimal details set needed to SSH to it
// and use the various SSH-requiring methods. You will need to manually set
// other properties for other functionality to work.
func NewServer(username, ip, key string) *Server {
	return &Server{
		UserName:     username,
		IP:           ip,
		PrivateKey:   key,
		cancelRunCmd: make(map[int]chan bool),
	}
}

// WaitUntilReady waits for the server to become fully ready: the boot process
// will have completed and ssh will work. This is not part of provider.Spawn()
// because you may not want or be able to ssh to your server, and so that you
// can Spawn() another server while waiting for this one to become ready. If you
// get an err, you will want to call server.Destroy() as this is not done for
// you.
//
// You supply a context so that you can cancel waiting if you no longer need
// this server. Be sure to Destroy() it after cancelling.
//
// files is a string in the format taken by the CopyOver() method; if supplied
// non-blank it will CopyOver the specified files (after the server is ready,
// before any postCreationScript is run).
//
// postCreationScript is the []byte content of a script that will be run on the
// server (as the user supplied to Spawn()) once it is ready, and it will
// complete before this function returns; empty slice means do nothing.
func (s *Server) WaitUntilReady(ctx context.Context, files string, postCreationScript []byte) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	// wait for ssh to come up
	_, _, err := s.SSHClient(ctx)
	if err != nil {
		return err
	}

	// wait for sentinelFilePath to exist, indicating that the server is
	// really ready to use
	limit := time.After(sentinelTimeOut)
	ticker := time.NewTicker(1 * time.Second)
SENTINEL:
	for {
		select {
		case <-ticker.C:
			o, e, fileErr := s.RunCmd(ctx, "file "+sentinelFilePath, false)
			if fileErr == nil && !strings.Contains(o, "No such file") && !strings.Contains(e, "No such file") {
				ticker.Stop()
				// *** o contains "empty"; test for that instead? Does file
				// behave the same way on all linux variants?
				_, _, rmErr := s.RunCmd(ctx, "sudo rm "+sentinelFilePath, false)
				if rmErr != nil {
					clog.Warn(ctx, "failed to remove sentinel file", "path", sentinelFilePath, "err", rmErr)
				}
				break SENTINEL
			}
			continue SENTINEL
		case <-limit:
			ticker.Stop()
			return errors.New("cloud server never became ready to use")
		case <-ctx.Done():
			ticker.Stop()
			return errors.New("cloud server waiting for ready was cancelled")
		}
	}

	// copy over any desired files
	if files != "" {
		err = s.CopyOver(ctx, files)
		if err != nil {
			return fmt.Errorf("cloud server files failed to upload: %s", err)
		}
		s.ConfigFiles = files
	}

	// run the postCreationScript
	if len(postCreationScript) > 0 {
		pcsPath := "/tmp/.postCreationScript"
		err = s.CreateFile(ctx, string(postCreationScript), pcsPath)
		if err != nil {
			return fmt.Errorf("cloud server start up script failed to upload: %s", err)
		}

		_, _, err = s.RunCmd(ctx, "chmod u+x "+pcsPath, false)
		if err != nil {
			return fmt.Errorf("cloud server start up script could not be made executable: %s", err)
		}

		// protect running the script with a timeout
		limit := time.After(pcsTimeOut)
		exiterr := make(chan error, 1)
		var stderr string
		go func() {
			var runerr error
			_, stderr, runerr = s.RunCmd(ctx, pcsPath, false)
			exiterr <- runerr
		}()
		select {
		case err = <-exiterr:
			if err != nil {
				err = fmt.Errorf("cloud server start up script failed: %s", err.Error())
				if len(stderr) > 0 {
					err = fmt.Errorf("%s\nSTDERR:\n%s", err.Error(), stderr)
				}
				return err
			}
		case <-limit:
			return fmt.Errorf("cloud server start up script failed to complete within %s", pcsTimeOut)
		}

		_, _, rmErr := s.RunCmd(ctx, "rm "+pcsPath, false)
		if rmErr != nil {
			clog.Warn(ctx, "failed to remove post creation script", "path", pcsPath, "err", rmErr)
		}

		s.Script = postCreationScript

		// because the postCreationScript may have altered PATH and other things
		// that subsequent RunCmd may rely on, clear the clients
		for _, client := range s.sshClients {
			err = client.Close()
			if err != nil {
				clog.Warn(ctx, "failed to close client ssh connection", "err", err)
			}
		}
		s.sshClients = []*ssh.Client{}
		s.sshClientSessions = []int{}
	}

	return nil
}

// Matches tells you if in principle a Server has the given os, script, config
// files, flavor and has a shared disk mounted. Useful before calling
// HasSpaceFor, since if you don't match these things you can't use the Server
// regardless of how empty it is. configFiles is in the CopyOver() format.
func (s *Server) Matches(os string, script []byte, configFiles string, flavor *Flavor, sharedDisk bool) bool {
	return s.OS == os && bytes.Equal(s.Script, script) && s.ConfigFiles == configFiles && (flavor == nil || flavor.ID == s.Flavor.ID) && s.SharedDisk == sharedDisk
}

// Allocate considers the current usage (according to prior calls)
// and records the given resources have now been used up on this server, if
// there was enough space. Returns true if there was enough space and the
// allocation occurred.
func (s *Server) Allocate(ctx context.Context, cores float64, ramMB, diskGB int) bool {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.checkSpace(cores, ramMB, diskGB) == 0 {
		return false
	}

	s.used = true

	if cores == 0 {
		s.usedZeroCores++
	} else {
		s.usedCores = internal.FloatAdd(s.usedCores, cores)
	}
	s.usedRAM += ramMB
	s.usedDisk += diskGB

	clog.Debug(ctx, "server allocate", "cores", cores, "RAM", ramMB, "disk", diskGB, "usedCores",
		s.usedCores, "usedZeroCores", s.usedZeroCores, "usedRAM", s.usedRAM, "usedDisk", s.usedDisk)

	// if the host has initiated its countdown to destruction, cancel that
	if s.onDeathrow {
		s.cancels++
		go func() {
			s.cancelDestruction <- true
		}()
	}

	return true
}

// Used tells you if this server has ever had Allocate() called on it.
func (s *Server) Used() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.used
}

// Release records that the given resources have now been freed.
func (s *Server) Release(ctx context.Context, cores float64, ramMB, diskGB int) {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if cores == 0 {
		s.usedZeroCores--
	} else {
		s.usedCores = internal.FloatSubtract(s.usedCores, cores)
	}
	s.usedRAM -= ramMB
	s.usedDisk -= diskGB
	clog.Debug(ctx, "server release", "cores", cores, "RAM", ramMB, "disk", diskGB, "usedCores", s.usedCores,
		"usedZeroCores", s.usedZeroCores, "usedRAM", s.usedRAM, "usedDisk", s.usedDisk)

	// if the server is now doing nothing, we'll initiate a countdown to
	// destroying the host
	if s.usedCores <= 0 && s.usedZeroCores <= 0 && s.usedRAM <= 0 && s.TTD.Seconds() > 0 {
		clog.Debug(ctx, "server idle")
		go func() {
			defer internal.LogPanic(ctx, "server release", false)

			s.mutex.Lock()
			if s.onDeathrow {
				s.mutex.Unlock()
				clog.Debug(ctx, "server already on deathrow")
				return
			} else if s.usedCores > 0 || s.usedRAM > 0 {
				s.mutex.Unlock()
				clog.Debug(ctx, "allocated before entering deathrow")
				return
			}
			s.cancelDestruction = make(chan bool)
			s.onDeathrow = true
			s.mutex.Unlock()

			timeToDie := time.After(s.TTD)
			clog.Debug(ctx, "server entering deathrow", "death", time.Now().Add(s.TTD))
			for {
				select {
				case <-s.cancelDestruction:
					// *** this block needed to fail the "Run lots of jobs on a
					// deathrow server" scheduler test prior to fix, but we have
					// no reasonable way for a scheduler test to turn this on...
					// s.mutex.RLock()
					// if s.cancels <= 5 {
					// 	s.mutex.RUnlock()
					// 	<-time.After(2 * time.Second)
					// } else {
					// 	s.mutex.RUnlock()
					// }
					s.mutex.Lock()
					for i := 1; i < s.cancels; i++ {
						<-s.cancelDestruction
					}
					s.cancels = 0
					s.onDeathrow = false

					s.mutex.Unlock()
					clog.Debug(ctx, "server cancelled deathrow")
					return
				case <-timeToDie:
					// destroy the server
					s.mutex.Lock()
					s.onDeathrow = false
					s.toBeDestroyed = true
					s.mutex.Unlock()
					err := s.Destroy(ctx)
					clog.Debug(ctx, "server died on deathrow", "err", err)
					return
				}
			}
		}()
	}
}

// HasSpaceFor considers the current usage (according to prior Allocation calls)
// and tells you how many of a cmd needing the given resources can run on this
// server.
func (s *Server) HasSpaceFor(cores float64, ramMB, diskGB int) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.checkSpace(cores, ramMB, diskGB)
}

// checkSpace does the work of HasSpaceFor. You must hold a read lock on mutex!
func (s *Server) checkSpace(cores float64, ramMB, diskGB int) int {
	if s.destroyed {
		return 0
	}
	if internal.FloatLessThan(float64(s.Flavor.Cores)-s.usedCores, cores) || (s.Flavor.RAM-s.usedRAM < ramMB) || (s.Disk-s.usedDisk < diskGB) {
		return 0
	}

	var canDo int
	if cores == 0 {
		// rather than allow an infinite or very large number of cmds to run on
		// this server, because there are still real limits on the number of
		// processes we can run at once before things start falling over, we
		// only allow double the actual core count of zero core things to run
		// (on top of up to actual core count of non-zero core things).
		// On a server with "zero" cores, we also allow a reasonable number of
		// zero core jobs to run
		if s.Flavor.Cores == 0 {
			canDo = internal.ZeroCoreMultiplier*internal.ZeroCoreMultiplier - s.usedZeroCores
		} else {
			canDo = s.Flavor.Cores*internal.ZeroCoreMultiplier - s.usedZeroCores
		}
	} else {
		canDo = int(math.Floor(internal.FloatSubtract(float64(s.Flavor.Cores), s.usedCores) / cores))
	}
	if canDo > 1 {
		var n int
		if ramMB > 0 {
			n = (s.Flavor.RAM - s.usedRAM) / ramMB
			if n < canDo {
				canDo = n
			}
		}
		if diskGB > 0 {
			n = (s.Disk - s.usedDisk) / diskGB
			if n < canDo {
				canDo = n
			}
		}
	}
	return canDo
}

// createSSHClientConfig creates an ssh client config and stores it on self.
func (s *Server) createSSHClientConfig(ctx context.Context) error {
	if s.PrivateKey == "" {
		if s.provider != nil && s.provider.PrivateKey() == "" {
			clog.Error(ctx, "resource file did not contain the ssh key", "path", s.provider.savePath)
		}

		return errors.New("missing ssh key")
	}

	// parse private key and make config
	signer, err := ssh.ParsePrivateKey([]byte(s.PrivateKey))
	if err != nil {
		path := "unknown"
		if s.provider != nil {
			path = s.provider.savePath
		}
		clog.Error(ctx, "failed to parse private key", "path", path, "err", err)

		return err
	}
	s.sshClientConfig = &ssh.ClientConfig{
		User: s.UserName,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // *** don't currently know the server's host key, want to use ssh.FixedHostKey(publicKey) instead...
		Timeout:         sshShortTimeOut,
	}

	return nil
}

// SSHClient returns an ssh.Client object that could be used to ssh to the
// server. Requires that port 22 is accessible for SSH. The client returned will
// be one that hasn't failed to create a session yet; a new client will be
// created if necessary. You get back the client's index, so that if this client
// fails to create a session you can mark this client as bad.
func (s *Server) SSHClient(ctx context.Context) (*ssh.Client, int, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// return a client that is still good (most likely to be a more recent
	// client)
	numClients := len(s.sshClients)
	var client *ssh.Client
	var index int
	for i := numClients - 1; i >= 0; i-- {
		if s.sshClientSessions[i] < maxSSHSessions {
			client = s.sshClients[i]
			s.sshClientSessions[i]++
			index = i
			break
		}
	}
	if client != nil {
		return client, index, nil
	}

	// create a new client, add it to the pool
	if s.sshClientConfig == nil {
		err := s.createSSHClientConfig(ctx)
		if err != nil {
			return nil, index, err
		}
	}

	// dial in to the server, allowing certain errors that indicate that the
	// network or server isn't really ready for ssh yet; wait for up to
	// 5mins for success, if we had only just created this server
	hostAndPort := s.IP + ":22"
	client, err := sshDial(ctx, hostAndPort, s.sshClientConfig)
	if err != nil {
		// if we're trying to destroy this server, just give up straight away
		if s.destroyed {
			return nil, index, err
		}

		// otherwise, keep trying
		limit := time.After(sshTimeOut)
		ticker := time.NewTicker(1 * time.Second)
		ticks := 0
	DIAL:
		for {
			select {
			case <-ticker.C:
				client, err = sshDial(ctx, hostAndPort, s.sshClientConfig)

				// if it's a known "ssh still starting up" error, wait until the
				// timeout, unless ssh had worked previously, in which case
				// bail immediately if it's "no route to host"
				if err != nil && (strings.HasSuffix(err.Error(), "connection timed out") || strings.HasSuffix(err.Error(), "no route to host") || strings.HasSuffix(err.Error(), "connection refused") || (s.created && strings.HasSuffix(err.Error(), "connection could not be established"))) {
					if s.sshStarted && strings.HasSuffix(err.Error(), "no route to host") {
						err = errors.New("ssh used to work, but now there's no route to host")
						break DIAL
					}
					continue DIAL
				}

				// if it worked, we stop trying; if it failed again with a
				// different error, we keep trying for at least 45 seconds
				// to allow for the vagueries of OS start ups (eg. CentOS
				// brings up sshd and starts rejecting connections before
				// the centos user gets added)
				ticks++
				if err != nil && err.Error() == "connection attempt cancelled" {
					ticker.Stop()
					break DIAL
				}
				if err == nil || ticks == 9 || !s.created {
					ticker.Stop()
					break DIAL
				} else {
					continue DIAL
				}
			case <-limit:
				ticker.Stop()
				err = errors.New("giving up waiting for ssh to work")
				break DIAL
			case <-ctx.Done():
				ticker.Stop()
				err = errors.New("cancelled waiting for ssh to work")
				break DIAL
			}
		}
		if err != nil {
			return nil, index, err
		}
	}

	s.sshClients = append(s.sshClients, client)
	s.sshClientSessions = append(s.sshClientSessions, 1)
	s.sshStarted = true

	return client, len(s.sshClients) - 1, nil
}

// sshDial calls ssh.Dial() and enforces the config's timeout, which ssh.Dial()
// doesn't always seem to obey.
func sshDial(ctx context.Context, addr string, sshConfig *ssh.ClientConfig) (*ssh.Client, error) {
	clientCh := make(chan *ssh.Client, 1)
	errCh := make(chan error, 1)
	go func() {
		defer internal.LogPanic(ctx, "sshDial", false)
		sshClient, err := ssh.Dial("tcp", addr, sshConfig)
		clientCh <- sshClient
		errCh <- err
	}()
	deadline := time.After(sshConfig.Timeout + 1*time.Second)
	select {
	case err := <-errCh:
		return <-clientCh, err
	case <-deadline:
		return nil, fmt.Errorf("connection could not be established")
	case <-ctx.Done():
		return nil, fmt.Errorf("connection attempt cancelled")
	}
}

// SSHSession returns an ssh.Session object that could be used to do things via
// ssh on the server. Will time out and return an error if the session can't be
// created within 5s. Also returns the index of the client this session came
// from, so that when you can call CloseSSHSession() when you're done with the
// returned session.
func (s *Server) SSHSession(ctx context.Context) (*ssh.Session, int, error) {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	sshClient, clientIndex, err := s.SSHClient(ctx)
	if err != nil {
		clog.Debug(ctx, "server ssh could not be established", "err", err)
		return nil, clientIndex, fmt.Errorf("cloud SSHSession() failed to get a client: %s", err.Error())
	}

	// *** even though sshclient has a timeout, it still hangs forever if we
	// try to get a NewSession to a dead server, so we implement our own 5s
	// timeout here

	done := make(chan error, 1)
	worked := make(chan bool, 1)
	sessionCh := make(chan *ssh.Session, 1)
	go func() {
		select {
		case <-time.After(sshShortTimeOut):
			clog.Debug(ctx, "server ssh timed out", "clientindex", clientIndex)
			done <- fmt.Errorf("cloud SSHSession() timed out")
		case <-ctx.Done():
			clog.Debug(ctx, "server ssh cancelled", "clientindex", clientIndex)
			done <- fmt.Errorf("cloud SSHSession() cancelled")
		case <-worked:
			return
		}
	}()
	go func() {
		defer internal.LogPanic(ctx, "server sshsession", false)
		session, errf := sshClient.NewSession()
		if errf != nil {
			clog.Debug(ctx, "server ssh failed", "err", errf, "clientindex", clientIndex)
			done <- fmt.Errorf("cloud SSHSession() failed to esatablish a session: %s", errf.Error())
			return
		}
		worked <- true
		done <- nil
		sessionCh <- session
	}()

	err = <-done
	if err != nil {
		s.mutex.Lock()
		// pretend we're now at max sessions, so this client won't be used again
		// in the future, at least until sessions get closed, when it might
		// start working again
		s.sshClientSessions[clientIndex] = maxSSHSessions
		s.mutex.Unlock()
		return nil, clientIndex, err
	}

	return <-sessionCh, clientIndex, nil
}

// CloseSSHSession is used to close a session opened with SSHSession(). If the
// client used to create the session (as indicated by the supplied index, also
// retrieved from SSHSession()) was marked as bad, it will now be marked as
// good, on the assumption there is now "space" for a new session.
func (s *Server) CloseSSHSession(ctx context.Context, session *ssh.Session, clientIndex int) {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	err := session.Close()
	s.closeWarning(ctx, err)

	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.sshClientSessions[clientIndex]--
}

// closeWarning warns about the given error if not nil, unless it is expected
// in a close situation.
func (s *Server) closeWarning(ctx context.Context, err error) {
	if err != nil && err.Error() != "EOF" && !strings.Contains(err.Error(), "use of closed network connection") {
		clog.Warn(ctx, "failed to close ssh session", "err", err)
	}
}

// SFTPClient is like sftp.NewClient(), but the underlying
// clientConn.conn.WriteCloser is mutex protected to avoid data races between
// closes due to errors and direct Close() calls on the *sftp.Client.
func SFTPClient(conn *ssh.Client) (*sftp.Client, error) {
	s, err := conn.NewSession()
	if err != nil {
		return nil, err
	}
	if err = s.RequestSubsystem("sftp"); err != nil {
		return nil, err
	}
	pw, err := s.StdinPipe()
	if err != nil {
		return nil, err
	}
	pw = &threadSafeWriteCloser{WriteCloser: pw}
	pr, err := s.StdoutPipe()
	if err != nil {
		return nil, err
	}

	return sftp.NewClientPipe(pr, pw)
}

type threadSafeWriteCloser struct {
	io.WriteCloser
	sync.Mutex
}

func (c *threadSafeWriteCloser) Close() error {
	c.Lock()
	defer c.Unlock()
	return c.WriteCloser.Close()
}

// RunCmd runs the given command on the server, optionally in the background.
// You get the command's STDOUT and STDERR as strings.
func (s *Server) RunCmd(ctx context.Context, cmd string, background bool) (stdout, stderr string, err error) {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	// create a session
	session, clientIndex, err := s.SSHSession(ctx)
	if err != nil {
		return stdout, stderr, err
	}

	defer s.CloseSSHSession(ctx, session, clientIndex)

	// if the sever is destroyed while running, arrange to immediately return an
	// error
	s.mutex.Lock()
	cancelID := s.cancelID
	s.cancelID = cancelID + 1
	cancelCh := make(chan bool, 1)
	s.cancelRunCmd[cancelID] = cancelCh
	done := make(chan error, 1)
	outCh := make(chan string, 1)
	errCh := make(chan string, 1)
	finished := make(chan bool, 1)
	go func() {
		defer internal.LogPanic(ctx, "server runcmd cancellation", false)
		select {
		case <-cancelCh:
			outCh <- ""
			errCh <- ""
			done <- fmt.Errorf("cloud RunCmd() cancelled due to destruction of server %s", s.ID)
		case <-ctx.Done():
			outCh <- ""
			errCh <- ""
			done <- fmt.Errorf("cloud RunCmd() on server %s cancelled on request", s.ID)
		case <-finished:
			// end select
		}
		s.mutex.Lock()
		close(cancelCh)
		delete(s.cancelRunCmd, cancelID)
		s.mutex.Unlock()
	}()
	go func() {
		defer internal.LogPanic(ctx, "server runcmd", false)

		// run the command, returning stdout
		if background {
			cmd = "sh -c 'nohup " + cmd + " > /dev/null 2>&1 &'"
		}
		var o bytes.Buffer
		var e bytes.Buffer
		session.Stdout = &o
		session.Stderr = &e
		errf := session.Run(cmd)
		finished <- true
		if o.Len() > 0 {
			outCh <- o.String()
		} else {
			outCh <- ""
		}
		if e.Len() > 0 {
			errCh <- e.String()
		} else {
			errCh <- ""
		}
		if errf != nil {
			done <- fmt.Errorf("cloud RunCmd(%s) failed: %s", cmd, errf.Error())
		} else {
			done <- nil
		}
	}()
	s.mutex.Unlock()

	err = <-done
	stdout = <-outCh
	stderr = <-errCh
	return stdout, stderr, err
}

// UploadFile uploads a local file to the given location on the server.
func (s *Server) UploadFile(ctx context.Context, source string, dest string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	sshClient, _, err := s.SSHClient(ctx)
	if err != nil {
		return err
	}

	client, err := SFTPClient(sshClient)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, client, "upload file client session", "source", source, "dest", dest)

	// create all parent dirs of dest
	err = s.MkDir(ctx, filepath.Dir(dest))
	if err != nil {
		return err
	}

	// open source, create dest
	sourceFile, err := os.Open(source)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, sourceFile, "upload file source", "source", source, "dest", dest)

	destFile, err := client.Create(dest)
	if err != nil {
		return err
	}

	// copy the file content over
	_, err = io.Copy(destFile, sourceFile)
	return err
}

// CopyOver uploads the given local files to the corresponding locations on the
// server. files argument is a comma separated list of local file paths.
// Absolute paths are uploaded to the same absolute path on the server. Paths
// beginning with ~/ are uploaded from the local home directory to the server's
// home directory.
//
// If local path and desired remote path are unrelated, the paths can be
// separated with a colon.
//
// If a specified local path does not exist, it is silently ignored, allowing
// the specification of multiple possible config files when you might only have
// one. The mtimes of the files are retained.
//
// NB: currently only works if the server supports the command 'pwd'.
func (s *Server) CopyOver(ctx context.Context, files string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	for _, path := range strings.Split(files, ",") {
		split := strings.Split(path, ":")
		var localPath, remotePath string
		if len(split) == 2 {
			localPath = split[0]
			remotePath = split[1]
		} else {
			localPath = path
			remotePath = path
		}

		// ignore if it doesn't exist locally
		localPath = internal.TildaToHome(localPath)
		info, err := os.Stat(localPath)
		if err != nil {
			err = nil
			continue
		}

		if strings.HasPrefix(remotePath, "~/") {
			homeDir, errh := s.HomeDir(ctx)
			if errh != nil {
				return errh
			}
			remotePath = strings.TrimLeft(remotePath, "~/")
			remotePath = filepath.Join(homeDir, remotePath)
		}

		err = s.UploadFile(ctx, localPath, remotePath)
		if err != nil {
			return err
		}

		// if these are config files we likely need to make them user-only read,
		// and if they're not, I can't see how it matters if group/all can't
		// read? This is a single user server and I'm the only one using it...
		_, _, err = s.RunCmd(ctx, "chmod 600 "+remotePath, false)
		if err != nil {
			return err
		}

		// sometimes the mtime of the file matters, so we try and set that on
		// the remote copy
		_, _, err = s.RunCmd(ctx, fmt.Sprintf("touch -d %s %s", info.ModTime().Format(touchStampFormat), remotePath), false)
		if err != nil {
			return err
		}
	}

	return nil
}

// HomeDir gets the absolute path to the server's home directory. Depends on
// 'pwd' command existing on the server.
func (s *Server) HomeDir(ctx context.Context) (string, error) {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.hmutex.Lock()
	defer s.hmutex.Unlock()
	if s.homeDir != "" {
		return s.homeDir, nil
	}

	stdout, _, err := s.RunCmd(ctx, "pwd", false)
	if err != nil {
		return "", err
	}
	s.homeDir = strings.TrimSuffix(stdout, "\n")
	return s.homeDir, nil
}

// CreateFile creates a new file with the given content on the server.
func (s *Server) CreateFile(ctx context.Context, content string, dest string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	sshClient, _, err := s.SSHClient(ctx)
	if err != nil {
		return err
	}

	client, err := SFTPClient(sshClient)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, client, "create file client session")

	// create all parent dirs of dest
	err = s.MkDir(ctx, filepath.Dir(dest))
	if err != nil {
		return err
	}

	// create dest
	destFile, err := client.Create(dest)
	if err != nil {
		return err
	}

	// write the content
	_, err = io.WriteString(destFile, content)
	return err
}

// DownloadFile downloads a file from the server and stores it locally. The
// directory for your local file must already exist.
func (s *Server) DownloadFile(ctx context.Context, source string, dest string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	sshClient, _, err := s.SSHClient(ctx)
	if err != nil {
		return err
	}

	client, err := SFTPClient(sshClient)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, client, "download file client session", "source", source, "dest", dest)

	// open source, create dest
	sourceFile, err := client.Open(source)
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, sourceFile, "download file source", "source", source, "dest", dest)

	destFile, err := os.Create(dest)
	if err != nil {
		return err
	}

	// copy the file content over
	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return os.Chmod(dest, 0600)
}

// MkDir creates a directory (and it's parents as necessary) on the server.
// Requires sudo.
func (s *Server) MkDir(ctx context.Context, dir string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	if dir == "." {
		return nil
	}

	//*** it would be nice to do this with client.Mkdir, but that doesn't do
	// the equivalent of mkdir -p, and errors out if dirs already exist... for
	// now it's easier to just call mkdir
	_, _, err := s.RunCmd(ctx, fmt.Sprintf("[ -d %s ]", dir), false)
	if err == nil {
		// dir already exists
		return nil
	}

	// try without sudo, so that if we create multiple dirs, they all have the
	// correct permissions
	_, _, err = s.RunCmd(ctx, "mkdir -p "+dir, false)
	if err == nil {
		return nil
	}

	// try again with sudo
	_, e, err := s.RunCmd(ctx, "sudo mkdir -p "+dir, false)
	if err != nil {
		return fmt.Errorf("%s; %s", e, err.Error())
	}

	// correct permission on leaf dir *** not currently correcting permission on
	// any parent dirs we might have just made
	_, e, err = s.RunCmd(ctx, fmt.Sprintf("sudo chown %s:%s %s", s.UserName, s.UserName, dir), false)
	if err != nil {
		return fmt.Errorf("%s; %s", e, err.Error())
	}

	return nil
}

// CreateSharedDisk creates an NFS share at /shared, which must be empty or not
// exist. This does not work for remote Servers, so only call this on the return
// value of LocalhostServer(). Does nothing and returns nil if the share was
// already created. NB: this is currently hard-coded to only work on Ubuntu, and
// the ability to sudo is required! Also assumes you don't have any other shares
// configured, and no other process started the NFS server!
func (s *Server) CreateSharedDisk() error {
	s.csmutex.Lock()
	defer s.csmutex.Unlock()
	if s.createdShare {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", "-c", "sudo apt-get update && sudo apt-get install nfs-kernel-server -y") // #nosec
	err := cmd.Run()
	if err != nil {
		return err
	}

	f, err := os.Open("/etc/exports")
	if err != nil {
		return err
	}

	defer internal.LogClose(ctx, f, "/etc/exports")

	scanner := bufio.NewScanner(f)
	var found bool
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), sharePath) {
			found = true
			break
		}
	}
	if !found {
		cmd = exec.CommandContext(ctx, "bash", "-c", fmt.Sprintf("echo '%s *(rw,sync,no_root_squash)' | sudo tee --append /etc/exports > /dev/null", sharePath)) // #nosec
		err = cmd.Run()
		if err != nil {
			return err
		}
	}

	if _, errs := os.Stat(sharePath); errs != nil && os.IsNotExist(errs) {
		cmd = exec.CommandContext(ctx, "bash", "-c", "sudo mkdir "+sharePath) // #nosec
		errs = cmd.Run()
		if errs != nil {
			return errs
		}

		cmd = exec.CommandContext(ctx, "bash", "-c", fmt.Sprintf("sudo chown %s:%s %s", s.UserName, s.UserName, sharePath)) // #nosec
		errs = cmd.Run()
		if errs != nil {
			return errs
		}
	}

	cmd = exec.CommandContext(ctx, "bash", "-c", "sudo systemctl start nfs-kernel-server.service && sudo export"+"fs -a") // #nosec (the split is to avoid a false-positive spelling mistake)
	err = cmd.Run()
	if err != nil {
		return err
	}

	s.createdShare = true
	s.SharedDisk = true
	return nil
}

// MountSharedDisk can be used to mount a share from another Server (identified
// by its IP address) that you called CreateSharedDisk() on. The shared disk
// will be accessible at /shared. Does nothing and returns nil if the share was
// already mounted (or created on this Server). NB: currently hard-coded to use
// apt-get to install nfs-common on the server first, so probably only
// compatible with Ubuntu. Requires sudo.
func (s *Server) MountSharedDisk(ctx context.Context, nfsServerIP string) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.csmutex.Lock()
	defer s.csmutex.Unlock()
	if s.createdShare {
		return nil
	}

	_, _, err := s.RunCmd(ctx, "sudo apt-get update && sudo apt-get install nfs-common -y", false)
	if err != nil {
		return err
	}

	err = s.MkDir(ctx, sharePath)
	if err != nil {
		return err
	}

	clog.Debug(ctx, "ran MkDir")

	stdo, stde, err := s.RunCmd(ctx, fmt.Sprintf("sudo mount %s:%s %s", nfsServerIP, sharePath, sharePath), false)
	if err != nil {
		clog.Error(ctx, "mount attempt failed", "stdout", stdo, "stderr", stde)
		return err
	}

	s.createdShare = true
	s.SharedDisk = true

	clog.Debug(ctx, "mounted shared disk")

	return nil
}

// GoneBad lets you mark a server as having something wrong with it, so you can
// avoid using it in the future, until the problems are confirmed. (At that
// point you'd either Destroy() it, or if this was a false alarm, call
// NotBad()).
//
// The optional permanentProblem arg (some explanatory error message) makes it
// such that NotBad() will have no effect. For use when the server is Alive()
// but you just never want to re-use this server. The only reason you don't just
// Destroy() it is that you want to allow an end user to investigate the server
// manually.
func (s *Server) GoneBad(permanentProblem ...string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.goneBad = time.Now()

	if len(permanentProblem) == 1 {
		s.permanentProblem = permanentProblem[0]
	}
}

// NotBad lets you change your mind about a server you called GoneBad() on.
// (Unless GoneBad() was called with a permanentProblem, or the server has been
// destroyed).
func (s *Server) NotBad() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if !s.destroyed && !s.toBeDestroyed && s.permanentProblem == "" {
		s.goneBad = time.Time{}
		return true
	}
	return false
}

// IsBad tells you if GoneBad() has been called (more recently than NotBad()).
func (s *Server) IsBad() bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return !s.goneBad.IsZero()
}

// BadDuration tells you how long it has been since the last GoneBad() call
// (when there hasn't been a NotBad() call since). Returns 0 seconds if not
// actually bad right now.
func (s *Server) BadDuration() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.goneBad.IsZero() {
		return 0 * time.Second
	}
	return time.Since(s.goneBad)
}

// PermanentProblem tells you if GoneBad("problem message") has been called,
// returning that reason the server is not usable.
func (s *Server) PermanentProblem() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.permanentProblem
}

// Destroy immediately destroys the server.
func (s *Server) Destroy(ctx context.Context) error {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.destroyed {
		return nil
	}

	// if the server has initiated its countdown to destruction, cancel that
	if s.onDeathrow {
		s.cancelDestruction <- true
	}

	// if the user is in the middle of RunCmd(), have those return an error now
	for _, ch := range s.cancelRunCmd {
		ch <- true
	}

	s.toBeDestroyed = false
	s.destroyed = true

	if s.sshStarted {
		s.mutex.Unlock()
		// sync the filesystem
		t := time.Now()
		session, clientIndex, err := s.SSHSession(context.Background())
		if err != nil {
			clog.Warn(ctx, "failed to ssh to cleanly shutdown", "took", time.Since(t), "err", err)
		} else {
			t = time.Now()
			stdo, stde, err := s.RunCmd(context.Background(), cleanShutDownCmd, false)
			rt := time.Since(t)
			if err != nil {
				clog.Warn(ctx, "clean shutdown failed", "took", rt, "err", err, "stdout", stdo, "stderr", stde)
			} else if rt > 10*time.Second {
				clog.Warn(ctx, "clean shutdown took a long time", "took", rt, "stdout", stdo)
			}
			s.CloseSSHSession(ctx, session, clientIndex)
		}
		s.mutex.Lock()
	}

	// explicitly close any client connections
	for _, client := range s.sshClients {
		err := client.Close()
		s.closeWarning(ctx, err)
	}

	if s.goneBad.IsZero() {
		s.goneBad = time.Now()
	}

	// for testing purposes, we anticipate that provider isn't set
	if s.provider == nil {
		return fmt.Errorf("provider not set")
	}

	err := s.provider.DestroyServer(ctx, s.ID)
	clog.Debug(ctx, "server destroyed", "err", err)
	if err != nil {
		// check if the server exists
		ok, errc := s.provider.CheckServer(ctx, s.ID)
		if ok && errc == nil {
			return err
		}
		// if not, assume there's no Server and ignore this error (which may
		// just be along the lines of "the server doesn't exist")
		return nil
	}

	return err
}

// Destroyed tells you if a server was destroyed using Destroy() or the
// automatic destruction due to being idle. It is NOT the opposite of Alive(),
// since it does not check if the server is still usable.
func (s *Server) Destroyed() bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.destroyed || s.toBeDestroyed
}

// Alive tells you if a server is usable. It first does the same check as
// Destroyed() before calling out to the provider. Supplying an optional boolean
// will double check the server to make sure it can be ssh'd to. If the server
// doesn't exist, it will be removed from the provider's resources file.
func (s *Server) Alive(ctx context.Context, checkSSH ...bool) bool {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	s.mutex.Lock()
	if s.destroyed || s.toBeDestroyed {
		s.mutex.Unlock()
		return false
	}

	ok, errc := s.provider.CheckServer(ctx, s.ID)
	if !ok || errc != nil {
		s.mutex.Unlock()
		return false
	}
	s.mutex.Unlock()

	if len(checkSSH) == 1 && checkSSH[0] {
		// provider may claim the server is fine, but it might not really be
		// usable; confirm we can still ssh to it
		session, clientIndex, err := s.SSHSession(context.Background())
		if err != nil {
			return false
		}

		s.CloseSSHSession(ctx, session, clientIndex)
	}

	return true
}

// Known tells you if a server exists according to the provider. This can
// return false even if the server exists, because the credentials you used for
// the provider are different to the ones used to create this server. If the
// server isn't known about, the provider's resource file is NOT updated,
// because this indicates you're using the wrong resource file for these
// credentials.
func (s *Server) Known(ctx context.Context) bool {
	ctx = clog.ContextWithServerID(ctx, s.ID)
	known, err := s.provider.ServerIsKnown(s.ID)
	if err != nil {
		clog.Warn(ctx, "could not check if the server is known about", "err", err)
	}

	return known
}
