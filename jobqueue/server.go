// Copyright © 2016-2020 Genome Research Limited
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

// This file contains the functions to implement a jobqueue server.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	"github.com/VertebrateResequencing/wr/cloud"
	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/VertebrateResequencing/wr/limiter"
	"github.com/VertebrateResequencing/wr/queue"
	"github.com/gorilla/websocket"
	"github.com/grafov/bcast" // *** must be commit e9affb593f6c871f9b4c3ee6a3c77d421fe953df or status web page updates break in certain cases
	"github.com/inconshreveable/log15"
	logext "github.com/inconshreveable/log15/ext"
	"github.com/sb10/waitgroup"
	"github.com/ugorji/go/codec"
	mangos "nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/rep"
	"nanomsg.org/go-mangos/transport/tlstcp"
)

// Err* constants are found in our returned Errors under err.Err, so you can
// cast and check if it's a certain type of error. ServerMode* constants are
// used to report on the status of the server, found inside ServerInfo.
const (
	ErrInternalError    = "internal error"
	ErrUnknownCommand   = "unknown command"
	ErrBadRequest       = "bad request (missing arguments?)"
	ErrBadJob           = "bad job (not in queue or correct sub-queue)"
	ErrMissingJob       = "corresponding job not found"
	ErrUnknown          = "unknown error"
	ErrClosedInt        = "queues closed due to SIGINT"
	ErrClosedTerm       = "queues closed due to SIGTERM"
	ErrClosedCert       = "queues closed due to certificate expiry"
	ErrClosedStop       = "queues closed due to manual Stop()"
	ErrQueueClosed      = "queue closed"
	ErrNoHost           = "could not determine the non-loopback ip address of this host"
	ErrNoServer         = "could not reach the server"
	ErrMustReserve      = "you must Reserve() a Job before passing it to other methods"
	ErrDBError          = "failed to use database"
	ErrS3DBBackupPath   = "invalid S3 database backup path"
	ErrPermissionDenied = "bad token: permission denied"
	ErrBeingDrained     = "server is being drained"
	ErrStopReserving    = "recovered on a new server; you should stop reserving"
	ErrBadLimitGroup    = "colons in limit group names must be followed by integers"
	ServerModeNormal    = "started"
	ServerModePause     = "paused"
	ServerModeDrain     = "draining"
)

// ServerVersion gets set during build:
// go build -ldflags "-X github.com/VertebrateResequencing/wr/jobqueue.ServerVersion=`git describe --tags --always --long --dirty`"
var ServerVersion string

// these global variables are primarily exported for testing purposes; you
// probably shouldn't change them (*** and they should probably be re-factored
// as fields of a config struct...)
var (
	ServerInterruptTime                             = 1 * time.Second
	ServerItemTTR                                   = 60 * time.Second
	ServerReserveTicker                             = 1 * time.Second
	ServerCheckRunnerTime                           = 1 * time.Minute
	ServerShutdownWaitTime                          = 5 * time.Second
	ServerLostJobCheckTimeout                       = 15 * time.Second
	ServerLostJobCheckRetryTime                     = 30 * time.Minute
	ServerMaximumRunForResourceRecommendation       = 100
	ServerMinimumScheduledForResourceRecommendation = 10
	ServerLogClientErrors                           = true
	serverShutdownRunnerTickerTime                  = 50 * time.Millisecond

	// httpServerShutdownTime is the time we'll wait before forcing
	// http.Server{}.Shutdown() to complete, otherwise it takes 500ms if there
	// were listeners.
	httpServerShutdownTime = 1 * time.Millisecond
)

// BsubID is used to give added jobs a unique (atomically incremented) id when
// pretending to be bsub.
var BsubID uint64

// Error records an error and the operation and item that caused it.
type Error struct {
	Op   string // name of the method
	Item string // the item's key
	Err  string // one of our Err* vars
}

func (e Error) Error() string {
	return "jobqueue " + e.Op + "(" + e.Item + "): " + e.Err
}

// serverResponse is the struct that the server sends to clients over the
// network in response to their clientRequest.
type serverResponse struct {
	Err         string // string instead of error so we can decode on the client side
	Added       int
	Existed     int
	AddedIDs    []string
	Modified    map[string]string
	KillCalled  bool
	Job         *Job
	Jobs        []*Job
	Limit       int
	LimitGroups map[string]int
	SInfo       *ServerInfo
	SStats      *ServerStats
	DB          []byte
	Path        string
	BadServers  []*BadServer
}

// ServerInfo holds basic addressing info about the server.
type ServerInfo struct {
	Addr       string // ip:port
	Host       string // hostname
	Port       string // port
	WebPort    string // port of the web interface
	PID        int    // process id of server
	Deployment string // deployment the server is running under
	Scheduler  string // the name of the scheduler that jobs are being submitted to
	Mode       string // ServerModeNormal if the server is running normally, or ServerModeDrain|Paused if draining or paused
}

// ServerVersions holds the server version (git tag) and API version supported.
type ServerVersions struct {
	Version string
	API     string
}

// ServerStats holds information about the jobqueue server for sending to
// clients.
type ServerStats struct {
	Delayed int           // how many jobs are waiting following a possibly transient error
	Ready   int           // how many jobs are ready to begin running
	Running int           // how many jobs are currently running
	Buried  int           // how many jobs are no longer being processed because of seemingly permanent errors
	ETC     time.Duration // how long until the the slowest of the currently running jobs is expected to complete
}

type rgToKeys struct {
	sync.RWMutex
	lookup map[string]map[string]bool
}

// jstateCount is the state count change we send to the status webpage; we are
// representing the jobs moving from one state to another.
type jstateCount struct {
	RepGroup  string // "+all+" is the special group representing all live jobs across all RepGroups
	FromState JobState
	ToState   JobState
	Count     int // num in FromState drop by this much, num in ToState rise by this much
}

// BadServer is the details of servers that have gone bad that we send to the
// status webpage. Previously bad servers can also be sent if they become good
// again, hence the IsBad boolean.
type BadServer struct {
	ID      string
	Name    string
	IP      string
	Date    int64 // seconds since Unix epoch
	IsBad   bool
	Problem string
}

// schedulerIssue is the details of scheduler problems encountered that we send
// to the status webpage.
type schedulerIssue struct {
	Msg       string
	FirstDate int64 // seconds since Unix epoch
	LastDate  int64
	Count     int // the number of identical Msg sent
}

// sgroup represents a scheduler group
type sgroup struct {
	name     string
	count    int
	skipped  int
	req      *scheduler.Requirements
	priority uint8
	sync.RWMutex
}

// clone creates a new copy of the sgroup with the given count
func (s *sgroup) clone(count int) *sgroup {
	s.RLock()
	defer s.RUnlock()
	return &sgroup{
		name:     s.name,
		count:    count,
		skipped:  s.skipped,
		req:      s.req.Clone(),
		priority: s.priority,
	}
}

// getCount is a thread-safe way of getting the current count
func (s *sgroup) getCount() int {
	s.RLock()
	defer s.RUnlock()
	return s.count
}

// decrement is a thread-safe way of dropping the count of the group by the
// given amount.
//
// If the sgroup's skipped is greater than 0, first decrements that and only
// decrements count if given drop is greater than skipped.
//
// Returns the new count, or -1 if the count didn't change.
func (s *sgroup) decrement(drop int) int {
	if drop < 1 {
		return -1
	}
	s.Lock()
	defer s.Unlock()

	if s.skipped > 0 {
		if drop <= s.skipped {
			s.skipped -= drop
			return -1
		}
		drop -= s.skipped
		s.skipped = 0
	}

	prev := s.count
	s.count -= drop
	if s.count < 0 {
		s.count = 0
	}

	if s.count == prev {
		return -1
	}
	return s.count
}

// hasSkips is a thread-safe way of seeing if skipped is greater than 0.
func (s *sgroup) hasSkips() bool {
	s.RLock()
	defer s.RUnlock()
	return s.skipped > 0
}

// Server represents the server side of the socket that clients Connect() to.
type Server struct {
	token     []byte
	uploadDir string
	sock      mangos.Socket
	ch        codec.Handle
	rc        string // runner command string compatible with fmt.Sprintf(..., schedulerGroup, deployment, serverAddr, reserveTimeout, maxMinsAllowed)
	log15.Logger
	ServerInfo                *ServerInfo
	ServerVersions            *ServerVersions
	db                        *db
	done                      chan error
	stopSigHandling           chan bool
	stopClientHandling        chan bool
	wg                        *waitgroup.WaitGroup
	q                         *queue.Queue
	rpl                       *rgToKeys
	limiter                   *limiter.Limiter
	scheduler                 *scheduler.Scheduler
	previouslyScheduledGroups map[string]*sgroup
	httpServer                *http.Server
	statusCaster              *bcast.Group
	badServerCaster           *bcast.Group
	schedCaster               *bcast.Group
	racCheckTimer             *time.Timer
	pauseRequests             int
	wsconns                   map[string]*websocket.Conn
	badServers                map[string]*cloud.Server
	schedIssues               map[string]*schedulerIssue
	racmutex                  sync.RWMutex // to protect the readyaddedcallback
	bsmutex                   sync.RWMutex
	simutex                   sync.RWMutex
	krmutex                   sync.RWMutex
	ssmutex                   sync.RWMutex // "server state mutex" to protect up, drain, blocking and ServerInfo.Mode
	psgmutex                  sync.RWMutex // to protect previouslyScheduledGroups
	rpmutex                   sync.Mutex   // to protect racPending, racRunning and waitingReserves
	sync.Mutex
	wsmutex              sync.Mutex
	up                   bool
	drain                bool
	blocking             bool
	racChecking          bool
	killRunners          bool
	racPending           bool
	racRunning           bool
	waitingReserves      []chan struct{}
	recoveredRunningJobs map[string]bool
}

// ServerConfig is supplied to Serve() to configure your jobqueue server. All
// fields are required with no working default unless otherwise noted.
type ServerConfig struct {
	// Port for client-server communication.
	Port string

	// Port for the web interface.
	WebPort string

	// Name of the desired scheduler (eg. "local" or "lsf" or "openstack") that
	// jobs will be submitted to.
	SchedulerName string

	// SchedulerConfig should define the config options needed by the chosen
	// scheduler, eg. scheduler.ConfigLocal{Deployment: "production", Shell:
	// "bash"} if using the local scheduler.
	SchedulerConfig interface{}

	// The command line needed to bring up a jobqueue runner client, which
	// should contain 6 %s parts which will be replaced with the scheduler
	// group, deployment, ip:host address of the server, domain name that the
	// server's certificate should be valid for, reservation time out and
	// maximum number of minutes allowed, eg. "my_jobqueue_runner_client --group
	// '%s' --deployment %s --server '%s' --domain %s --reserve_timeout %d
	// --max_mins %d". If you supply an empty string (the default), runner
	// clients will not be spawned; for any work to be done you will have to run
	// your runner client yourself manually.
	RunnerCmd string

	// Absolute path to where the database file should be saved. The database is
	// used to ensure no loss of added commands, to keep a permanent history of
	// all jobs completed, and to keep various stats, amongst other things.
	DBFile string

	// Absolute path to where the database file should be backed up to.
	DBFileBackup string

	// Absolute path to where the server will store the authorization token
	// needed by clients to communicate with the server. Storing it in a file
	// could make using any CLI clients more convenient. The file will be
	// read-only by the user starting the server. The default of empty string
	// means the token is not saved to disk.
	TokenFile string

	// Absolute path to where CA PEM file is that will be used for
	// securing access to the web interface. If the given file does not exist,
	// a certificate will be generated for you at this path.
	CAFile string

	// Absolute path to where certificate PEM file is that will be used for
	// securing access to the web interface. If the given file does not exist,
	// a certificate will be generated for you at this path.
	CertFile string

	// Absolute path to where key PEM file is that will be used for securing
	// access to the web interface. If the given file does not exist, a
	// key will be generated for you at this path.
	KeyFile string

	// Domain that a generated CertFile should be valid for. If not supplied,
	// defaults to "localhost".
	//
	// When using your own CertFile, this should be set to a domain that the
	// certifcate is valid for, as when the server spawns clients, those clients
	// will validate the server's certifcate based on this domain. For the web
	// interface and REST API, it is up to you to ensure that your DNS has an
	// entry for this domain that points to the IP address of the machine
	// running your server.
	CertDomain string

	// If using a CertDomain, and if you have (or very soon will) set the domain
	// to point to the server's IP address, set this to true. This will result
	// in runner clients spawned by the server being told to access the server
	// at CertDomain (instead of the current IP address), which means if the
	// server's host is lost and you bring it back at a different IP address and
	// update the domain again, those clients will be able to reconnect and
	// continue running.
	DomainMatchesIP bool

	// AutoConfirmDead is the time that a spawned server must be considered
	// dead before it is automatically destroyed and jobs running on it are
	// confirmed lost. The default of 0 time disables automatic destruction.
	// Only relevant when using a scheduler that spawns servers on which to
	// execute jobs.
	AutoConfirmDead time.Duration

	// Name of the deployment ("development" or "production"); development
	// databases are deleted and recreated on start up by default.
	Deployment string

	// CIDR is the IP address range of your network. When the server needs to
	// know its own IP address, it uses this CIDR to confirm it got it correct
	// (ie. it picked the correct network interface). You can leave this unset,
	// in which case it will do its best to pick correctly. (This is only a
	// possible issue if you have multiple network interfaces.)
	CIDR string

	// UploadDir is the directory where files uploaded to the Server will be
	// stored. They get given unique names based on the MD5 checksum of the file
	// uploaded. Defaults to /tmp.
	UploadDir string

	// Logger is a logger object that will be used to log uncaught errors and
	// debug statements. "Uncought" errors are all errors generated during
	// operation that either shouldn't affect the success of operations, and can
	// be ignored (logged at the Warn level, and which is why the errors are not
	// returned by the methods generating them), or errors that could not be
	// returned (logged at the Error level, eg. generated during a go routine,
	// such as errors by the server handling a particular client request).
	// We attempt to recover from panics during server operation and log these
	// at the Crit level.
	//
	// If your logger is levelled and set to the debug level, you will also get
	// information tracking the inner workings of the server.
	//
	// If this is unset, nothing is logged (defaults to a logger using a
	// log15.DiscardHandler()).
	Logger log15.Logger
}

// Serve is for use by a server executable and makes it start listening on
// localhost at the configured port for Connect()ions from clients, and then
// handles those clients.
//
// It returns a *Server that you will typically call Block() on to block until
// your executable receives a SIGINT or SIGTERM, or you call Stop(), at which
// point the queues will be safely closed (you'd probably just exit at that
// point).
//
// If it creates a db file or recreates one from backup, and if it creates TLS
// certificates, it will say what it did in the returned msg string.
//
// The returned token must be provided by any client to authenticate. The server
// is a single user system, so there is only 1 token kept for its entire
// lifetime. If config.TokenFile has been set, the token will also be written to
// that file, potentially making it easier for any CLI clients to authenticate
// with this returned Server. If that file already exists prior to calling this,
// the token in that file will be re-used, allowing reconnection of existing
// clients if this server dies ungracefully.
//
// The possible errors from Serve() will be related to not being able to start
// up at the supplied address; errors encountered while dealing with clients are
// logged but otherwise ignored.
//
// It also spawns your runner clients as needed, running them via the configured
// job scheduler, using the configured shell. It determines the command line to
// execute for your runner client from the configured RunnerCmd string you
// supplied.
func Serve(config ServerConfig) (s *Server, msg string, token []byte, err error) {
	// if a logger was configured we will log debug statements and "harmless"
	// errors not worth returning (or not possible to return), along with
	// panics. Otherwise we create a default logger that discards all log
	// attempts.
	serverLogger := config.Logger
	if serverLogger == nil {
		serverLogger = log15.New()
		serverLogger.SetHandler(log15.DiscardHandler())
	} else {
		serverLogger = serverLogger.New()
	}
	defer internal.LogPanic(serverLogger, "jobqueue serve", true)

	// generate a secure token for clients to authenticate with
	token, err = generateToken(config.TokenFile)
	if err != nil {
		return s, msg, token, err
	}

	// check if the cert files are available
	httpAddr := "0.0.0.0:" + config.WebPort
	caFile := config.CAFile
	certFile := config.CertFile
	keyFile := config.KeyFile
	certDomain := config.CertDomain
	if certDomain == "" {
		certDomain = localhost
	}
	err = internal.CheckCerts(certFile, keyFile)
	var certMsg string
	if err != nil {
		// if not, generate our own
		err = internal.GenerateCerts(caFile, certFile, keyFile, certDomain)
		if err != nil {
			serverLogger.Error("GenerateCerts failed", "err", err)
			return s, msg, token, err
		}
		certMsg = "created a new key and certificate for TLS"
	}

	// we need to persist stuff to disk, and we do so using boltdb
	db, msg, err := initDB(config.DBFile, config.DBFileBackup, config.Deployment, serverLogger)
	if certMsg != "" {
		if msg == "" {
			msg = certMsg
		} else {
			msg = certMsg + ". " + msg
		}
	}
	if err != nil {
		return s, msg, token, err
	}
	defer func() {
		if err != nil {
			errc := db.close()
			if errc != nil {
				err = fmt.Errorf("%s; db close also failed: %s", err.Error(), errc.Error())
			}
		}
	}()

	sock, err := rep.NewSocket()
	if err != nil {
		return s, msg, token, err
	}
	defer func() {
		if err != nil {
			errc := sock.Close()
			if errc != nil {
				err = fmt.Errorf("%s; socket close also failed: %s", err.Error(), errc.Error())
			}
		}
	}()

	// we open ourselves up to possible denial-of-service attack if a client
	// sends us tons of data, but at least the client doesn't silently hang
	// forever when it legitimately wants to Add() a ton of jobs
	// unlimited Recv() length
	if err = sock.SetOption(mangos.OptionMaxRecvSize, 0); err != nil {
		return s, msg, token, err
	}

	// we use raw mode, allowing us to respond to multiple clients in
	// parallel
	if err = sock.SetOption(mangos.OptionRaw, true); err != nil {
		return s, msg, token, err
	}

	// we'll wait ServerInterruptTime to recv from clients before trying again,
	// allowing us to check if signals have been passed
	if err = sock.SetOption(mangos.OptionRecvDeadline, ServerInterruptTime); err != nil {
		return s, msg, token, err
	}

	// check certificate expiry, because everything breaks with generic errors
	// when it expires
	expiry, err := internal.CertExpiry(caFile)
	if err != nil {
		return s, msg, token, err
	}
	if time.Now().After(expiry) {
		return s, msg, token, internal.CertError{Type: internal.ErrExpiredCert, Path: caFile}
	}
	expiry2, err := internal.CertExpiry(certFile)
	if err != nil {
		return s, msg, token, err
	}
	if time.Now().After(expiry2) {
		return s, msg, token, internal.CertError{Type: internal.ErrExpiredCert, Path: certFile}
	}
	if expiry2.Before(expiry) {
		expiry = expiry2
	}

	// have mangos listen using TLS over TCP
	sock.AddTransport(tlstcp.NewTransport())
	cer, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return s, msg, token, err
	}
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}
	listenOpts := make(map[string]interface{})
	caCert, err := os.ReadFile(caFile)
	if err == nil {
		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)
		tlsConfig.RootCAs = certPool
	}
	listenOpts[mangos.OptionTLSConfig] = tlsConfig
	if err = sock.ListenOptions("tls+tcp://0.0.0.0:"+config.Port, listenOpts); err != nil {
		return s, msg, token, err
	}

	// serving will happen in a goroutine that will stop on SIGINT or SIGTERM,
	// or if something is sent on the stopSigHandling channel. The done channel
	// is used to report back to a user that called Block() when and why we
	// stopped serving. stopClientHandling is used to stop client handling at
	// the right moment during the shutdown process. To know when all the
	// goroutines we start actually finish, the shutdown process will check a
	// waitgroup as well.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	stopSigHandling := make(chan bool, 1)
	stopClientHandling := make(chan bool)
	done := make(chan error, 1)
	wg := waitgroup.New()

	// if we end up spawning clients on other machines, they'll need to know
	// our non-loopback ip address so they can connect to us
	var ip string
	if config.DomainMatchesIP {
		ip = config.CertDomain
	} else {
		ip, err = internal.CurrentIP(config.CIDR)
		if err != nil {
			serverLogger.Error("getting current IP failed", "err", err)
		}
		if ip == "" {
			return s, msg, token, Error{"Serve", "", ErrNoHost}
		}
	}

	// we will spawn runner clients via the requested job scheduler
	sch, err := scheduler.New(config.SchedulerName, config.SchedulerConfig, serverLogger)
	if err != nil {
		return s, msg, token, err
	}

	uploadDir := config.UploadDir
	if uploadDir == "" {
		uploadDir = "/tmp"
	}

	// our limiter will use a callback that gets group limits from our database
	l := limiter.New(db.retrieveLimitGroup)

	s = &Server{
		ServerInfo:                &ServerInfo{Addr: ip + ":" + config.Port, Host: certDomain, Port: config.Port, WebPort: config.WebPort, PID: os.Getpid(), Deployment: config.Deployment, Scheduler: config.SchedulerName, Mode: ServerModeNormal},
		ServerVersions:            &ServerVersions{Version: ServerVersion, API: restAPIVersion},
		token:                     token,
		uploadDir:                 uploadDir,
		sock:                      sock,
		ch:                        new(codec.BincHandle),
		rpl:                       &rgToKeys{lookup: make(map[string]map[string]bool)},
		limiter:                   l,
		db:                        db,
		stopSigHandling:           stopSigHandling,
		stopClientHandling:        stopClientHandling,
		done:                      done,
		wg:                        wg,
		up:                        true,
		scheduler:                 sch,
		previouslyScheduledGroups: make(map[string]*sgroup),
		rc:                        config.RunnerCmd,
		wsconns:                   make(map[string]*websocket.Conn),
		statusCaster:              bcast.NewGroup(),
		badServerCaster:           bcast.NewGroup(),
		badServers:                make(map[string]*cloud.Server),
		schedCaster:               bcast.NewGroup(),
		schedIssues:               make(map[string]*schedulerIssue),
		recoveredRunningJobs:      make(map[string]bool),
		Logger:                    serverLogger,
	}

	// if we're restarting from a state where there were incomplete jobs, we
	// need to load those in to our queue now
	s.createQueue()
	priorJobs, err := db.recoverIncompleteJobs()
	if err != nil {
		return nil, msg, token, err
	}
	if len(priorJobs) > 0 {
		var loginUser string
		var ttd time.Duration
		if cloudConfig, ok := config.SchedulerConfig.(scheduler.CloudConfig); ok {
			// *** for server recovery purposes, which involves ssh'ing to
			// existing servers and monitoring them, we need to know the login
			// username, but we don't. The best we can do is hope the configured
			// default username is the right one
			loginUser = cloudConfig.GetOSUser()
			ttd = cloudConfig.GetServerKeepTime()
		}

		var itemdefs []*queue.ItemDef
		for _, job := range priorJobs {
			var deps []string
			deps, err = job.Dependencies.incompleteJobKeys(s.db)
			if err != nil {
				return nil, msg, token, err
			}

			itemdef := &queue.ItemDef{Key: job.Key(), ReserveGroup: job.getSchedulerGroup(), Data: job, Priority: job.Priority, Delay: 0 * time.Second, TTR: ServerItemTTR, Dependencies: deps}

			switch job.State {
			case JobStateRunning:
				itemdef.StartQueue = queue.SubQueueRun

				if len(job.LimitGroups) > 0 {
					if s.limiter.Increment(job.LimitGroups) {
						// (our note of incrementation done in the server that died
						//  is not stored in the db)
						job.noteIncrementedLimitGroups(job.LimitGroups)
					}
				}

				req := reqForScheduler(job.Requirements)
				errr := s.scheduler.Recover(fmt.Sprintf(s.rc, req.Stringify(), s.ServerInfo.Deployment, s.ServerInfo.Addr, s.ServerInfo.Host, s.scheduler.ReserveTimeout(req), int(s.scheduler.MaxQueueTime(req).Minutes())), req, &scheduler.RecoveredHostDetails{Host: job.Host, UserName: loginUser, TTD: ttd})
				if errr != nil {
					s.Warn("recovery of an old cmd failed", "cmd", job.Cmd, "host", job.Host, "err", errr)
				}
				s.recoveredRunningJobs[job.Key()] = true
			case JobStateBuried:
				itemdef.StartQueue = queue.SubQueueBury
			}

			itemdefs = append(itemdefs, itemdef)
		}
		_, _, err = s.enqueueItems(itemdefs)
		if err != nil {
			return nil, msg, token, err
		}
	}

	// wait for signal or s.Stop() and call s.shutdown(). (We don't use the
	// waitgroup here since we call shutdown, which waits on the group)
	certExpired := time.After(time.Until(expiry))
	go func() {
		// log panics and die
		defer internal.LogPanic(s.Logger, "jobqueue serving", true)

		for {
			select {
			case sig := <-sigs:
				var reason string
				switch sig {
				case os.Interrupt:
					reason = ErrClosedInt
				case syscall.SIGTERM:
					reason = ErrClosedTerm
				}
				signal.Stop(sigs)
				s.shutdown(reason, true, false)
				return
			case <-certExpired:
				signal.Stop(sigs)
				s.shutdown(ErrClosedCert, true, false)
			case <-stopSigHandling: // s.Stop() causes this to be sent during s.shutdown(), which it calls
				signal.Stop(sigs)
				return
			}
		}
	}()

	// set up the web interface
	ready := make(chan bool)
	wgk := wg.Add(1)
	go func() {
		// log panics and die
		defer internal.LogPanic(s.Logger, "jobqueue web server", true)
		defer wg.Done(wgk)

		mux := http.NewServeMux()
		mux.HandleFunc("/", webInterfaceStatic(s))
		mux.HandleFunc("/status_ws", webInterfaceStatusWS(s))
		mux.HandleFunc(restJobsEndpoint, restJobs(s))
		mux.HandleFunc(restWarningsEndpoint, restWarnings(s))
		mux.HandleFunc(restBadServersEndpoint, restBadServers(s))
		mux.HandleFunc(restFileUploadEndpoint, restFileUpload(s))
		mux.HandleFunc(restInfoEndpoint, restInfo(s))
		mux.HandleFunc(restVersionEndpoint, restVersion(s))
		srv := &http.Server{Addr: httpAddr, Handler: mux}
		wgk2 := wg.Add(1)
		go func() {
			defer internal.LogPanic(s.Logger, "jobqueue web server listenAndServe", true)
			defer wg.Done(wgk2)
			errs := srv.ListenAndServeTLS(certFile, keyFile)
			if errs != nil && errs != http.ErrServerClosed {
				s.Error("server web interface had problems", "err", errs)
			}
		}()
		s.httpServer = srv

		wgk3 := wg.Add(1)
		go func() {
			defer internal.LogPanic(s.Logger, "jobqueue web server status casting", true)
			defer wg.Done(wgk3)
			s.statusCaster.Broadcasting(0)
		}()
		wgk4 := wg.Add(1)
		go func() {
			defer internal.LogPanic(s.Logger, "jobqueue web server server casting", true)
			defer wg.Done(wgk4)
			s.badServerCaster.Broadcasting(0)
		}()
		wgk5 := wg.Add(1)
		go func() {
			defer internal.LogPanic(s.Logger, "jobqueue web server scheduler casting", true)
			defer wg.Done(wgk5)
			s.schedCaster.Broadcasting(0)
		}()

		badServerCB := func(server *cloud.Server) {
			s.bsmutex.Lock()
			skip := false
			if server.IsBad() {
				// double check that due to timing issues this server hasn't
				// been destroyed, which is not something to warn anyone about
				if server.Destroyed() {
					skip = true
				} else {
					s.badServers[server.ID] = server

					// arrange to confirm this dead after the configured time
					if config.AutoConfirmDead > 0 {
						go func(id string) {
							<-time.After(config.AutoConfirmDead)
							s.bsmutex.Lock()
							defer s.bsmutex.Unlock()
							if badServer, exists := s.badServers[id]; exists && badServer.BadDuration() >= config.AutoConfirmDead {
								delete(s.badServers, id)
								waited := badServer.BadDuration()
								errd := badServer.Destroy()
								s.Warn("server destroyed after remaining bad for some time", "server", id, "waited", waited, "err", errd)
								serverIDs := make(map[string]bool)
								serverIDs[id] = true
								s.killJobsOnServers(serverIDs)

								if errd == nil {
									// make the message in the web interface
									// about this server go away
									s.badServerCaster.Send(&BadServer{
										ID:      id,
										Name:    badServer.Name,
										IP:      badServer.IP,
										Date:    time.Now().Unix(),
										IsBad:   false,
										Problem: badServer.PermanentProblem(),
									})
								}
							}
						}(server.ID)
					}
				}
			} else {
				delete(s.badServers, server.ID)
			}
			s.bsmutex.Unlock()

			if !skip {
				s.badServerCaster.Send(&BadServer{
					ID:      server.ID,
					Name:    server.Name,
					IP:      server.IP,
					Date:    time.Now().Unix(),
					IsBad:   server.IsBad(),
					Problem: server.PermanentProblem(),
				})
			}
		}
		s.scheduler.SetBadServerCallBack(badServerCB)

		messageCB := func(msg string) {
			s.simutex.Lock()
			var si *schedulerIssue
			var existed bool
			if si, existed = s.schedIssues[msg]; existed {
				si.LastDate = time.Now().Unix()
				si.Count++
			} else {
				si = &schedulerIssue{
					Msg:       msg,
					FirstDate: time.Now().Unix(),
					LastDate:  time.Now().Unix(),
					Count:     1,
				}
				s.schedIssues[msg] = si
			}
			s.simutex.Unlock()
			s.schedCaster.Send(si)
		}
		s.scheduler.SetMessageCallBack(messageCB)

		// wait a while for ListenAndServe() to start listening
		<-time.After(10 * time.Millisecond)
		ready <- true
	}()
	<-ready

	// store token on disk
	if config.TokenFile != "" {
		err = os.WriteFile(config.TokenFile, token, 0600)
		if err != nil {
			return s, msg, token, err
		}
	}

	// now that we're ready, set up responding to command-line clients
	wgk = wg.Add(1)
	go func() {
		// log panics and die
		defer internal.LogPanic(s.Logger, "jobqueue serving", true)
		defer wg.Done(wgk)

		for {
			select {
			case <-stopClientHandling: // s.shutdown() sends this
				return
			default:
				// receive a clientRequest from a client
				m, rerr := sock.RecvMsg()
				if rerr != nil {
					s.krmutex.RLock()
					inShutdown := s.killRunners
					s.krmutex.RUnlock()
					if !inShutdown && rerr != mangos.ErrRecvTimeout {
						s.Error("Server socket Receive error", "err", rerr)
					}
					continue
				}

				// parse the request, do the desired work and respond to the client
				wgk2 := wg.Add(1)
				go func() {
					// log panics and continue
					defer internal.LogPanic(s.Logger, "jobqueue server client handling", false)
					defer wg.Done(wgk2)

					herr := s.handleRequest(m)
					if ServerLogClientErrors && herr != nil {
						s.krmutex.RLock()
						inShutdown := s.killRunners
						s.krmutex.RUnlock()
						if !inShutdown {
							s.Error("Server handle client request error", "err", herr)
						}
					}
				}()
			}
		}
	}()

	return s, msg, token, err
}

// Block makes you block while the server does the job of serving clients. This
// will return with an error indicating why it stopped blocking, which will
// be due to receiving a signal or because you called Stop()
func (s *Server) Block() error {
	s.ssmutex.Lock()
	s.blocking = true
	s.ssmutex.Unlock()
	return <-s.done
}

// Stop will cause a graceful shut down of the server. Supplying an optional
// bool of true will cause Stop() to wait until all runners have exited and
// the server is truly down before returning.
func (s *Server) Stop(wait ...bool) {
	s.shutdown(ErrClosedStop, len(wait) == 1 && wait[0], true)
}

// Drain will stop the server spawning new runners and stop Reserve*() from
// returning any more Jobs. Once all current runners exit, we Stop().
func (s *Server) Drain() error {
	s.ssmutex.Lock()
	defer s.ssmutex.Unlock()
	if !s.up {
		return Error{"Drain", "", ErrNoServer}
	}
	if s.drain && s.ServerInfo.Mode == ServerModeDrain {
		return nil
	}

	s.drain = true
	s.ServerInfo.Mode = ServerModeDrain
	go func() {
		defer internal.LogPanic(s.Logger, "jobqueue drain", true)

		ticker := time.NewTicker(1 * time.Second)
	TICKS:
		for range ticker.C {
			// check our queue for things running, which is cheap
			stats := s.q.Stats()
			if stats.Running > 0 {
				continue TICKS
			}
			ticker.Stop()

			// now that we think nothing should be running, get
			// Stop() to wait for the runner clients to exit so the
			// job scheduler will be nice and clean
			s.Stop(true)
			break
		}
	}()
	return nil
}

// Pause is like Drain(), except that we don't Stop(). Returns true if we were
// not already paused.
func (s *Server) Pause() (bool, error) {
	s.ssmutex.Lock()
	defer s.ssmutex.Unlock()
	if !s.up {
		return false, Error{"Pause", "", ErrNoServer}
	}
	if s.drain {
		if s.ServerInfo.Mode == ServerModeDrain {
			return false, Error{"Pause", "", ErrBeingDrained}
		}
	}
	s.drain = true
	s.ServerInfo.Mode = ServerModePause
	s.pauseRequests++
	return s.pauseRequests == 1, nil
}

// Resume undoes Pause(). Does not return an error if we were not paused.
// If multiple pauses have been requested at once, actually does nothing until
// the number of resume requests matches the number of pauses.
// Returns true if actually resumed.
func (s *Server) Resume() (bool, error) {
	s.ssmutex.Lock()
	defer s.ssmutex.Unlock()
	if !s.up {
		return false, Error{"Resume", "", ErrNoServer}
	}
	if !s.drain {
		return false, nil
	}
	if s.ServerInfo.Mode == ServerModeDrain {
		return false, Error{"Resume", "", ErrBeingDrained}
	}
	s.pauseRequests--
	if s.pauseRequests > 0 {
		return false, nil
	} else if s.pauseRequests < 0 {
		s.pauseRequests = 0
	}
	s.drain = false
	s.ServerInfo.Mode = ServerModeNormal
	s.q.TriggerReadyAddedCallback()
	return true, nil
}

// GetServerStats returns some simple live stats about what's happening in the
// server's queue.
func (s *Server) GetServerStats() *ServerStats {
	var delayed, ready, running, buried int
	now := time.Now()
	etc := now

	stats := s.q.Stats()
	delayed += stats.Delayed
	ready += stats.Ready
	buried += stats.Buried

	for _, inter := range s.q.GetRunningData() {
		running++

		// work out when this Job is going to end, and update etc if later
		job := inter.(*Job)
		job.RLock()
		if !job.StartTime.IsZero() && job.Requirements.Time.Seconds() > 0 {
			endTime := job.StartTime.Add(job.Requirements.Time)
			if endTime.After(etc) {
				etc = endTime
			}
		}
		job.RUnlock()
	}

	return &ServerStats{Delayed: delayed, Ready: ready, Running: running, Buried: buried, ETC: etc.Truncate(time.Second).Sub(now.Truncate(time.Second))}
}

// BackupDB lets you do a manual live backup of the server's database to a given
// writer. Note that automatic backups occur to the configured location
// without calling this.
func (s *Server) BackupDB(w io.Writer) error {
	return s.db.backup(w)
}

// HasRunners tells you if there are currently runner clients in the job
// scheduler (either running or pending).
func (s *Server) HasRunners() bool {
	return s.scheduler.Busy()
}

// uploadFile uploads the given file data to the given path on the machine where
// the server process is running.
//
// If savePath is an empty string, the file is stored at a path based on the MD5
// checksum of the file data, rooted in the server's configured UploadDir. If it
// turns out such a file already exists, no error is generated. savePath can be
// prefixed with ~/ to have it saved relative to the server's home directory.
//
// Files stored will only be readable by the user that started the server.
//
// Note that this is only intended for a a few small files, such as config files
// that need to be passed through to spawned cloud servers, when doing a cloud
// deployment.
//
// Returns the absolute path to the file that now contains the given file data.
func (s *Server) uploadFile(source io.Reader, savePath string) (string, error) {
	var file *os.File
	var err error
	usedTempFile := false
	if savePath == "" {
		if _, err = os.Stat(s.uploadDir); err != nil && os.IsNotExist(err) {
			err = os.MkdirAll(s.uploadDir, os.ModePerm)
			if err != nil {
				s.Error("uploadFile create directory error", "err", err)
				return "", err
			}
		}
		file, err = os.CreateTemp(s.uploadDir, "file_upload")
		if err != nil {
			s.Error("uploadFile temp file create error", "err", err)
			return "", err
		}
		savePath = file.Name()
		usedTempFile = true
	} else {
		savePath = internal.TildaToHome(savePath)
		err = os.MkdirAll(filepath.Dir(savePath), os.ModePerm)
		if err != nil {
			s.Error("uploadFile create directory error", "err", err)
			return "", err
		}
		file, err = os.OpenFile(savePath, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			s.Error("uploadFile create file error", "err", err)
			return "", err
		}
	}

	_, err = io.Copy(file, source)
	if err != nil {
		s.Error("uploadFile store file error", "err", err)
		return "", err
	}
	err = file.Close()
	if err != nil {
		s.Warn("uploadFile close file error", "err", err)
	}

	if usedTempFile {
		// rename the file to one based on the md5 checksum of the file
		var md5 string
		md5, err = internal.FileMD5(savePath, s.Logger)
		if err != nil {
			s.Error("uploadFile md5 calculation error", "err", err)
			return "", err
		}

		dir, leaf := calculateHashedDir(s.uploadDir, md5)
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			s.Error("uploadFile create directory error", "err", err)
			return "", err
		}

		finalPath := path.Join(dir, leaf)
		_, err = os.Stat(finalPath)
		if err != nil {
			if os.IsNotExist(err) {
				err = os.Rename(savePath, finalPath)
				if err != nil {
					s.Error("uploadFile rename file error", "err", err)
					return "", err
				}
			} else {
				s.Error("uploadFile stat file error", "err", err)
				return "", err
			}
		} else {
			// already exists, delete the temp file
			err = os.Remove(savePath)
			if err != nil {
				s.Warn("uploadFile file removal error", "err", err)
			}
		}
		savePath = finalPath
	}

	return savePath, nil
}

// createQueue creates and stores a queue.Queue on the Server and sets up its
// callbacks.
func (s *Server) createQueue() {
	q := queue.New("cmds", s.Logger)
	s.q = q

	// we set a callback for things entering this queue's ready sub-queue.
	// This function will be called in a go routine and receives a slice of
	// all the ready jobs. Based on the requirements, we add to each job a
	// schedulerGroup, which the runners we spawn will be able to pass to
	// Reserve() so that they run the correct jobs for the machine and
	// resource reservations the job scheduler will run them under. queue
	// package will only call this once at a time, so we don't need to worry
	// about locking across the whole function.
	q.SetReadyAddedCallback(func(queuename string, allitemdata []interface{}) {
		defer internal.LogPanic(s.Logger, "jobqueue ready added callback", true)

		s.Debug("rac started")
		defer s.Debug("rac finished")

		s.ssmutex.RLock()
		if s.drain || !s.up {
			s.ssmutex.RUnlock()
			return
		}
		s.ssmutex.RUnlock()

		s.rpmutex.Lock()
		s.racRunning = true
		s.rpmutex.Unlock()
		defer func() {
			s.rpmutex.Lock()
			s.racPending = false
			s.racRunning = false
			for _, ch := range s.waitingReserves {
				close(ch)
			}
			s.waitingReserves = nil
			s.rpmutex.Unlock()
		}()

		s.racmutex.RLock()
		rc := s.rc
		s.racmutex.RUnlock()

		// calculate, set and count jobs by schedulerGroup
		groups := make(map[string]*sgroup)
		reqGroupToReqs := make(map[string]*scheduler.Requirements)
		groupLimits := make(map[string]int)
		for _, inter := range allitemdata {
			job := inter.(*Job)

			// depending on job.Override, get memory, disk and time
			// recommendations, which are rounded to get fewer larger
			// groups
			var recommendedReq *scheduler.Requirements
			if rec, existed := reqGroupToReqs[job.ReqGroup]; existed {
				recommendedReq = rec
			} else {
				recm, errm := s.db.recommendedReqGroupMemory(job.ReqGroup)
				recd, errd := s.db.recommendedReqGroupDisk(job.ReqGroup)
				recs, errs := s.db.recommendedReqGroupTime(job.ReqGroup)
				if errm != nil || errd != nil || errs != nil {
					reqGroupToReqs[job.ReqGroup] = nil
				} else {
					recmMBs := 0
					if recm > 0 {
						recmMBs = recm
					}
					recdGBs := 0
					if recd > 0 {
						recdGBs = int(math.Ceil(float64(recd) / float64(1024)))
					}
					recsSecs := 0
					if recs > 0 {
						recsSecs = recs
					}
					recommendedReq = &scheduler.Requirements{RAM: recmMBs, Disk: recdGBs, DiskSet: true, Time: time.Duration(recsSecs) * time.Second}
					reqGroupToReqs[job.ReqGroup] = recommendedReq
				}
			}

			if recommendedReq != nil || job.FailReason == FailReasonRAM || job.FailReason == FailReasonDisk || job.FailReason == FailReasonTime {
				job.Lock()
				if job.RequirementsOrig == nil {
					job.RequirementsOrig = &scheduler.Requirements{
						RAM:     job.Requirements.RAM,
						Time:    job.Requirements.Time,
						Disk:    job.Requirements.Disk,
						DiskSet: job.Requirements.DiskSet,
					}
				}

				if recommendedReq.RAM > 0 {
					if job.RequirementsOrig.RAM > 0 {
						switch job.Override {
						case 0:
							job.Requirements.RAM = recommendedReq.RAM
						case 1:
							if recommendedReq.RAM > job.Requirements.RAM {
								job.Requirements.RAM = recommendedReq.RAM
							}
						}
					} else {
						job.Requirements.RAM = recommendedReq.RAM
					}
				}

				if recommendedReq.Disk > 0 {
					if job.RequirementsOrig.Disk > 0 || job.RequirementsOrig.DiskSet {
						switch job.Override {
						case 0:
							job.Requirements.Disk = recommendedReq.Disk
						case 1:
							if recommendedReq.Disk > job.Requirements.Disk {
								job.Requirements.Disk = recommendedReq.Disk
							}
						}
					} else {
						job.Requirements.Disk = recommendedReq.Disk
					}
				}

				if recommendedReq.Time.Seconds() > 0 {
					if job.RequirementsOrig.Time > 0 {
						switch job.Override {
						case 0:
							job.Requirements.Time = recommendedReq.Time
						case 1:
							if recommendedReq.Time > job.Requirements.Time {
								job.Requirements.Time = recommendedReq.Time
							}
						}
					} else {
						job.Requirements.Time = recommendedReq.Time
					}
				}

				switch job.FailReason {
				case FailReasonRAM:
					// increase by 1GB or [100% if under 8GB, 30% if over],
					// whichever is greater, and round up to nearest 100 ***
					// increase to greater than max seen for jobs in our
					// ReqGroup?
					updatedMB := float64(job.PeakRAM)
					if updatedMB <= RAMIncreaseMultBreakpoint {
						updatedMB *= RAMIncreaseMultLow
					} else {
						updatedMB *= RAMIncreaseMultHigh
					}
					if updatedMB < float64(job.PeakRAM)+RAMIncreaseMin {
						updatedMB = float64(job.PeakRAM) + RAMIncreaseMin
					}
					newRAM := int(math.Ceil(updatedMB/100) * 100)
					if newRAM > job.Requirements.RAM {
						job.Requirements.RAM = newRAM
					}
				case FailReasonDisk:
					// flat increase of 30%
					updatedMB := float64(job.PeakDisk) / float64(1024)
					updatedMB *= RAMIncreaseMultHigh
					newDisk := int(math.Ceil(updatedMB/100) * 100)
					if newDisk > job.Requirements.Disk {
						job.Requirements.Disk = newDisk
					}
				case FailReasonTime:
					// flat increase of 1 hour
					newTime := job.EndTime.Sub(job.StartTime) + (1 * time.Hour)
					if newTime > job.Requirements.Time {
						job.Requirements.Time = newTime
					}
				}

				job.Unlock()
			}

			req := reqForScheduler(job.Requirements)

			prevSchedGroup := job.getSchedulerGroup()
			schedulerGroup := job.generateSchedulerGroup(req)
			if rc != "" && prevSchedGroup != schedulerGroup {
				job.setSchedulerGroup(schedulerGroup)
				errs := q.SetReserveGroup(job.Key(), schedulerGroup)
				if errs != nil {
					// we could be trying to set the reserve group after the
					// job has already completed, if they complete
					// ~instantly
					if qerr, ok := errs.(queue.Error); !ok || qerr.Err != queue.ErrNotFound {
						s.Warn("readycallback queue setreservegroup failed", "err", errs)
					}
				}
			}

			if rc != "" {
				group, set := groups[schedulerGroup]
				if !set {
					group = &sgroup{
						name: schedulerGroup,
						req:  req.Clone(),
					}
					groups[schedulerGroup] = group
				}

				// ignore jobs that would put us over the limit
				limit, set := groupLimits[schedulerGroup]
				if !set {
					limitGroups := s.schedGroupToLimitGroups(schedulerGroup)
					if len(limitGroups) > 0 {
						groupLimits[schedulerGroup] = s.limiter.GetRemainingCapacity(limitGroups)
					} else {
						groupLimits[schedulerGroup] = -1
					}
					limit = groupLimits[schedulerGroup]
				}
				if limit >= 0 && group.count == limit {
					group.skipped++
					continue
				}

				group.count++

				if job.Priority > group.priority {
					group.priority = job.Priority
				}
			}
		}

		if rc != "" {
			for name, group := range groups {
				s.Debug("rac saw ready jobs", "group", name, "count", group.count, "limitskipped", group.skipped)
			}

			// add in info for running jobs
			s.psgmutex.Lock()
			for _, inter := range q.GetRunningData() {
				job := inter.(*Job)
				schedulerGroup := job.getSchedulerGroup()
				group, set := groups[schedulerGroup]
				if !set {
					group, set = s.previouslyScheduledGroups[schedulerGroup]
					if set {
						group = group.clone(0)
					} else {
						// this can happen if a newly added job is reserved the moment it
						// is added, so it becomes running before being processed by this
						// rac
						job.Lock()
						group = &sgroup{
							name: schedulerGroup,
							req:  job.Requirements.Clone(),
						}
						job.Unlock()
					}
					groups[schedulerGroup] = group
				}
				group.count++
			}

			// unschedule groups we no longer need
			for name, group := range s.previouslyScheduledGroups {
				if _, needed := groups[name]; needed {
					continue
				}

				wgk := s.wg.Add(1)
				go func(group *sgroup) {
					defer internal.LogPanic(s.Logger, "jobqueue unschedule runners", true)
					defer s.wg.Done(wgk)
					s.Debug("rac unscheduling uneeded group", "group", group.name)
					s.scheduleRunners(group)
				}(group.clone(0))
				delete(s.previouslyScheduledGroups, name)
				s.Debug("rac deleted previous unneeded group", "group", name)
			}

			// schedule runners for each group in the job scheduler
			for name, group := range groups {
				if group.count <= 0 {
					s.Debug("rac scheduling no jobs", "group", name, "count", group.count, "limitskipped", group.skipped)
				} else {
					s.Debug("rac scheduling jobs", "group", name, "count", group.count, "limitskipped", group.skipped)
				}

				wgk := s.wg.Add(1)
				group.Lock()
				go func(group *sgroup) {
					defer internal.LogPanic(s.Logger, "jobqueue schedule runners", true)
					defer s.wg.Done(wgk)
					s.scheduleRunners(group)
					group.Unlock()
				}(group)

				s.previouslyScheduledGroups[name] = group
			}
			s.psgmutex.Unlock()

			// in the event that the runners we spawn can't reach us temporarily
			// and just die (or they manage to run and exit due to a limit), we
			// need to make sure this callback gets triggered again even if no
			// new jobs get added
			s.racmutex.Lock()
			defer s.racmutex.Unlock()
			if s.racChecking {
				if !s.racCheckTimer.Stop() {
					<-s.racCheckTimer.C
				}
				s.racCheckTimer.Reset(ServerCheckRunnerTime)
			} else {
				s.racCheckTimer = time.NewTimer(ServerCheckRunnerTime)

				wgk := s.wg.Add(1)
				go func() {
					defer internal.LogPanic(s.Logger, "jobqueue rac checking", true)
					defer s.wg.Done(wgk)

					select {
					case <-s.racCheckTimer.C:
						break
					case <-s.stopClientHandling:
						return
					}

					s.racmutex.Lock()
					s.racChecking = false
					stats := q.Stats()

					if stats.Ready > 0 {
						s.racmutex.Unlock()
						q.TriggerReadyAddedCallback()
					} else {
						s.racmutex.Unlock()
					}
				}()

				s.racChecking = true
			}
		}
	})

	// we set a callback for things changing in the queue, which lets us
	// update the status webpage with the minimal work and data transfer
	q.SetChangedCallback(func(fromQ, toQ queue.SubQueue, data []interface{}) {
		if toQ != queue.SubQueueReady {
			// readyAddedCallback won't be called, cancel racPending
			defer func() {
				s.rpmutex.Lock()
				if s.racPending {
					s.racPending = false
					for _, ch := range s.waitingReserves {
						close(ch)
					}
					s.waitingReserves = nil
				}
				s.rpmutex.Unlock()
			}()
		}

		var from, to JobState
		if toQ == queue.SubQueueRemoved {
			// things are removed from the queue if deleted or completed;
			// disambiguate
			to = JobStateDeleted
			for _, inter := range data {
				job := inter.(*Job)
				job.RLock()
				jState := job.State
				job.RUnlock()
				if jState == JobStateComplete {
					to = JobStateComplete
					break
				}
			}
		} else {
			to = subqueueToJobState[toQ]
		}
		from = subqueueToJobState[fromQ]

		// calculate counts per RepGroup
		groups := make(map[string]int)
		groupsLost := make(map[string]int)
		lost := 0
		for _, inter := range data {
			job := inter.(*Job)

			// track lost jobs
			if from == JobStateRunning {
				job.RLock()
				l := job.Lost
				job.RUnlock()
				if l {
					lost++
					groupsLost[job.RepGroup]++
					continue
				}
			}

			groups[job.RepGroup]++
		}

		// send out the counts
		s.statusCaster.Send(&jstateCount{"+all+", from, to, len(data) - lost})
		for group, count := range groups {
			s.statusCaster.Send(&jstateCount{group, from, to, count})
		}

		if lost > 0 {
			s.statusCaster.Send(&jstateCount{"+all+", JobStateLost, to, lost})
			for group, count := range groupsLost {
				s.statusCaster.Send(&jstateCount{group, JobStateLost, to, count})
			}
		}
	})

	// we set a callback for running items that hit their ttr because the
	// runner died or because of networking issues: we keep them in the
	// running queue, but mark them up as having possibly failed, leaving it
	// up the user if they want to confirm the jobs are dead by killing
	// them or leaving them to spring back to life if not. If they already
	// killed it, however, we'll do normal releasing behaviour afterwards.
	q.SetTTRCallback(func(data interface{}) queue.SubQueue {
		job := data.(*Job)

		job.Lock()
		if !job.StartTime.IsZero() && !job.Exited {
			job.Lost = true
			job.FailReason = FailReasonLost
			job.EndTime = time.Now()

			// we don't test recovered jobs are dead because they might have
			// exited while the server wasn't running, and we want the existing
			// client to tell us if it should be archived or buried
			if !job.killCalled && !s.recoveredRunningJobs[job.Key()] && s.confirmJobDeadAndKill(job) {
				s.Info("killed a job after confirming it was dead", "key", job.Key())
			} else if job.killCalled {
				defer func() {
					go func() {
						defer internal.LogPanic(s.Logger, "jobqueue ttr callback releaseJob", true)

						// wait for the item to go back to run queue
						<-time.After(50 * time.Millisecond)

						// now release it
						err := s.releaseJob(job, &JobEndState{Exitcode: -1, Exited: true}, FailReasonLost, false, false)
						if err != nil {
							s.Warn("failed to release job after TTR", "err", err)
						}
					}()
				}()
			}

			// since our changed callback won't be called, send out this
			// transition from running to lost state
			defer s.statusCaster.Send(&jstateCount{"+all+", JobStateRunning, JobStateLost, 1})
			defer s.statusCaster.Send(&jstateCount{job.RepGroup, JobStateRunning, JobStateLost, 1})

			job.Unlock()
			return queue.SubQueueRun
		}

		job.Unlock()
		job.decrementLimitGroups(s.limiter)
		return queue.SubQueueDelay
	})
}

// enqueueItems adds new items to a queue, for when we have new jobs to handle.
func (s *Server) enqueueItems(itemdefs []*queue.ItemDef) (added, dups int, err error) {
	s.rpmutex.Lock()
	s.racPending = true
	s.rpmutex.Unlock()
	added, dups, err = s.q.AddMany(itemdefs)
	if err != nil {
		s.rpmutex.Lock()
		s.racPending = false
		s.rpmutex.Unlock()
		return added, dups, err
	}

	// add to our lookup of job RepGroup to key
	s.rpl.Lock()
	for _, itemdef := range itemdefs {
		rp := itemdef.Data.(*Job).RepGroup
		if _, exists := s.rpl.lookup[rp]; !exists {
			s.rpl.lookup[rp] = make(map[string]bool)
		}
		s.rpl.lookup[rp][itemdef.Key] = true
	}
	s.rpl.Unlock()

	return added, dups, err
}

// createJobs creates new jobs, adding them to the database and the in-memory
// queue. It returns 2 errors; the first is one of our Err constant strings,
// the second is the actual error with more details.
func (s *Server) createJobs(inputJobs []*Job, envkey string, ignoreComplete bool) (added, dups, alreadyComplete int, srerr string, qerr error) {
	s.racmutex.RLock()
	rcSet := s.rc != ""
	s.racmutex.RUnlock()

	// create itemdefs for the jobs
	limitGroups := make(map[string]int)
	for _, job := range inputJobs {
		job.Lock()
		job.EnvKey = envkey
		job.UntilBuried = job.Retries + 1
		if rcSet {
			job.schedulerGroup = job.generateSchedulerGroup(job.Requirements)
		}
		if job.BsubMode != "" {
			job.BsubID = atomic.AddUint64(&BsubID, 1)
		}

		if len(job.LimitGroups) > 0 {
			err := s.handleUserSpecifiedJobLimitGroups(job, limitGroups)
			if err != nil {
				return added, dups, alreadyComplete, ErrBadLimitGroup, err
			}
		}

		job.Unlock()
	}

	err := s.storeLimitGroups(limitGroups)
	if err != nil {
		return added, dups, alreadyComplete, ErrDBError, err
	}

	// keep an on-disk record of these new jobs; we sacrifice a lot of speed by
	// waiting on this database write to persist to disk. The alternative would
	// be to return success to the client as soon as the jobs were in the in-
	// memory queue, then lazily persist to disk in a goroutine, but we must
	// guarantee that jobs are never lost or a workflow could hopelessly break
	// if the server node goes down between returning success and the write to
	// disk succeeding. (If we don't return success to the client, it won't
	// Remove the job that created the new jobs from the queue and when we
	// recover, at worst the creating job will be run again - no jobs get lost.)
	jobsToQueue, jobsToUpdate, alreadyComplete, err := s.db.storeNewJobs(inputJobs, ignoreComplete)
	if err != nil {
		srerr = ErrDBError
		qerr = err
	} else {
		// now that jobs are in the db we can get dependencies fully, so now we
		// can build our itemdefs *** we really need to test for cycles, because
		// if the user creates one, we won't let them delete the bad jobs!
		// storeNewJobs() returns jobsToQueue, which is all of cr.Jobs plus any
		// previously Archive()d jobs that were resurrected because of one of
		// their DepGroup dependencies being in cr.Jobs
		var itemdefs []*queue.ItemDef
		for _, job := range jobsToQueue {
			deps, err := job.Dependencies.incompleteJobKeys(s.db)
			if err != nil {
				srerr = ErrDBError
				qerr = err
				break
			}
			itemdefs = append(itemdefs, &queue.ItemDef{Key: job.Key(), ReserveGroup: job.getSchedulerGroup(), Data: job, Priority: job.Priority, Delay: 0 * time.Second, TTR: ServerItemTTR, Dependencies: deps})
		}

		srerr, qerr = s.updateJobDependencies(jobsToUpdate)

		if qerr != nil {
			srerr = ErrInternalError
		} else {
			// add the jobs to the in-memory job queue
			added, dups, qerr = s.enqueueItems(itemdefs)
			if qerr != nil {
				srerr = ErrInternalError
			}
		}
	}
	return added, dups, alreadyComplete, srerr, qerr
}

// handleUserSpecifiedJobLimitGroups takes limit groups on a job that may have
// been specified like name:limit, and fixes them to remove the limit suffix,
// dedup and sort the groups, and fill in your supplied limitGroups map with the
// latest limit on groups, if any were specified. You should hold the lock on
// the Job before calling this.
func (s *Server) handleUserSpecifiedJobLimitGroups(job *Job, limitGroups map[string]int) error {
	// remove limit suffixes and remember the last limit per group specified
	for i, group := range job.LimitGroups {
		name, limit, suffixed, err := s.splitSuffixedLimitGroup(group)
		if err != nil {
			return err
		}
		if suffixed {
			job.LimitGroups[i] = name
			limitGroups[name] = limit
		}
	}

	// because these later become part of scheduler groups names, store
	// them in sorted order, with no duplicates
	if len(job.LimitGroups) > 1 {
		job.LimitGroups = internal.DedupSortStrings(job.LimitGroups)
	}

	return nil
}

// storeLimitGroups calls db.storeLimitGroups() and handles updating the
// in-memory representation of the groups.
func (s *Server) storeLimitGroups(limitGroups map[string]int) error {
	changed, removed, err := s.db.storeLimitGroups(limitGroups)
	if err != nil {
		return err
	}
	for _, group := range changed {
		s.limiter.SetLimit(group, uint(limitGroups[group]))
	}
	for _, group := range removed {
		s.limiter.RemoveLimit(group)
	}
	return nil
}

// updateJobDependencies is used to handle the jobsToUpdate from storeNewJobs()
// and db.modifyLiveJobs(). These are those jobs currently in the queue that
// need their dependencies updated because they just changed when we stored the
// jobs.
func (s *Server) updateJobDependencies(jobs []*Job) (srerr string, qerr error) {
	s.rpmutex.Lock()
	s.racPending = true
	s.rpmutex.Unlock()
	for _, job := range jobs {
		deps, err := job.Dependencies.incompleteJobKeys(s.db)
		if err != nil {
			srerr = ErrDBError
			qerr = err
			break
		}
		thisErr := s.q.Update(job.Key(), job.getSchedulerGroup(), job, job.Priority, 0*time.Second, ServerItemTTR, deps)
		if thisErr != nil {
			qerr = thisErr
			break
		}
	}
	return srerr, qerr
}

// confirmJobDeadAndKill calls and returns the value of confirmJobDead(). If
// true, kills the job and triggers behaviours in a goroutine. If false,
// arranges to re-call this in an hour. This is so that if we can't currently
// confirm the job is dead due to an ssh issue, but later on the job really does
// die because the server it was running on gets rebooted, we eventually
// auto-kill the job.
func (s *Server) confirmJobDeadAndKill(job *Job) bool {
	if !s.confirmJobDead(job) {
		go func() {
			select {
			case <-time.After(ServerLostJobCheckRetryTime):
				item, err := s.q.Get(job.Key())
				if err != nil || item.Stats().State != queue.ItemStateRun {
					return
				}

				job = item.Data().(*Job)
				if job.State == JobStateRunning && job.Lost {
					s.confirmJobDeadAndKill(job)
				}
			case <-s.stopClientHandling:
				return
			}
		}()

		return false
	}

	go func() {
		_, errk := s.killJob(job.Key())
		if errk != nil {
			s.Warn("failed to kill a job after TTR", "err", errk)
		} else {
			errt := job.TriggerBehaviours(false)
			if errt != nil {
				s.Warn("failed to run behaviours for a killed lost job", "err", errt)
			}
		}
	}()

	return true
}

// confirmJobDead() checks if the actual PID isn't running on the job's host.
//  You must hold the job.Lock() before calling this.
func (s *Server) confirmJobDead(job *Job) bool {
	if job.Pid == 0 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), ServerLostJobCheckTimeout)
	defer cancel()

	return s.scheduler.ProcessNotRunngingOnHost(ctx, job.Pid, job.Host)
}

// releaseJob either releases or buries a job as per its retries, and updates
// our scheduling counts as appropriate.
func (s *Server) releaseJob(job *Job, endState *JobEndState, failReason string, forceStorage bool, forceBury bool) error {
	// first check the job hasn't already been released/buried, only attempt
	// queue changes if not
	job.RLock()
	bury := forceBury
	if !bury && !job.StartTime.IsZero() {
		bury = job.UntilBuried == 1
	}
	key := job.Key()
	currentState := job.State
	job.RUnlock()

	item, err := s.q.Get(key)
	if err != nil {
		return err
	}

	var errq error
	if bury {
		if item.Stats().State == queue.ItemStateBury {
			if currentState == JobStateBuried {
				return nil
			}
		} else {
			errq = s.q.Bury(key)
			if errq == nil {
				s.deleteJobIfRequested(job)
			}
		}
	} else if item.Stats().State == queue.ItemStateDelay {
		if currentState == JobStateDelayed {
			return nil
		}
	} else {
		errq = s.q.Release(key)
	}

	if errq != nil {
		return errq
	}

	job.updateAfterExit(endState, s.limiter)

	job.Lock()
	if forceBury {
		job.UntilBuried = 0
	} else if !job.StartTime.IsZero() {
		// obey jobs's Retries count by adjusting UntilBuried if a
		// client reserved this job and started to run the job's cmd
		job.UntilBuried--
	}

	sgroup := job.schedulerGroup
	var msg string
	if job.UntilBuried <= 0 {
		job.State = JobStateBuried
		msg = "buried job"
	} else {
		job.State = JobStateDelayed
		msg = "released job"
	}
	job.FailReason = failReason
	job.Unlock()

	s.decrementGroupCount(sgroup)
	s.db.updateJobAfterExit(job, endState.Stdout, endState.Stderr, forceStorage)
	s.Debug(msg, "cmd", job.Cmd, "schedGrp", sgroup)
	return nil
}

// inputToQueuedJobs shows you which of the inputJobs are now actually in the
// queue
func (s *Server) inputToQueuedJobs(inputJobs []*Job) []*Job {
	// *** queue.AddMany doesn't currently return which jobs were added and
	// which were dups, and server.createJobs doesn't know which were ignored
	// due to being incomplete, so we do this loop even though it's probably
	// slow and wasteful?...
	var jobs []*Job
	for _, job := range inputJobs {
		item, qerr := s.q.Get(job.Key())
		if qerr == nil && item != nil {
			// append the q's version of the job, not the input job, since the
			// job may have been a duplicate and we want to return its current
			// state
			jobs = append(jobs, s.itemToJob(item, false, false))
		}
	}
	return jobs
}

// killJob sets the killCalled property on a job, to change the subsequent
// behaviour of touching, which should result in an executing job killing
// itself.
//
// If we have lost contact with the job, calling killJob is also the way to
// confirm it is definitely dead and won't spring back to life in the future:
// we release or bury it as appropriate.
//
// If the job wasn't running, returned bool will be false and nothing will have
// been done.
func (s *Server) killJob(jobkey string) (bool, error) {
	item, err := s.q.Get(jobkey)
	if err != nil || item.Stats().State != queue.ItemStateRun {
		return false, err
	}

	job := item.Data().(*Job)
	job.Lock()
	job.killCalled = true

	if job.Lost {
		job.Unlock()
		err = s.releaseJob(job, &JobEndState{Exitcode: -1, Exited: true}, FailReasonLost, false, false)
		return true, err
	}

	job.Unlock()
	return true, err
}

// deleteJobs deletes the jobs with the given keys from the
// bury/delay/dependent/ready queue and the live bucket. Does not delete jobs
// that have jobs dependant upon them, unless all those dependants were also
// supplied to this method at the same time (in any order). Returns the keys of
// jobs actually deleted.
func (s *Server) deleteJobs(keys []string) []string {
	var deleted []string
	for {
		var skippedDeps []string
		var toDelete []string
		schedGroups := make(map[string]int)
		var repGroups []string
		for _, jobkey := range keys {
			item, err := s.q.Get(jobkey)
			if err != nil || item == nil {
				continue
			}
			iState := item.Stats().State
			if iState == queue.ItemStateRun {
				continue
			}

			// we can't allow the removal of jobs that have
			// dependencies, as *queue would regard that as satisfying
			// the dependency and downstream jobs would start
			hasDeps, err := s.q.HasDependents(jobkey)
			if err != nil || hasDeps {
				if hasDeps {
					skippedDeps = append(skippedDeps, jobkey)
				}
				continue
			}
			err = s.q.Remove(jobkey)
			if err == nil {
				deleted = append(deleted, jobkey)
				toDelete = append(toDelete, jobkey)

				job := item.Data().(*Job)
				schedGroups[job.getSchedulerGroup()]++
				repGroups = append(repGroups, job.RepGroup)
				s.Debug("removed job", "cmd", job.Cmd)
			}
		}

		if len(toDelete) > 0 {
			// delete from db live bucket all in one go
			errd := s.db.deleteLiveJobs(toDelete)
			if errd != nil {
				s.Error("job deletion from database failed", "err", errd)
			}

			// update scheduler now we have fewer jobs
			for sg, count := range schedGroups {
				s.decrementGroupCount(sg, count)
			}

			// clean up rpl lookups
			s.rpl.Lock()
			for i, rg := range repGroups {
				delete(s.rpl.lookup[rg], toDelete[i])
			}
			s.rpl.Unlock()

			// if we skipped any due to deps, repeat and see if we
			// can remove everything desired by going down the
			// dependency tree
			if len(skippedDeps) > 0 {
				keys = skippedDeps
				continue
			}
		}
		break
	}
	return deleted
}

// deleteJobIfRequested checks the job's behaviours and deletes the job if
// requested.
func (s *Server) deleteJobIfRequested(job *Job) {
	if job.RemovalRequested() {
		go s.deleteJobs([]string{job.Key()})
	}
}

// killJobsOnServers kills running and confirms lost jobs that were running on
// hosts with the given IDs. Returns the affected jobs.
func (s *Server) killJobsOnServers(serverIDs map[string]bool) []*Job {
	var jobs []*Job
	if len(serverIDs) > 0 {
		running := s.getJobsCurrent(0, JobStateRunning, false, false)
		lost := s.getJobsCurrent(0, JobStateLost, false, false)
		for _, job := range append(running, lost...) {
			if serverIDs[job.HostID] {
				k, err := s.killJob(job.Key())
				if err != nil {
					s.Error("failed to kill a job after destroying its server: %s", err)
				} else if k {
					// try and grab the latest job state after
					// having killed it, but still return the client
					// version of the job
					if item, err := s.q.Get(job.Key()); err == nil && item != nil {
						liveJob := item.Data().(*Job)
						job.State = liveJob.State
						job.UntilBuried = liveJob.UntilBuried
						if job.State == JobStateRunning && !liveJob.StartTime.IsZero() {
							// we're going to release the job as
							// soon as it goes from running to lost
							job.UntilBuried--
						}
					}
					jobs = append(jobs, job)
				}
			}
		}
		s.Debug("killed jobs on bad servers", "number", len(jobs))
	}
	return jobs
}

// getJobsByKeys gets jobs with the given keys (current and complete).
func (s *Server) getJobsByKeys(keys []string, getStd bool, getEnv bool) (jobs []*Job, srerr string, qerr string) {
	var notfound []string
	for _, jobkey := range keys {
		// try and get the job from the in-memory queue
		item, err := s.q.Get(jobkey)
		var job *Job
		if err == nil && item != nil {
			job = s.itemToJob(item, getStd, getEnv)
		} else {
			notfound = append(notfound, jobkey)
		}

		if job != nil {
			jobs = append(jobs, job)
		}
	}

	if len(notfound) > 0 {
		// try and get the jobs from the permanent store
		found, err := s.db.retrieveCompleteJobsByKeys(notfound)
		if err != nil {
			srerr = ErrDBError
			qerr = err.Error()
		} else if len(found) > 0 {
			if getEnv { // complete jobs don't have any std
				for _, job := range found {
					s.jobPopulateStdEnv(job, false, getEnv)
				}
			}
			jobs = append(jobs, found...)
		}
	}

	return jobs, srerr, qerr
}

// checkJobByKey checks to see if the given key corresponds to a job currently
// in the queue, or complete in the database.
func (s *Server) checkJobByKey(key string) (bool, error) {
	item, err := s.q.Get(key)
	if err != nil {
		if qerr, ok := err.(queue.Error); !ok || qerr.Err != queue.ErrNotFound {
			return false, err
		}
	}
	if item != nil {
		return true, nil
	}

	found, err := s.db.retrieveCompleteJobsByKeys([]string{key})
	return len(found) == 1, err
}

// searchRepGroups looks up the rep groups of all jobs that have ever been added
// and returns those that contain the given sub string.
func (s *Server) searchRepGroups(partialRepGroup string) ([]string, error) {
	rgs, err := s.db.retrieveRepGroups()
	if err != nil {
		return nil, err
	}

	var matching []string
	for _, rg := range rgs {
		if strings.Contains(rg, partialRepGroup) {
			matching = append(matching, rg)
		}
	}
	return matching, err
}

// getJobsByRepGroup gets jobs in the given group (current and complete).
func (s *Server) getJobsByRepGroup(repgroup string, search bool, limit int, state JobState, getStd bool, getEnv bool) (jobs []*Job, srerr string, qerr string) {
	var rgs []string
	if search {
		var errs error
		rgs, errs = s.searchRepGroups(repgroup)
		if errs != nil {
			return nil, ErrDBError, errs.Error()
		}
	} else {
		rgs = append(rgs, repgroup)
	}

	for _, rg := range rgs {
		// look in the in-memory queue for matching jobs
		s.rpl.RLock()
		for key := range s.rpl.lookup[rg] {
			item, err := s.q.Get(key)
			if err == nil && item != nil {
				job := s.itemToJob(item, false, false)
				jobs = append(jobs, job)
			}
		}
		s.rpl.RUnlock()

		// look in the permanent store for matching jobs
		if state == "" || state == JobStateComplete {
			var complete []*Job
			complete, srerr, qerr = s.getCompleteJobsByRepGroup(rg)
			if len(complete) > 0 {
				// a job is stored in the db with only the single most recent
				// RepGroup it had, but we're able to retrieve jobs based on any of
				// the RepGroups it ever had; set the RepGroup to the one the user
				// requested *** may want to change RepGroup to store a slice of
				// RepGroups? But that could be massive...
				for _, cj := range complete {
					cj.RepGroup = rg
				}
				jobs = append(jobs, complete...)
			}
		}
	}

	if limit > 0 || state != "" || getStd || getEnv {
		jobs = s.limitJobs(jobs, limit, state, getStd, getEnv)
	}
	return jobs, srerr, qerr
}

// getCompleteJobsByRepGroup gets complete jobs in the given group.
func (s *Server) getCompleteJobsByRepGroup(repgroup string) (jobs []*Job, srerr string, qerr string) {
	jobs, err := s.db.retrieveCompleteJobsByRepGroup(repgroup)
	if err != nil {
		srerr = ErrDBError
		qerr = err.Error()
	}
	return jobs, srerr, qerr
}

// getJobsCurrent gets all current (incomplete) jobs.
func (s *Server) getJobsCurrent(limit int, state JobState, getStd bool, getEnv bool) []*Job {
	allItems := s.q.AllItems()
	jobs := make([]*Job, 0, len(allItems))
	for _, item := range allItems {
		jobs = append(jobs, s.itemToJob(item, false, false))
	}

	if limit > 0 || state != "" || getStd || getEnv {
		jobs = s.limitJobs(jobs, limit, state, getStd, getEnv)
	}

	return jobs
}

// limitJobs handles the limiting of jobs for getJobsByRepGroup() and
// getJobsCurrent(). States 'reserved' and 'running' are treated as the same
// state.
func (s *Server) limitJobs(jobs []*Job, limit int, state JobState, getStd bool, getEnv bool) []*Job {
	groups := make(map[string][]*Job)
	var limited []*Job
	for _, job := range jobs {
		job.RLock()
		jState := job.State
		jExitCode := job.Exitcode
		jFailReason := job.FailReason
		jLost := job.Lost
		job.RUnlock()
		if jState == JobStateRunning {
			if jLost {
				jState = JobStateLost
			} else {
				jState = JobStateReserved
			}
		}

		if state != "" {
			if state == JobStateRunning {
				state = JobStateReserved
			}
			if state == JobStateDeletable {
				if jState == JobStateRunning || jState == JobStateComplete {
					continue
				}
			} else if jState != state {
				continue
			}
		}

		if limit == 0 {
			limited = append(limited, job)
		} else {
			group := fmt.Sprintf("%s.%d.%s", jState, jExitCode, jFailReason)
			jobs, existed := groups[group]
			if existed {
				lenj := len(jobs)
				if lenj == limit {
					jobs[lenj-1].Similar++
				} else {
					jobs = append(jobs, job)
					groups[group] = jobs
				}
			} else {
				jobs = []*Job{job}
				groups[group] = jobs
			}
		}
	}

	if limit > 0 {
		for _, jobs := range groups {
			limited = append(limited, jobs...)
		}
	}

	if getEnv || getStd {
		for _, job := range limited {
			s.jobPopulateStdEnv(job, getStd, getEnv)
		}
	}

	return limited
}

// schedulerGroupDetails is used for debugging purposes to see how many jobs are
// associated with which scheduler groups.
func (s *Server) schedulerGroupDetails() []string {
	s.psgmutex.RLock()
	defer s.psgmutex.RUnlock()
	result := make([]string, len(s.previouslyScheduledGroups))
	i := 0
	for name, group := range s.previouslyScheduledGroups {
		result[i] = fmt.Sprintf("%s (%d jobs)", name, group.getCount())
		i++
	}
	return result
}

func (s *Server) groupToScheduleCmd(rc, group string, req *scheduler.Requirements) string {
	return fmt.Sprintf(rc, group, s.ServerInfo.Deployment, s.ServerInfo.Addr, s.ServerInfo.Host, s.scheduler.ReserveTimeout(req), int(s.scheduler.MaxQueueTime(req).Minutes()))
}

func (s *Server) scheduleRunners(group *sgroup) {
	s.racmutex.RLock()
	rc := s.rc
	s.racmutex.RUnlock()
	if rc == "" {
		return
	}

	err := s.scheduler.Schedule(s.groupToScheduleCmd(rc, group.name, group.req), group.req, group.priority, group.count)
	if err != nil {
		problem := true
		if serr, ok := err.(scheduler.Error); ok && serr.Err == scheduler.ErrImpossible {
			// bury all jobs in this scheduler group
			problem = false
			for {
				item, errr := s.q.Reserve(group.name, 0)
				if errr != nil {
					if qerr, ok := errr.(queue.Error); !ok || qerr.Err != queue.ErrNothingReady {
						s.Warn("scheduleRunners failed to reserve an item", "group", group, "err", errr)
						problem = true
					}
					break
				}
				if item == nil {
					break
				}
				job := item.Data().(*Job)
				job.Lock()
				job.FailReason = FailReasonResource
				job.Unlock()
				errb := s.q.Bury(item.Key)
				if errb != nil {
					s.Warn("scheduleRunners failed to bury an item", "err", errb)
				} else {
					s.deleteJobIfRequested(job)
				}
			}
			s.q.TriggerReadyAddedCallback()
		}

		if problem {
			// log the error *** and inform (by email) the user about this
			// problem if it's persistent, once per hour (day?)
			s.Warn("Server scheduling runners error", "err", err)

			// retry the schedule in a while
			wgk := s.wg.Add(1)
			go func() {
				defer internal.LogPanic(s.Logger, "jobqueue schedule runners retry", true)
				defer s.wg.Done(wgk)

				select {
				case <-time.After(ServerCheckRunnerTime):
					break
				case <-s.stopClientHandling:
					return
				}

				group.Lock()
				s.scheduleRunners(group)
				group.Unlock()
			}()
			return
		}
	}
}

// adjust our count of how many jobs with this schedulerGroup we need in the job
// scheduler. Optionally supply the number to decrement by (default 1).
func (s *Server) decrementGroupCount(schedulerGroup string, optionalDrop ...int) {
	drop := 1
	if len(optionalDrop) == 1 {
		drop = optionalDrop[0]
	}
	s.racmutex.RLock()
	rc := s.rc
	s.racmutex.RUnlock()
	if rc == "" {
		return
	}

	s.psgmutex.RLock()
	group, existed := s.previouslyScheduledGroups[schedulerGroup]
	s.psgmutex.RUnlock()
	if !existed {
		return
	}

	if group.hasSkips() {
		defer s.q.TriggerReadyAddedCallback()
	}

	count := group.decrement(drop)
	if count >= 0 {
		clone := group.clone(count)
		s.scheduleRunners(clone)
	}
}

// getBadServers converts the slice of cloud.Server objects we hold in to a
// slice of badServer structs.
func (s *Server) getBadServers() []*BadServer {
	s.bsmutex.RLock()
	bs := make([]*BadServer, 0, len(s.badServers))
	for _, server := range s.badServers {
		bs = append(bs, &BadServer{
			ID:      server.ID,
			Name:    server.Name,
			IP:      server.IP,
			Date:    time.Now().Unix(),
			IsBad:   server.IsBad(),
			Problem: server.PermanentProblem(),
		})
	}
	s.bsmutex.RUnlock()
	return bs
}

// getSetLimitGroup does the server side of Client.GetOrSetLimitGroup(), taking
// the same argument. The string return value is one of our Err* constants.
func (s *Server) getSetLimitGroup(group string) (int, string, error) {
	name, limit, suffixed, err := s.splitSuffixedLimitGroup(group)
	if err != nil {
		return 0, ErrBadLimitGroup, err
	}
	if suffixed {
		_, removed, err := s.db.storeLimitGroups(map[string]int{name: limit})
		if err != nil {
			return -1, ErrDBError, err
		}
		if limit >= 0 {
			s.limiter.SetLimit(name, uint(limit))
		}
		for _, g := range removed {
			s.limiter.RemoveLimit(g)
		}
		s.q.TriggerReadyAddedCallback()
		return limit, "", nil
	}
	return s.limiter.GetLimit(name), "", nil
}

// splitSuffixedLimitGroup parses a limit group that might be suffixed with a
// colon and the limit of that group. Returns the group name, and if the final
// bool is true, the int will be the desired limit for that group.
func (s *Server) splitSuffixedLimitGroup(group string) (string, int, bool, error) {
	parts := strings.Split(group, ":")
	if len(parts) == 2 {
		limit, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", -1, false, err
		}
		return parts[0], limit, true, nil
	}
	return group, -1, false, nil
}

// storeWebSocketConnection stores a connection and returns a unique identifier
// so that it can be later closed with closeWebSocketConnection(unique) or
// during Server shutdown.
func (s *Server) storeWebSocketConnection(conn *websocket.Conn) string {
	s.wsmutex.Lock()
	defer s.wsmutex.Unlock()
	unique := logext.RandId(8)
	s.wsconns[unique] = conn
	return unique
}

// closeWebSocketConnection closes the connection that was stored with
// storeWebSocketConnection() and that returned the given unique string.
// Closing it this way means that during Server shutdown we won't try and close
// it again.
func (s *Server) closeWebSocketConnection(unique string) {
	s.wsmutex.Lock()
	defer s.wsmutex.Unlock()
	conn, found := s.wsconns[unique]
	if !found {
		return
	}
	err := conn.Close()
	if err != nil {
		s.Warn("websocket close failed", "err", err)
	}
	delete(s.wsconns, unique)
}

// shutdown stops listening to client connections, close all queues and
// persists them to disk.
//
// Does nothing if already shutdown.
//
// For now it also kills all currently running jobs so that their runners don't
// stay alive uselessly. *** This adds 15s to our shutdown time...
func (s *Server) shutdown(reason string, wait bool, stopSigHandling bool) {
	s.ssmutex.Lock()

	if !s.up {
		s.ssmutex.Unlock()
		return
	}
	if stopSigHandling {
		close(s.stopSigHandling)
	}

	s.psgmutex.Lock()
	for name, group := range s.previouslyScheduledGroups {
		s.scheduleRunners(group.clone(0))
		delete(s.previouslyScheduledGroups, name)
	}
	s.psgmutex.Unlock()

	// change touch to always return a kill signal
	s.up = false
	s.drain = true
	s.ServerInfo.Mode = ServerModeDrain
	s.ssmutex.Unlock()
	s.krmutex.Lock()
	s.killRunners = true
	s.krmutex.Unlock()
	if s.HasRunners() {
		// wait until everything must have attempted a touch
		<-time.After(ClientTouchInterval)
	}

	// wait for the runners to actually die
	if wait {
		ticker := time.NewTicker(serverShutdownRunnerTickerTime)
		for range ticker.C {
			if !s.HasRunners() {
				ticker.Stop()
				break
			}
		}
	}

	// stop the scheduler
	s.scheduler.Cleanup()

	// graceful shutdown of all websocket-related goroutines and connections
	s.statusCaster.Close()
	s.badServerCaster.Close()
	s.schedCaster.Close()
	s.wsmutex.Lock()
	for unique, conn := range s.wsconns {
		errc := conn.Close()
		if errc != nil {
			s.Warn("server shutdown failed to close a websocket", "err", errc)
		}
		delete(s.wsconns, unique)
	}
	s.wsmutex.Unlock()

	// not-fully graceful shutdown of http server, since it takes too long to
	// shutdown normally due to a fixed 500ms poll
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	go func() {
		<-time.After(httpServerShutdownTime)
		cancel()
	}()
	err := s.httpServer.Shutdown(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.Warn("server shutdown of web interface failed", "err", err)
	}

	// close our command line interface
	close(s.stopClientHandling)
	err = s.sock.Close()
	if err != nil {
		s.Warn("server shutdown socket close failed", "err", err)
	}

	// close the database
	err = s.db.close()
	if err != nil {
		s.Warn("server shutdown database close failed", "err", err)
	}

	// free any waiting reserves
	s.rpmutex.Lock()
	s.racPending = false
	s.racRunning = false
	for _, ch := range s.waitingReserves {
		close(ch)
	}
	s.waitingReserves = nil
	s.rpmutex.Unlock()

	// wait for our goroutines to finish
	s.wg.Wait(ServerShutdownWaitTime)

	// wait until the ports are really no longer being listened to (which isn't
	// the same as them being available to be reconnected to, but this is the
	// best we can do?)
	for {
		conn, _ := net.DialTimeout("tcp", net.JoinHostPort("", s.ServerInfo.Port), 10*time.Millisecond)
		if conn != nil {
			errc := conn.Close()
			if errc != nil {
				s.Warn("server shutdown port close failed", "port", s.ServerInfo.WebPort, "err", errc)
			}
			continue
		}
		conn, _ = net.DialTimeout("tcp", net.JoinHostPort("", s.ServerInfo.WebPort), 10*time.Millisecond)
		if conn != nil {
			errc := conn.Close()
			if errc != nil {
				s.Warn("server shutdown port close failed", "port", s.ServerInfo.WebPort, "err", errc)
			}
			continue
		}
		break
	}

	// clean up our queues and empty everything out to be garbage collected,
	// in case the same process calls Serve() again after this
	err = s.q.Destroy()
	if err != nil {
		s.Warn("server shutdown queue destruction failed", "err", err)
	}
	s.q = nil

	s.krmutex.Lock()
	s.killRunners = false
	s.krmutex.Unlock()

	s.ssmutex.Lock()
	s.drain = false
	wasBlocking := s.blocking
	s.blocking = false
	s.ssmutex.Unlock()

	if wasBlocking {
		s.done <- Error{"Serve", "", reason}
	}
}
