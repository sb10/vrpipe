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

package jobqueue

// This file contains functions for interacting with our database, which is
// boltdb, a simple key/val store with transactions and hot backup ability.
// We don't use a generic ORM for boltdb like Storm, because we can do custom
// queries that are multiple times faster than what Storm can do.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	sync "github.com/sasha-s/go-deadlock"

	"github.com/VertebrateResequencing/muxfys/v4"
	"github.com/VertebrateResequencing/wr/internal"
	lru "github.com/hashicorp/golang-lru"
	"github.com/inconshreveable/log15"
	"github.com/sb10/waitgroup"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	dbDelimiter                   = "_::_"
	jobStatWindowPercent          = float32(5)
	dbFilePermission              = 0600
	minimumTimeBetweenBackups     = 30 * time.Second
	dbRunningTransactionsWaitTime = 1 * time.Minute
)

var (
	bucketJobsLive     = []byte("jobslive")
	bucketJobsComplete = []byte("jobscomplete")
	bucketRTK          = []byte("repgroupToKey")
	bucketRGs          = []byte("repgroups")
	bucketLGs          = []byte("limitgroups")
	bucketDTK          = []byte("depgroupToKey")
	bucketRDTK         = []byte("reverseDepgroupToKey")
	bucketEnvs         = []byte("envs")
	bucketStdO         = []byte("stdo")
	bucketStdE         = []byte("stde")
	bucketJobRAM       = []byte("jobRAM")
	bucketJobDisk      = []byte("jobDisk")
	bucketJobSecs      = []byte("jobSecs")
	wipeDevDBOnInit    = true
	forceBackups       = false
)

// Rec* variables are only exported for testing purposes (*** though they should
// probably be user configurable somewhere...).
var (
	RecMBRound  = 100 // when we recommend amount of memory to reserve for a job, we round up to the nearest RecMBRound MBs
	RecSecRound = 1   // when we recommend time to reserve for a job, we round up to the nearest RecSecRound seconds
)

// sobsd ('slice of byte slice doublets') implements sort interface so we can
// sort a slice of []byte doublets, sorting on the first byte slice, needed for
// efficient Puts in to the database.
type sobsd [][2][]byte

func (s sobsd) Len() int {
	return len(s)
}
func (s sobsd) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sobsd) Less(i, j int) bool {
	cmp := bytes.Compare(s[i][0], s[j][0])
	return cmp == -1
}

// sobsdStorer is the kind of function that stores the contents of a sobsd in
// a particular bucket
type sobsdStorer func(bucket []byte, encodes sobsd) (err error)

type db struct {
	backupLast    time.Time
	backupPath    string
	backupPathTmp string
	ch            codec.Handle
	log15.Logger
	backupStopWait       chan bool
	backupMount          *muxfys.MuxFys
	backupNotification   chan bool
	backupWait           time.Duration
	bolt                 *bolt.DB
	envcache             *lru.ARCCache
	updatingAfterJobExit int
	wg                   *waitgroup.WaitGroup
	wgMutex              sync.Mutex // protects wg since we want to call Wait() while another goroutine might call Add()
	sync.RWMutex
	backingUp      bool
	backupFinal    bool
	backupQueued   bool
	backupsEnabled bool
	s3accessor     *muxfys.S3Accessor
	closed         bool
	slowBackups    bool // just for testing purposes
}

// initDB opens/creates our database and sets things up for use. If dbFile
// doesn't exist or seems corrupted, we copy it from backup if that exists,
// otherwise we start fresh.
//
// dbBkFile can be an S3 url specified like: s3://[profile@]bucket/path/file
// which will cause that s3 path to be mounted in the same directory as dbFile
// and backups will be written there.
//
// In development we delete any existing db and force a fresh start. Backups
// are also not carried out, so dbBkFile is ignored.
func initDB(dbFile string, dbBkFile string, deployment string, logger log15.Logger) (*db, string, error) {
	l := logger.New()

	var backupsEnabled bool

	var accessor *muxfys.S3Accessor

	backupPathTmp := dbBkFile + ".tmp"

	var msg string

	if deployment == internal.Production || forceBackups {
		backupsEnabled = true

		if internal.InS3(dbBkFile) {
			if deployment == internal.Development {
				dbBkFile += "." + deployment
			}

			path := strings.TrimPrefix(dbBkFile, internal.S3Prefix)
			pp := strings.Split(path, "@")
			profile := "default"
			if len(pp) == 2 {
				profile = pp[0]
				path = pp[1]
			}
			path = filepath.Dir(path)

			accessorConfig, err := muxfys.S3ConfigFromEnvironment(profile, path)
			if err != nil {
				return nil, "", err
			}

			accessor, err = muxfys.NewS3Accessor(accessorConfig)
			if err != nil {
				return nil, "", err
			}

			dbBkFile = filepath.Join(path, filepath.Base(dbBkFile))
			dbBkFile, err = stripBucketFromS3Path(dbBkFile)
			if err != nil {
				return nil, "", err
			}

			backupPathTmp = dbFile + ".s3backup_tmp"

			if _, err = os.Stat(dbFile); os.IsNotExist(err) {
				err = accessor.DownloadFile(dbBkFile, dbFile)
				if err == nil {
					msg = "recreated missing db file " + dbFile + " from s3 backup file " + dbBkFile
				}
			}
		}
	}

	if wipeDevDBOnInit && deployment == internal.Development {
		errr := os.Remove(dbFile)
		if errr != nil && !os.IsNotExist(errr) {
			l.Warn("Failed to remove database file", "path", dbFile, "err", errr)
		}

		if accessor != nil {
			errr = accessor.DeleteFile(dbBkFile)
		} else {
			errr = os.Remove(dbBkFile)
		}

		if errr != nil && !os.IsNotExist(errr) {
			l.Warn("Failed to remove database backup file", "path", dbBkFile, "err", errr)
		}
	}

	var boltdb *bolt.DB
	var err error
	if _, err = os.Stat(dbFile); os.IsNotExist(err) {
		if _, err = os.Stat(dbBkFile); os.IsNotExist(err) {
			boltdb, err = bolt.Open(dbFile, dbFilePermission, nil)
			msg = "created new empty db file " + dbFile
		} else {
			err = copyFile(dbBkFile, dbFile)
			if err != nil {
				return nil, msg, err
			}
			boltdb, err = bolt.Open(dbFile, dbFilePermission, nil)
			msg = "recreated missing db file " + dbFile + " from backup file " + dbBkFile
		}
	} else {
		boltdb, err = bolt.Open(dbFile, dbFilePermission, nil)
		if err != nil {
			// try the backup
			bkPath := dbBkFile
			if accessor != nil {
				bkPath = backupPathTmp

				errdl := accessor.DownloadFile(dbBkFile, bkPath)
				if errdl != nil {
					msg = fmt.Sprintf("tried to recreate corrupt (?) db file %s "+
						"from s3 backup file %s (error with original db file was: %s)",
						dbFile, dbBkFile, err)

					return nil, msg, errdl
				}

				defer func() {
					errr := os.Remove(bkPath)
					if errr != nil {
						l.Warn("failed to remove temporary s3 download of database backup", "err", errr)
					}
				}()
			}

			if _, errbk := os.Stat(bkPath); errbk == nil {
				boltdb, errbk = bolt.Open(bkPath, dbFilePermission, nil)
				if errbk == nil {
					origerr := err
					msg = fmt.Sprintf("tried to recreate corrupt (?) db file %s from backup file %s (error with original db file was: %s)", dbFile, dbBkFile, err)
					err = os.Remove(dbFile)
					if err != nil {
						return nil, msg, err
					}
					err = copyFile(bkPath, dbFile)
					if err != nil {
						return nil, msg, err
					}
					boltdb, err = bolt.Open(dbFile, dbFilePermission, nil)
					msg = fmt.Sprintf("recreated corrupt (?) db file %s from backup file %s (error with original db file was: %s)", dbFile, dbBkFile, origerr)
				}
			}
		}
	}
	if err != nil {
		return nil, msg, err
	}

	// ensure our buckets are in place
	err = boltdb.Update(func(tx *bolt.Tx) error {
		_, errf := tx.CreateBucketIfNotExists(bucketJobsLive)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketJobsLive, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketJobsComplete)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketJobsComplete, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketRTK)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketRTK, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketRGs)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketRGs, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketLGs)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketLGs, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketDTK)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketDTK, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketRDTK)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketRDTK, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketEnvs)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketEnvs, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketStdO)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketStdO, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketStdE)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketStdE, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketJobRAM)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketJobRAM, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketJobDisk)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketJobDisk, errf)
		}
		_, errf = tx.CreateBucketIfNotExists(bucketJobSecs)
		if errf != nil {
			return fmt.Errorf("create bucket %s: %s", bucketJobSecs, errf)
		}
		return nil
	})
	if err != nil {
		return nil, msg, err
	}

	// we will cache frequently used things to avoid actual db (disk) access
	envcache, err := lru.NewARC(12) // we don't expect that many different ENVs to be in use at once
	if err != nil {
		return nil, msg, err
	}

	dbstruct := &db{
		bolt:               boltdb,
		envcache:           envcache,
		ch:                 new(codec.BincHandle),
		backupsEnabled:     backupsEnabled,
		backupPath:         dbBkFile,
		backupPathTmp:      backupPathTmp,
		backupNotification: make(chan bool),
		backupWait:         minimumTimeBetweenBackups,
		backupStopWait:     make(chan bool),
		s3accessor:         accessor,
		wg:                 waitgroup.New(),
		Logger:             l,
	}

	return dbstruct, msg, err
}

// storeLimitGroups stores a mapping of group names to unsigned ints in a
// dedicated bucket. If a group was already in the database, and it had a
// different value, that group name will be returned in the changed slice. If
// the group is given with a value less than 0, it is not stored in the
// database; any existing entry is removed and the name is returned in the
// removed slice.
func (db *db) storeLimitGroups(limitGroups map[string]int) (changed []string, removed []string, err error) {
	err = db.bolt.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketLGs)

		for group, limit := range limitGroups {
			key := []byte(group)

			v := b.Get(key)
			if v != nil {
				if limit < 0 {
					errd := b.Delete(key)
					if errd != nil {
						return errd
					}
					removed = append(removed, group)
					continue
				}

				if binary.BigEndian.Uint64(v) == uint64(limit) {
					continue
				}
				changed = append(changed, group)
			} else if limit < 0 {
				continue
			}

			v = make([]byte, 8)
			binary.BigEndian.PutUint64(v, uint64(limit))
			errp := b.Put(key, v)
			if errp != nil {
				return errp
			}
		}

		return nil
	})
	return changed, removed, err
}

// retrieveLimitGroup gets a value for a particular group from the db that was
// stored with storeLimitGroups(). If the group wasn't stored, returns -1.
func (db *db) retrieveLimitGroup(group string) int {
	v := db.retrieve(bucketLGs, group)
	if v == nil {
		return -1
	}
	return int(binary.BigEndian.Uint64(v))
}

// storeNewJobs stores jobs in the live bucket, where they will only be used for
// disaster recovery. It also stores a lookup from the Job.RepGroup to the Job's
// key, and since this is independent, and we call this prior to checking for
// dups, we allow the same job to be looked up by multiple RepGroups. Likewise,
// we store a lookup for the Job.DepGroups and .Dependencies.DepGroups().
//
// If ignoreAdded is true, jobs that have already completed will be ignored
// along with those that have been added and the returned alreadyAdded value
// will increase.
//
// While storing it also checks if any previously stored jobs depend on a dep
// group that an input job is a member of. If not, jobsToQueue return value will
// be identical to the input job slice (minus any jobs ignored due to being
// complete). Otherwise, if the affected job was Archive()d (and not currently
// being re-run), then it will be appended to (a copy of) the input job slice
// and returned in jobsToQueue. If the affected job was in the live bucket
// (currently queued), it will be returned in the jobsToUpdate slice: you should
// use queue methods to update the job in the queue.
//
// Finally, it triggers a background database backup.
func (db *db) storeNewJobs(jobs []*Job, ignoreAdded bool) (jobsToQueue []*Job, jobsToUpdate []*Job, alreadyAdded int, err error) {
	encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, jobsToQueue, jobsToUpdate, alreadyAdded, err := db.prepareNewJobs(jobs, ignoreAdded)

	if err != nil {
		return jobsToQueue, jobsToUpdate, alreadyAdded, err
	}

	if len(encodedJobs) > 0 {
		// now go ahead and store the lookups and jobs
		numStores := 2
		if len(rgs) > 0 {
			numStores++
		}
		if len(dgLookups) > 0 {
			numStores++
		}
		if len(rdgLookups) > 0 {
			numStores++
		}
		errors := make(chan error, numStores)

		db.wgMutex.Lock()
		wgk := db.wg.Add(1)
		go func() {
			defer internal.LogPanic(db.Logger, "jobqueue database storeNewJobs rglookups", true)
			defer db.wg.Done(wgk)
			sort.Sort(rgLookups)
			errors <- db.storeBatched(bucketRTK, rgLookups, db.storeLookups)
		}()

		if len(rgs) > 0 {
			wgk2 := db.wg.Add(1)
			go func() {
				defer internal.LogPanic(db.Logger, "jobqueue database storeNewJobs repGroups", true)
				defer db.wg.Done(wgk2)
				sort.Sort(rgs)
				errors <- db.storeBatched(bucketRGs, rgs, db.storeLookups)
			}()
		}

		if len(dgLookups) > 0 {
			wgk3 := db.wg.Add(1)
			go func() {
				defer internal.LogPanic(db.Logger, "jobqueue database dgLookups", true)
				defer db.wg.Done(wgk3)
				sort.Sort(dgLookups)
				errors <- db.storeBatched(bucketDTK, dgLookups, db.storeLookups)
			}()
		}

		if len(rdgLookups) > 0 {
			wgk4 := db.wg.Add(1)
			go func() {
				defer internal.LogPanic(db.Logger, "jobqueue database storeNewJobs rdgLookups", true)
				defer db.wg.Done(wgk4)
				sort.Sort(rdgLookups)
				errors <- db.storeBatched(bucketRDTK, rdgLookups, db.storeLookups)
			}()
		}

		wgk5 := db.wg.Add(1)
		go func() {
			defer internal.LogPanic(db.Logger, "jobqueue database storeNewJobs encodedJobs", true)
			defer db.wg.Done(wgk5)
			sort.Sort(encodedJobs)
			errors <- db.storeBatched(bucketJobsLive, encodedJobs, db.storeEncodedJobs)
		}()
		db.wgMutex.Unlock()

		seen := 0
		for thisErr := range errors {
			if thisErr != nil {
				err = thisErr
			}

			seen++
			if seen == numStores {
				close(errors)
				break
			}
		}
	}

	// *** on error, because we were batching, and doing lookups separately to
	// each other and jobs, we should go through and remove anything we did
	// manage to add... (but this isn't so critical, since on failure here,
	// they are not added to the in-memory queue and user gets an error and they
	// would try to add everything back again; conversely, if we try to retrieve
	// non-existent jobs based on lookups that shouldn't be there, they are
	// silently skipped)

	if err == nil && alreadyAdded != len(jobs) {
		db.backgroundBackup()
	}

	return jobsToQueue, jobsToUpdate, alreadyAdded, err
}

func (db *db) prepareNewJobs(jobs []*Job, ignoreAdded bool) (encodedJobs, rgLookups, dgLookups, rdgLookups, rgs sobsd, jobsToQueue []*Job, jobsToUpdate []*Job, alreadyAdded int, err error) {
	// turn the jobs in to sobsd and sort by their keys, likewise for the
	// lookups
	repGroups := make(map[string]bool)
	depGroups := make(map[string]bool)
	newJobKeys := make(map[string]bool)
	var keptJobs []*Job
	for _, job := range jobs {
		keyStr := job.Key()

		if ignoreAdded {
			var added bool
			added, err = db.checkIfAdded(keyStr)
			if err != nil {
				return encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, jobsToQueue, jobsToUpdate, alreadyAdded, err
			}
			if added {
				alreadyAdded++
				continue
			}
			keptJobs = append(keptJobs, job)
		}

		newJobKeys[keyStr] = true
		key := []byte(keyStr)

		job.RLock()
		rgLookups = append(rgLookups, [2][]byte{db.generateLookupKey(job.RepGroup, key), nil})
		repGroups[job.RepGroup] = true

		for _, depGroup := range job.DepGroups {
			if depGroup != "" {
				dgLookups = append(dgLookups, [2][]byte{db.generateLookupKey(depGroup, key), nil})
				depGroups[depGroup] = true
			}
		}

		for _, depGroup := range job.Dependencies.DepGroups() {
			rdgLookups = append(rdgLookups, [2][]byte{db.generateLookupKey(depGroup, key), nil})
		}
		job.RUnlock()

		var encoded []byte
		enc := codec.NewEncoderBytes(&encoded, db.ch)
		job.RLock()
		err = enc.Encode(job)
		job.RUnlock()
		if err != nil {
			return encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, jobsToQueue, jobsToUpdate, alreadyAdded, err
		}
		encodedJobs = append(encodedJobs, [2][]byte{key, encoded})
	}

	if len(encodedJobs) > 0 {
		if !ignoreAdded {
			keptJobs = jobs
		}

		// first determine if any of these new jobs are the parent of previously
		// stored jobs
		if len(depGroups) > 0 {
			jobsToQueue, jobsToUpdate, err = db.retrieveDependentJobs(depGroups, newJobKeys)

			// arrange to have resurrected complete jobs stored in the live
			// bucket again
			for _, job := range jobsToQueue {
				key := []byte(job.Key())
				var encoded []byte
				enc := codec.NewEncoderBytes(&encoded, db.ch)
				job.RLock()
				err = enc.Encode(job)
				job.RUnlock()
				if err != nil {
					return encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, jobsToQueue, jobsToUpdate, alreadyAdded, err
				}
				encodedJobs = append(encodedJobs, [2][]byte{key, encoded})
			}

			if len(jobsToQueue) > 0 {
				jobsToQueue = append(jobsToQueue, jobs...)
			} else {
				jobsToQueue = keptJobs
			}
		} else {
			jobsToQueue = keptJobs
		}

		for rg := range repGroups {
			rgs = append(rgs, [2][]byte{[]byte(rg), nil})
		}
	}

	return encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, jobsToQueue, jobsToUpdate, alreadyAdded, err
}

// generateLookupKey creates a lookup key understood by the retrieval methods,
// concatenating prefix with a delimiter and the job key.
func (db *db) generateLookupKey(prefix string, jobKey []byte) []byte {
	key := append([]byte(prefix), []byte(dbDelimiter)...)
	return append(key, jobKey...)
}

// checkIfLive tells you if a job with the given key is currently in the live
// bucket.
func (db *db) checkIfLive(key string) (bool, error) {
	var isLive bool
	err := db.bolt.View(func(tx *bolt.Tx) error {
		newJobBucket := tx.Bucket(bucketJobsLive)
		if newJobBucket.Get([]byte(key)) != nil {
			isLive = true
		}
		return nil
	})
	return isLive, err
}

// checkIfAdded tells you if a job with the given key is currently in the
// complete bucket or the live bucket.
func (db *db) checkIfAdded(key string) (bool, error) {
	var isInDB bool
	err := db.bolt.View(func(tx *bolt.Tx) error {
		newJobBucket := tx.Bucket(bucketJobsLive)
		completeJobBucket := tx.Bucket(bucketJobsComplete)
		if newJobBucket.Get([]byte(key)) != nil || completeJobBucket.Get([]byte(key)) != nil {
			isInDB = true
		}
		return nil
	})
	return isInDB, err
}

// archiveJob deletes a job from the live bucket, and adds a new version of it
// (with different properties) to the complete bucket.
//
// Also does what updateJobAfterExit does, except for the storage of any new
// stdout/err.
//
// The key you supply must be the key of the job you supply, or bad things will
// happen - no checking is done! A backgroundBackup() is triggered afterwards.
func (db *db) archiveJob(key string, job *Job) error {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, db.ch)
	job.RLock()
	err := enc.Encode(job)
	job.RUnlock()
	if err != nil {
		return err
	}

	err = db.bolt.Batch(func(tx *bolt.Tx) error {
		bo := tx.Bucket(bucketStdO)
		be := tx.Bucket(bucketStdE)
		key := []byte(key)
		errf := bo.Delete(key)
		if errf != nil {
			return errf
		}
		errf = be.Delete(key)
		if errf != nil {
			return errf
		}

		b := tx.Bucket(bucketJobsLive)
		errf = b.Delete(key)
		if errf != nil {
			return errf
		}

		b = tx.Bucket(bucketJobsComplete)
		errf = b.Put(key, encoded)
		if errf != nil {
			return errf
		}

		b = tx.Bucket(bucketJobRAM)
		errf = b.Put([]byte(fmt.Sprintf("%s%s%20d", job.ReqGroup, dbDelimiter, job.PeakRAM)), []byte(strconv.Itoa(job.PeakRAM)))
		if errf != nil {
			return errf
		}
		b = tx.Bucket(bucketJobDisk)
		errf = b.Put([]byte(fmt.Sprintf("%s%s%20d", job.ReqGroup, dbDelimiter, job.PeakDisk)), []byte(strconv.Itoa(int(job.PeakDisk))))
		if errf != nil {
			return errf
		}
		b = tx.Bucket(bucketJobSecs)
		secs := int(math.Ceil(job.EndTime.Sub(job.StartTime).Seconds()))
		return b.Put([]byte(fmt.Sprintf("%s%s%20d", job.ReqGroup, dbDelimiter, secs)), []byte(strconv.Itoa(secs)))
	})

	db.backgroundBackup()

	return err
}

// deleteLiveJob remove a job from the live bucket, for use when jobs were
// added in error.
func (db *db) deleteLiveJob(key string) {
	db.remove(bucketJobsLive, key)
	db.backgroundBackup()
	//*** we're not removing the lookup entries from the bucket*TK buckets...
}

// deleteLiveJobs remove multiple jobs from the live bucket.
func (db *db) deleteLiveJobs(keys []string) error {
	err := db.bolt.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobsLive)
		for _, key := range keys {
			errd := b.Delete([]byte(key))
			if errd != nil {
				return errd
			}
		}
		return nil
	})

	if err != nil {
		return err
	}

	db.backgroundBackup()
	//*** we're not removing the lookup entries from the bucket*TK buckets...

	return nil
}

// recoverIncompleteJobs returns all jobs in the live bucket, for use when
// restarting the server, allowing you start working on any jobs that were
// stored with storeNewJobs() but not yet archived with archiveJob().
//
// Note that you will get back the job as it was in its last recorded state.
// The state is recorded when a job starts to run, when it exits, and when it
// is kicked.
func (db *db) recoverIncompleteJobs() ([]*Job, error) {
	var jobs []*Job
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobsLive)
		return b.ForEach(func(key, encoded []byte) error {
			if encoded != nil {
				dec := codec.NewDecoderBytes(encoded, db.ch)
				job := &Job{}
				errf := dec.Decode(job)
				if errf != nil {
					return errf
				}
				jobs = append(jobs, job)
			}
			return nil
		})
	})
	return jobs, err
}

// retrieveCompleteJobsByKeys gets jobs with the given keys from the completed
// jobs bucket (ie. those that have gone through the queue and been Remove()d).
func (db *db) retrieveCompleteJobsByKeys(keys []string) ([]*Job, error) {
	var jobs []*Job
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketJobsComplete)
		for _, key := range keys {
			encoded := b.Get([]byte(key))
			if encoded != nil {
				dec := codec.NewDecoderBytes(encoded, db.ch)
				job := &Job{}
				err := dec.Decode(job)
				if err == nil {
					jobs = append(jobs, job)
				}
			}
		}
		return nil
	})
	return jobs, err
}

// retrieveRepGroups gets the rep groups of all jobs that have ever been added.
func (db *db) retrieveRepGroups() ([]string, error) {
	var rgs []string
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRGs)
		return b.ForEach(func(k, v []byte) error {
			rgs = append(rgs, string(k))
			return nil
		})
	})
	return rgs, err
}

// retrieveCompleteJobsByRepGroup gets jobs with the given RepGroup from the
// completed jobs bucket (ie. those that have gone through the queue and been
// Archive()d), but not those that are also currently live (ie. are being
// re-run).
func (db *db) retrieveCompleteJobsByRepGroup(repgroup string) ([]*Job, error) {
	var jobs []*Job
	err := db.bolt.View(func(tx *bolt.Tx) error {
		newJobBucket := tx.Bucket(bucketJobsLive)
		completeJobBucket := tx.Bucket(bucketJobsComplete)
		lookupBucket := tx.Bucket(bucketRTK).Cursor()
		prefix := []byte(repgroup + dbDelimiter)
		for k, _ := lookupBucket.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = lookupBucket.Next() {
			key := bytes.TrimPrefix(k, prefix)
			encoded := completeJobBucket.Get(key)
			if len(encoded) > 0 && newJobBucket.Get(key) == nil {
				dec := codec.NewDecoderBytes(encoded, db.ch)
				job := &Job{}
				err := dec.Decode(job)
				if err != nil {
					return err
				}
				jobs = append(jobs, job)
			}
		}
		return nil
	})
	return jobs, err
}

// retrieveDependentJobs gets previously stored jobs that had a dependency on
// one for the input depGroups. If the job is found in the live bucket, then it
// is returned in the jobsToUpdate return value. If it is found in the complete
// bucket, and is not true in the supplied newJobKeys map, then it is returned
// in the jobsToQueue return value.
func (db *db) retrieveDependentJobs(depGroups map[string]bool, newJobKeys map[string]bool) (jobsToQueue []*Job, jobsToUpdate []*Job, err error) {
	// first convert the depGroups in to sorted prefixes, for linear searching
	prefixes := make(sobsd, 0, len(depGroups))
	for depGroup := range depGroups {
		prefixes = append(prefixes, [2][]byte{[]byte(depGroup + dbDelimiter), nil})
	}
	sort.Sort(prefixes)

	err = db.bolt.View(func(tx *bolt.Tx) error {
		newJobBucket := tx.Bucket(bucketJobsLive)
		completeJobBucket := tx.Bucket(bucketJobsComplete)
		lookupBucket := tx.Bucket(bucketRDTK).Cursor()
		doneKeys := make(map[string]bool)
		for {
			newDepGroups := make(map[string]bool)
			for _, bsd := range prefixes {
				for k, _ := lookupBucket.Seek(bsd[0]); bytes.HasPrefix(k, bsd[0]); k, _ = lookupBucket.Next() {
					key := bytes.TrimPrefix(k, bsd[0])
					keyStr := string(key)
					if doneKeys[keyStr] {
						continue
					}

					encoded := newJobBucket.Get(key)
					live := false
					if len(encoded) > 0 {
						live = true
					} else if !newJobKeys[keyStr] {
						encoded = completeJobBucket.Get(key)
					}

					if len(encoded) > 0 {
						dec := codec.NewDecoderBytes(encoded, db.ch)
						job := &Job{}
						errf := dec.Decode(job)
						if errf != nil {
							return errf
						}

						// since we're going to add this job, we also need to
						// check its DepGroups and repeat this loop on any new
						// ones
						for _, depGroup := range job.DepGroups {
							if depGroup != "" && !depGroups[depGroup] {
								newDepGroups[depGroup] = true
							}
						}

						if live {
							jobsToUpdate = append(jobsToUpdate, job)
						} else {
							jobsToQueue = append(jobsToQueue, job)
						}
					}

					doneKeys[keyStr] = true
				}
			}

			if len(newDepGroups) > 0 {
				var newPrefixes sobsd
				for depGroup := range newDepGroups {
					newPrefixes = append(newPrefixes, [2][]byte{[]byte(depGroup + dbDelimiter), nil})
					depGroups[depGroup] = true
				}
				sort.Sort(newPrefixes)
				prefixes = newPrefixes
			} else {
				break
			}
		}
		return nil
	})
	return jobsToQueue, jobsToUpdate, err
}

// retrieveIncompleteJobKeysByDepGroup gets jobs with the given DepGroup from
// the live bucket (ie. those that have been added to the queue and not yet
// Archive()d - even if they've been added and archived in the past).
func (db *db) retrieveIncompleteJobKeysByDepGroup(depgroup string) ([]string, error) {
	var jobKeys []string
	err := db.bolt.View(func(tx *bolt.Tx) error {
		newJobBucket := tx.Bucket(bucketJobsLive)
		lookupBucket := tx.Bucket(bucketDTK).Cursor()
		prefix := []byte(depgroup + dbDelimiter)
		for k, _ := lookupBucket.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = lookupBucket.Next() {
			key := bytes.TrimPrefix(k, prefix)
			if newJobBucket.Get(key) != nil {
				jobKeys = append(jobKeys, string(key))
			}
		}
		return nil
	})
	return jobKeys, err
}

// storeEnv stores a clientRequest.Env in db unless cached, which means it must
// already be there. Returns a key by which the stored Env can be retrieved.
func (db *db) storeEnv(env []byte) (string, error) {
	envkey := byteKey(env)
	if !db.envcache.Contains(envkey) {
		err := db.store(bucketEnvs, envkey, env)
		if err != nil {
			return envkey, err
		}
		db.envcache.Add(envkey, env)
	}
	return envkey, nil
}

// retrieveEnv gets a value from the db that was stored with storeEnv(). The
// value may come from the cache, avoiding db access.
func (db *db) retrieveEnv(envkey string) []byte {
	cached, got := db.envcache.Get(envkey)
	if got {
		return cached.([]byte)
	}

	envc := db.retrieve(bucketEnvs, envkey)
	db.envcache.Add(envkey, envc)
	return envc
}

// updateJobAfterExit stores the Job's peak RAM usage and wall time against the
// Job's ReqGroup, but only if the job failed for using too much RAM or time,
// allowing recommendedReqGroup*(ReqGroup) to work.
//
// So that state can be restored if the server crashes and is restarted, the
// job is rewritten in its current state in to the live bucket.
//
// It also updates the stdout/err associated with a job. We don't want to store
// these in the job, since that would waste a lot of the queue's memory; we
// store in db instead, and only retrieve when a client needs to see these. To
// stop the db file becoming enormous, we only store these if the cmd failed (or
// if forceStorage is true: used when the job got buried) and also delete these
// from db when the cmd completes successfully.
//
// By doing the deletion upfront, we also ensure we have the latest std, which
// may be nil even on cmd failure. Since it is not critical to the running of
// jobs and workflows that this works 100% of the time, we ignore errors and
// write to bolt in a goroutine, giving us a significant speed boost.
func (db *db) updateJobAfterExit(job *Job, stdo []byte, stde []byte, forceStorage bool) {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, db.ch)
	db.Lock()
	defer db.Unlock()
	if db.closed {
		return
	}
	jobkey := job.Key()
	job.RLock()
	secs := int(math.Ceil(job.EndTime.Sub(job.StartTime).Seconds()))
	jrg := job.ReqGroup
	jpr := job.PeakRAM
	jpd := job.PeakDisk
	jec := job.Exitcode
	jfr := job.FailReason
	err := enc.Encode(job)
	job.RUnlock()
	if err != nil {
		db.Error("Database operation updateJobAfterExit failed due to Encode failure", "err", err)
		return
	}

	db.updatingAfterJobExit++

	db.wgMutex.Lock()
	defer db.wgMutex.Unlock()
	wgk := db.wg.Add(1)
	go func() {
		defer internal.LogPanic(db.Logger, "updateJobAfterExit", true)

		err := db.bolt.Batch(func(tx *bolt.Tx) error {
			key := []byte(jobkey)

			bjl := tx.Bucket(bucketJobsLive)
			if bjl.Get(key) != nil {
				errf := bjl.Put(key, encoded)
				if errf != nil {
					return errf
				}
			}

			bo := tx.Bucket(bucketStdO)
			be := tx.Bucket(bucketStdE)
			errf := bo.Delete(key)
			if errf != nil {
				return errf
			}
			errf = be.Delete(key)
			if errf != nil {
				return errf
			}

			if jec != 0 || forceStorage {
				if len(stdo) > 0 {
					errf = bo.Put(key, stdo)
				}
				if len(stde) > 0 {
					errf = be.Put(key, stde)
				}
			}
			if errf != nil {
				return errf
			}

			switch jfr {
			case FailReasonRAM:
				b := tx.Bucket(bucketJobRAM)
				errf = b.Put([]byte(fmt.Sprintf("%s%s%20d", jrg, dbDelimiter, jpr)), []byte(strconv.Itoa(jpr)))
			case FailReasonDisk:
				b := tx.Bucket(bucketJobDisk)
				errf = b.Put([]byte(fmt.Sprintf("%s%s%20d", jrg, dbDelimiter, jpd)), []byte(strconv.Itoa(int(jpd))))
			case FailReasonTime:
				b := tx.Bucket(bucketJobSecs)
				errf = b.Put([]byte(fmt.Sprintf("%s%s%20d", jrg, dbDelimiter, secs)), []byte(strconv.Itoa(secs)))
			}
			return errf
		})
		db.wg.Done(wgk)
		if err != nil {
			db.Error("Database operation updateJobAfterExit failed", "err", err)
		}
		db.Lock()
		db.updatingAfterJobExit--
		db.Unlock()
	}()
}

// updateJobAfterChange rewrites the job's entry in the live bucket, to enable
// complete recovery after a crash. This happens in a goroutine, since it isn't
// essential this happens, and we benefit from the speed.
func (db *db) updateJobAfterChange(job *Job) {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, db.ch)
	db.RLock()
	defer db.RUnlock()
	if db.closed {
		return
	}
	key := []byte(job.Key())
	job.RLock()
	err := enc.Encode(job)
	job.RUnlock()
	if err != nil {
		db.Error("Database operation updateJobAfterChange failed due to Encode failure", "err", err)
		return
	}

	db.wgMutex.Lock()
	defer db.wgMutex.Unlock()
	wgk := db.wg.Add(1)
	go func() {
		defer internal.LogPanic(db.Logger, "updateJobAfterChange", true)

		err := db.bolt.Batch(func(tx *bolt.Tx) error {
			bjl := tx.Bucket(bucketJobsLive)
			if bjl.Get(key) == nil {
				// it's possible for these batches to be interleaved with
				// archiveJob batches, and for this batch to update that a job
				// was started to actually execute after the batch that says the
				// job completed, removing it from the live bucket. In that
				// case, don't add it back to the live bucket here.
				return nil
			}
			return bjl.Put(key, encoded)
		})
		db.wg.Done(wgk)
		if err != nil {
			db.Error("Database operation updateJobAfterChange failed", "err", err)
			return
		}
		db.backgroundBackup()
	}()
}

// modifyLiveJobs is for use if jobs currently in the queue are modified such
// that their Key() changes, or their dependencies or dependency groups change.
// We simply remove all reference to the old keys in the lookup buckets, as well
// as the old jobs from the live bucket, and then do the equivalent of
// storeNewJobs() on the supplied new version of the jobs. (This is all done in
// one transaction, so won't leave things in a bad state if interuppted half
// way.)
// The order of oldKeys should match the order or new jobs. Ie. oldKeys[0] is
// the old Key() of jobs[0]. This is so that any stdout/err of old jobs is
// associated with the new jobs.
func (db *db) modifyLiveJobs(oldKeys []string, jobs []*Job) error {
	encodedJobs, rgLookups, dgLookups, rdgLookups, rgs, _, _, _, err := db.prepareNewJobs(jobs, false)
	if err != nil {
		return err
	}
	sort.Sort(rgLookups)
	sort.Sort(rgs)
	sort.Sort(dgLookups)
	sort.Sort(rdgLookups)
	sort.Sort(encodedJobs)

	lookupBuckets := [][]byte{bucketRTK, bucketDTK, bucketRDTK}

	err = db.bolt.Batch(func(tx *bolt.Tx) error {
		// delete old jobs and their lookups
		newJobBucket := tx.Bucket(bucketJobsLive)
		bo := tx.Bucket(bucketStdO)
		be := tx.Bucket(bucketStdE)
		os := make([][]byte, len(oldKeys))
		es := make([][]byte, len(oldKeys))
		var hadStd bool
		for i, oldKey := range oldKeys {
			suffix := []byte(dbDelimiter + oldKey)
			for _, bucket := range lookupBuckets {
				b := tx.Bucket(bucket)
				// *** currently having to go through the the whole lookup
				// buckets; if this is a noticeable performance issue, will have
				// to implement a reverse lookup...
				errf := b.ForEach(func(k, v []byte) error {
					if bytes.HasSuffix(k, suffix) {
						errd := b.Delete(k)
						if errd != nil {
							return errd
						}
					}
					return nil
				})
				if errf != nil {
					return errf
				}
			}

			key := []byte(oldKey)
			errd := newJobBucket.Delete(key)
			if errd != nil {
				return errd
			}

			o := bo.Get(key)
			if o != nil {
				os[i] = o
				errd = bo.Delete(key)
				if errd != nil {
					return errd
				}
				hadStd = true
			}

			e := be.Get(key)
			if e != nil {
				es[i] = e
				errd = be.Delete(key)
				if errd != nil {
					return errd
				}
				hadStd = true
			}
		}

		if len(encodedJobs) > 0 {
			// now go ahead and store the new lookups and jobs
			errs := db.putLookups(tx, bucketRTK, rgLookups)
			if errs != nil {
				return errs
			}

			if len(rgs) > 0 {
				errs = db.putLookups(tx, bucketRGs, rgs)
				if errs != nil {
					return errs
				}
			}

			if len(dgLookups) > 0 {
				errs = db.putLookups(tx, bucketDTK, dgLookups)
				if errs != nil {
					return errs
				}
			}

			if len(rdgLookups) > 0 {
				errs = db.putLookups(tx, bucketRDTK, rdgLookups)
				if errs != nil {
					return errs
				}
			}

			if hadStd {
				for i, job := range jobs {
					if os[i] != nil {
						errs = bo.Put([]byte(job.Key()), os[i])
						if errs != nil {
							return errs
						}
					}
					if es[i] != nil {
						errs = be.Put([]byte(job.Key()), es[i])
						if errs != nil {
							return errs
						}
					}
				}
			}

			return db.putEncodedJobs(tx, bucketJobsLive, encodedJobs)
		}
		return nil
	})
	if err != nil {
		db.Error("Database error during modify", "err", err)
	}

	go db.backgroundBackup()

	return err
}

// retrieveJobStd gets the values that were stored using updateJobStd() for the
// given job.
func (db *db) retrieveJobStd(jobkey string) (stdo []byte, stde []byte) {
	// first wait for any existing updateJobAfterExit() calls to complete
	//*** this method of waiting seems really bad and should be improved, but in
	//    practice we probably never wait
	for {
		db.RLock()
		if db.updatingAfterJobExit == 0 {
			db.RUnlock()
			break
		}
		db.RUnlock()
		<-time.After(10 * time.Millisecond)
	}

	err := db.bolt.View(func(tx *bolt.Tx) error {
		bo := tx.Bucket(bucketStdO)
		be := tx.Bucket(bucketStdE)
		key := []byte(jobkey)
		o := bo.Get(key)
		if o != nil {
			stdo = make([]byte, len(o))
			copy(stdo, o)
		}
		e := be.Get(key)
		if e != nil {
			stde = make([]byte, len(e))
			copy(stde, e)
		}
		return nil
	})
	if err != nil {
		// impossible, but to keep the linter happy and incase things change in
		// the future
		db.Error("Database retrieve failed", "err", err)
	}
	return stdo, stde
}

// recommendedReqGroupMemory returns the 95th percentile peak memory usage of
// all jobs that previously ran with the given reqGroup. If there are too few
// prior values to calculate a 95th percentile, or if the 95th percentile is
// very close to the maximum value, returns the maximum value instead. In either
// case, the true value is rounded up to the nearest 100 MB. Returns 0 if there
// are no prior values.
func (db *db) recommendedReqGroupMemory(reqGroup string) (int, error) {
	return db.recommendedReqGroupStat(bucketJobRAM, reqGroup, RecMBRound)
}

// recommendedReqGroupDisk returns the 95th percentile peak disk usage of
// all jobs that previously ran with the given reqGroup. If there are too few
// prior values to calculate a 95th percentile, or if the 95th percentile is
// very close to the maximum value, returns the maximum value instead. In either
// case, the true value is rounded up to the nearest 100 MB. Returns 0 if there
// are no prior values.
func (db *db) recommendedReqGroupDisk(reqGroup string) (int, error) {
	return db.recommendedReqGroupStat(bucketJobDisk, reqGroup, RecMBRound)
}

// recommendReqGroupTime returns the 95th percentile wall time taken of all jobs
// that previously ran with the given reqGroup. If there are too few prior
// values to calculate a 95th percentile, or if the 95th percentile is very
// close to the maximum value, returns the maximum value instead. In either
// case, the true value is rounded up to the nearest second. Returns 0 if there
// are no prior values.
func (db *db) recommendedReqGroupTime(reqGroup string) (int, error) {
	return db.recommendedReqGroupStat(bucketJobSecs, reqGroup, RecSecRound)
}

// recommendedReqGroupStat is the implementation for the other recommend*()
// methods.
func (db *db) recommendedReqGroupStat(statBucket []byte, reqGroup string, roundAmount int) (int, error) {
	prefix := []byte(reqGroup)
	max := 0
	var recommendation int
	err := db.bolt.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(statBucket).Cursor()

		// we seek over the bucket, and to avoid having to do it twice (first to
		// get the overall count, then to get the 95th percentile), we keep the
		// previous 5%-sized window of values, updating recommendation as the
		// window fills
		count := 0
		window := jobStatWindowPercent
		var prev []int
		var erra error
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			max, erra = strconv.Atoi(string(v))
			if erra != nil {
				return erra
			}

			count++
			if count > 100 {
				window = (float32(count) / 100) * jobStatWindowPercent
			}

			prev = append(prev, max)
			if float32(len(prev)) > window {
				recommendation, prev = prev[0], prev[1:]
			}
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	if recommendation == 0 {
		if max == 0 {
			return recommendation, err
		}
		recommendation = max
	}

	if max-recommendation < roundAmount {
		recommendation = max
	}

	if recommendation < roundAmount {
		recommendation = roundAmount
	}

	if recommendation%roundAmount > 0 {
		recommendation = int(math.Ceil(float64(recommendation)/float64(roundAmount))) * roundAmount
	}

	return recommendation, err
}

// store does a basic set of a key/val in a given bucket
func (db *db) store(bucket []byte, key string, val []byte) error {
	err := db.bolt.Batch(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		err := b.Put([]byte(key), val)
		return err
	})
	return err
}

// retrieve does a basic get of a key from a given bucket. An error isn't
// possible here.
func (db *db) retrieve(bucket []byte, key string) []byte {
	var val []byte
	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		v := b.Get([]byte(key))
		if v != nil {
			val = make([]byte, len(v))
			copy(val, v)
		}
		return nil
	})
	if err != nil {
		// impossible, but to keep the linter happy and incase things change in
		// the future
		db.Error("Database retrieve failed", "err", err)
	}
	return val
}

// remove does a basic delete of a key from a given bucket. We don't care about
// errors here.
func (db *db) remove(bucket []byte, key string) {
	db.wgMutex.Lock()
	defer db.wgMutex.Unlock()
	wgk := db.wg.Add(1)
	go func() {
		defer internal.LogPanic(db.Logger, "jobqueue database remove", true)
		defer db.wg.Done(wgk)
		err := db.bolt.Batch(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucket)
			return b.Delete([]byte(key))
		})
		if err != nil {
			db.Error("Database remove failed", "err", err)
		}
	}()
}

// storeBatched stores items in the db in batches for efficiency. bucket is the
// name of the bucket to store in.
func (db *db) storeBatched(bucket []byte, data sobsd, storer sobsdStorer) error {
	// we want to add in batches of size data/10, minimum 1000, rounded to
	// the nearest 1000
	num := len(data)
	batchSize := num / 10
	rem := batchSize % 1000
	if rem > 500 {
		batchSize = batchSize - rem + 1000
	} else {
		batchSize -= rem
	}
	if batchSize < 1000 {
		batchSize = 1000
	}

	// based on https://github.com/boltdb/bolt/issues/337#issue-64861745
	if num < batchSize {
		return storer(bucket, data)
	}

	batches := num / batchSize
	offset := num - (num % batchSize)

	for i := 0; i < batches; i++ {
		err := storer(bucket, data[i*batchSize:(i+1)*batchSize])
		if err != nil {
			return err
		}
	}

	if offset != 0 {
		err := storer(bucket, data[offset:])
		if err != nil {
			return err
		}
	}
	return nil
}

// storeLookups is a sobsdStorer for storing Job.[somevalue]->Job.Key() lookups
// in the db.
func (db *db) storeLookups(bucket []byte, lookups sobsd) error {
	err := db.bolt.Batch(func(tx *bolt.Tx) error {
		return db.putLookups(tx, bucket, lookups)
	})
	return err
}

// putLookups does the work of storeLookups(). You must be inside a bolt
// transaction when calling this.
func (db *db) putLookups(tx *bolt.Tx, bucket []byte, lookups sobsd) error {
	lookup := tx.Bucket(bucket)
	for _, doublet := range lookups {
		err := lookup.Put(doublet[0], nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// storeEncodedJobs is a sobsdStorer for storing Jobs in the db.
func (db *db) storeEncodedJobs(bucket []byte, encodes sobsd) error {
	err := db.bolt.Batch(func(tx *bolt.Tx) error {
		return db.putEncodedJobs(tx, bucket, encodes)
	})
	return err
}

// putEncodedJobs does the work of storeEncodedJobs(). You nust be inside a bolt
// transaction when calling this.
func (db *db) putEncodedJobs(tx *bolt.Tx, bucket []byte, encodes sobsd) error {
	bjobs := tx.Bucket(bucket)
	for _, doublet := range encodes {
		err := bjobs.Put(doublet[0], doublet[1])
		if err != nil {
			return err
		}
	}
	return nil
}

// close shuts down the db, should be used prior to exiting. Ensures any
// ongoing backgroundBackup() completes first (but does not wait for backup() to
// complete).
func (db *db) close() error {
	db.Lock()
	defer db.Unlock()
	if !db.closed {
		db.closed = true

		// before actually closing, wait for any go routines doing database
		// transactions to complete
		if db.backingUp {
			db.backupFinal = true
			close(db.backupStopWait)
			db.Unlock()
			<-db.backupNotification
			db.wgMutex.Lock()
			db.wg.Wait(dbRunningTransactionsWaitTime)
			db.wgMutex.Unlock()
			db.Lock()
		} else {
			db.Unlock()
			db.wgMutex.Lock()
			db.wg.Wait(dbRunningTransactionsWaitTime)
			db.wgMutex.Unlock()
			db.Lock()
		}

		// do a final backup
		if db.backupsEnabled && db.backupQueued {
			db.Debug("Jobqueue database not backed up, will do final backup")
			db.backupToBackupFile(false)
		}

		err := db.bolt.Close()
		if db.backupMount != nil {
			erru := db.backupMount.Unmount()
			if erru != nil {
				if err == nil {
					err = erru
				} else {
					err = fmt.Errorf("%s (and unmounting backup failed: %s)", err.Error(), erru)
				}
			}
		}
		return err
	}
	return nil
}

// backgroundBackup backs up the database to a file (the location given during
// initDB()) in a goroutine, doing one backup at a time and queueing a further
// backup if any other backup requests come in while a backup is running. Any
// errors are silently ignored. Spaces out sequential backups so that there is a
// gap of max(30s, [time taken to complete previous backup]) seconds between
// them.
func (db *db) backgroundBackup() {
	db.Lock()
	defer db.Unlock()
	if db.closed || !db.backupsEnabled {
		return
	}
	if db.backingUp {
		db.backupQueued = true
		return
	}

	db.backingUp = true
	slowBackups := db.slowBackups
	go func(last time.Time, wait time.Duration, doNotWait bool) {
		defer internal.LogPanic(db.Logger, "backgroundBackup", true)

		if !doNotWait {
			now := time.Now()
			if !last.IsZero() && last.Add(wait).After(now) {
				// wait before doing another backup, so we don't slow down new
				// db accessses all the time
				select {
				case <-time.After(last.Add(wait).Sub(now)):
					break
				case <-db.backupStopWait:
					break
				}
			}
		}

		if slowBackups {
			// just for testing purposes
			<-time.After(100 * time.Millisecond)
		}

		start := time.Now()
		db.backupToBackupFile(slowBackups)

		db.Lock()
		db.backingUp = false
		db.backupLast = time.Now()
		duration := time.Since(start)
		if duration > minimumTimeBetweenBackups {
			db.backupWait = duration
		}

		if db.backupFinal {
			// close() has been called, don't do any more backups and tell
			// close() we finished our backup
			db.backupFinal = false
			db.backupStopWait = make(chan bool)
			db.Unlock()
			db.backupNotification <- true
			return
		}

		if db.backupQueued {
			db.backupQueued = false
			db.Unlock()
			db.backgroundBackup()
		} else {
			db.Unlock()
		}
	}(db.backupLast, db.backupWait, db.backupFinal)
}

// backupToBackupFile is used by backgroundBackup() and close() to do the actual
// backup.
func (db *db) backupToBackupFile(slowBackups bool) {
	// we most likely triggered this backup immediately following an operation
	// that alters (the important parts of) the database; wait for those
	// transactions to actually complete before backing up
	db.wgMutex.Lock()
	db.wg.Wait(dbRunningTransactionsWaitTime)

	wgk := db.wg.Add(1)
	db.wgMutex.Unlock()
	defer db.wg.Done(wgk)

	// create the new backup file with temp name
	tmpBackupPath := db.backupPathTmp

	err := db.bolt.View(func(tx *bolt.Tx) error {
		return tx.CopyFile(tmpBackupPath, dbFilePermission)
	})

	if slowBackups {
		<-time.After(100 * time.Millisecond)
	}

	if err != nil {
		db.Error("Database backup failed", "err", err)

		// if it failed, delete any partial file that got made
		errr := os.Remove(tmpBackupPath)
		if errr != nil && !os.IsNotExist(errr) {
			db.Warn("Removing bad database backup file failed", "path", tmpBackupPath, "err", errr)
		}
	} else {
		// backup succeeded
		if db.s3accessor != nil {
			// upload to s3 then delete it
			errr := db.s3accessor.UploadFile(tmpBackupPath, db.backupPath, "application/octet-stream")
			if errr != nil {
				db.Warn("Uploading new database backup file to S3 failed",
					"source", tmpBackupPath, "dest", db.backupPath, "err", errr)
			}

			errr = os.Remove(tmpBackupPath)

			if errr != nil {
				db.Warn("failed to delete temporary backup file after uploading to s3", "path", tmpBackupPath, "err", errr)
			}
		} else {
			// move it over any old backup
			errr := os.Rename(tmpBackupPath, db.backupPath)
			if errr != nil {
				db.Warn("Renaming new database backup file failed", "source", tmpBackupPath, "dest", db.backupPath, "err", errr)
			}
		}
	}
}

// backup backs up the database to the given writer. Can be called at the same
// time as an active backgroundBackup() or even another backup(). You will get
// a consistent view of the database at the time you call this. NB: this can be
// interrupted by calling db.close().
func (db *db) backup(w io.Writer) error {
	db.RLock()
	if db.closed {
		db.RUnlock()
		return fmt.Errorf("database closed")
	}
	db.RUnlock()

	return db.bolt.View(func(tx *bolt.Tx) error {
		_, txErr := tx.WriteTo(w)
		return txErr
	})
}

// stripBucketFromS3Path removes the first directory from the given path. If
// there are no directories, returns an error.
func stripBucketFromS3Path(path string) (string, error) {
	if idx := strings.IndexByte(path, '/'); idx >= 0 {
		return path[idx+1:], nil
	}

	return "", Error{Err: ErrS3DBBackupPath}
}
