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

// This file contains the implementation of Job behaviours.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/hashicorp/go-multierror"
)

// BehaviourTrigger is supplied to a Behaviour to define under what circumstance
// that Behaviour will trigger.
type BehaviourTrigger uint8

const (
	// OnExit is a BehaviourTrigger for Behaviours that should trigger when a
	// Job's Cmd is executed and finishes running. These behaviours will trigger
	// after OnSucess and OnFailure triggers, which makes OnExit different to
	// specifying OnSuccess|OnFailure.
	OnExit BehaviourTrigger = 1 << iota

	// OnSuccess is a BehaviourTrigger for Behaviours that should trigger when a
	// Job's Cmd is executed and exits 0.
	OnSuccess

	// OnFailure is a BehaviourTrigger for Behaviours that should trigger when a
	// Job's Cmd is executed and exits non-0.
	OnFailure
)

// BehaviourAction is supplied to a Behaviour to define what should happen when
// that behaviour triggers. (It's a uint8 type as opposed to an actual func to
// save space since we need to store these on every Job; do not treat as a flag
// and OR multiple actions together!)
type BehaviourAction uint8

const (
	// CleanupAll is a BehaviourAction that will delete any directories that
	// were created by a Job due to CwdMatters being false. Note that if the
	// Job's Cmd created output files within the actual cwd, these would get
	// deleted along with everything else. It takes no arguments.
	CleanupAll BehaviourAction = 1 << iota

	// Cleanup is a BehaviourAction that behaves exactly as CleanupAll in the
	// case that no output files have been specified on the Job. If some have,
	// everything except those files gets deleted. It takes no arguments.
	// (NB: since output file specification has not yet been implemented, this
	// is currently identical to CleanupAll.)
	Cleanup

	// Run is a BehaviourAction that runs a given command (supplied as a single
	// string Arg to the Behaviour) in the Job's actual cwd.
	Run

	// CopyToManager is a BehaviourAction that copies the given files (specified
	// as a slice of string paths Arg to the Behaviour) from the Job's actual
	// cwd to a configured location on the machine that the jobqueue server is
	// running on. *** not yet implemented!
	CopyToManager

	// Nothing is a BehaviourAction that does nothing. It allows you to define
	// a Behaviour that will do nothing, distinguishable from a nil Behaviour,
	// for situations where you want to store a desire to change another
	// Behaviour to turn it off.
	Nothing

	// Remove is a BehaviourAction that requests the Job is removed from the
	// queue after being buried. Useful when working with another workflow
	// management system that keeps track of jobs itself and may try to add
	// failed jobs again, in which case they mustn't be in the queue.
	//
	// Unlike other behaviours, the action doesn't occur when Trigger()ed, but
	// rather RemoveRequested() should be called after a Job is buried to ask if
	// it should be removed.
	Remove
)

// Behaviour describes something that should happen in response to a Job's Cmd
// exiting a certain way.
type Behaviour struct {
	When BehaviourTrigger
	Do   BehaviourAction
	Arg  interface{} // the arg needed by your chosen action
}

// Trigger will carry out our BehaviourAction if the supplied status matches our
// BehaviourTrigger.
func (b *Behaviour) Trigger(status BehaviourTrigger, j *Job) error {
	if b.When&status == 0 {
		return nil
	}

	switch b.Do {
	case CleanupAll:
		return b.cleanup(j, true)
	case Cleanup:
		return b.cleanup(j, false)
	case Run:
		return b.run(j)
	case CopyToManager:
		return b.copyToManager(j)
	case Remove, Nothing:
		return nil
	}
	return fmt.Errorf("invalid status %d", status)
}

// fillBVJM converts to a bvjMapping. Supply an empty or existing one and this
// will add to it.
func (b *Behaviour) fillBVJM(bvjm *bvjMapping) {
	var bvj BehaviourViaJSON
	switch b.Do {
	case Run:
		var arg string
		if cmd, wasStr := b.Arg.(string); wasStr {
			arg = cmd
		} else {
			arg = "!invalid!"
		}
		bvj = BehaviourViaJSON{Run: arg}
	case CopyToManager:
		var arg []string
		if files, wasStrSlice := b.Arg.([]string); wasStrSlice {
			arg = files
		} else {
			arg = []string{"!invalid!"}
		}
		bvj = BehaviourViaJSON{CopyToManager: arg}
	case Cleanup:
		bvj = BehaviourViaJSON{Cleanup: true}
	case CleanupAll:
		bvj = BehaviourViaJSON{CleanupAll: true}
	case Remove:
		bvj = BehaviourViaJSON{Remove: true}
	case Nothing:
		bvj = BehaviourViaJSON{Nothing: true}
	default:
		return
	}

	switch b.When {
	case OnFailure:
		bvjm.OnFailure = append(bvjm.OnFailure, bvj)
	case OnSuccess:
		bvjm.OnSuccess = append(bvjm.OnSuccess, bvj)
	case OnFailure | OnSuccess:
		bvjm.OnFS = append(bvjm.OnFS, bvj)
	case OnExit:
		bvjm.OnExit = append(bvjm.OnExit, bvj)
	default:
		return
	}
}

// String provides a nice string representation of a Behaviour for user
// interface display purposes. It is in the form of a JSON string that can be
// converted back to a Behaviour via a BehaviourViaJSON.
func (b *Behaviour) String() string {
	bvjm := &bvjMapping{}
	b.fillBVJM(bvjm)

	// because of automatic HTML escaping, we can't just use json.Marshal(bvjm)

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(bvjm)
	if err != nil {
		panic(fmt.Sprintf("Encoding a bvjm failed: %s", err))
	}

	return strings.TrimSpace(buffer.String())
}

// cleanup with all == true wipes out the Job's unique dir as aggressively as
// possible, along with all empty parent dirs up to Cwd. Without all, will keep
// files designated as outputs (*** designation not yet implemented).
func (b *Behaviour) cleanup(j *Job, all bool) error {
	if !all {
		// *** not yet implemented, we just wipe everything!
	}

	if j.ActualCwd == "" {
		// must be a CwdMatters job, or somehow ActualCwd didn't get set; we do
		// nothing in this case
		return nil
	}

	// it's the parent of ActualCwd that is the unique dir that got created
	// that should be deleted; it contains tmp, cwd and possibly mount cache
	// dirs (that we don't want to delete).
	workSpace := filepath.Dir(j.ActualCwd)

	if len(j.MountConfigs) > 0 {
		// if we have mounts, we don't want to delete the cache dirs or any
		// mounted directories, so we'll have to go through and delete
		// everything else manually
		var keepDirs []string
		var keepActualCwd bool
		for _, mc := range j.MountConfigs {
			if mc.Mount == "" {
				keepActualCwd = true
				break
			}
			if !filepath.IsAbs(mc.Mount) {
				keepDirs = append(keepDirs, mc.Mount)
			}
		}

		if !keepActualCwd {
			if len(keepDirs) > 0 {
				err := removeAllExcept(j.ActualCwd, keepDirs)
				if err != nil {
					return err
				}
			} else {
				err := os.RemoveAll(j.ActualCwd)
				if err != nil {
					return err
				}
			}
		}

		// finally delete everything inside workSpace except for cwd and the
		// cache dirs, incase a job.Cmd did something like `touch ../foo`
		entries, err := os.ReadDir(workSpace)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if entry.Name() != "cwd" && !strings.HasPrefix(entry.Name(), ".muxfys") {
				err = os.RemoveAll(filepath.Join(workSpace, entry.Name()))
				if err != nil {
					return err
				}
			}
		}
	} else {
		// just try and delete everything in one go
		err := os.RemoveAll(workSpace)
		if err != nil {
			return err
		}
	}

	// delete any empty parent directories up to Cwd
	return rmEmptyDirs(workSpace, j.Cwd)
}

// run simply runs the given command from Job's actual cwd.
func (b *Behaviour) run(j *Job) error {
	actualCwd := j.ActualCwd
	if actualCwd == "" {
		actualCwd = j.Cwd
	}

	bc, wasStr := b.Arg.(string)
	if !wasStr {
		return fmt.Errorf("arg %s is type %T, not string", b.Arg, b.Arg)
	}
	if strings.Contains(bc, " | ") {
		bc = "set -o pipefail; " + bc
	}
	// *** hardcoding bash here, when we could in theory have client.Execute()
	// pass shell in? And yes, we're allowing user to run absolutely any command
	// they like, but that is the very nature of this app. This runs as them,
	// so can do whatever they can do...
	cmd := exec.Command("/bin/bash", "-c", bc) // #nosec
	cmd.Dir = actualCwd
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("run behaviour failed: %s\n%s", err, string(out))
	}
	return err
}

// copyToManager copies the files specified in the Arg slice to the configured
// location on the manager's machine.
func (b *Behaviour) copyToManager(j *Job) error {
	_, wasStrSlice := b.Arg.([]string)
	if !wasStrSlice {
		return fmt.Errorf("arg %s is type %T, not []string", b.Arg, b.Arg)
	}

	// *** not yet implemented

	return nil
}

// Behaviours are a slice of Behaviour.
type Behaviours []*Behaviour

// Trigger calls Trigger on each constituent Behaviour, first all those for
// OnSuccess if success = true or OnFailure otherwise, then those for OnExit.
func (bs Behaviours) Trigger(success bool, j *Job) error {
	if len(bs) == 0 {
		return nil
	}

	var status BehaviourTrigger
	if success {
		status = OnSuccess
	} else {
		status = OnFailure
	}

	var merr *multierror.Error
	for _, b := range bs {
		err := b.Trigger(status, j)
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}

	status = OnExit
	for _, b := range bs {
		err := b.Trigger(status, j)
		if err != nil {
			merr = multierror.Append(merr, err)
		}
	}

	return merr.ErrorOrNil()
}

// RemovalRequested tells you if one of the behaviours is Remove.
func (bs Behaviours) RemovalRequested() bool {
	for _, b := range bs {
		if b.Do == Remove {
			return true
		}
	}

	return false
}

// String provides a nice string representation of Behaviours for user
// interface display purposes. It takes the form of a JSON string that can
// be converted back to Behaviours using a BehavioursViaJSON for each key. The
// keys are "on_failure", "on_success", "on_failure|success" and "on_exit".
func (bs Behaviours) String() string {
	if len(bs) == 0 {
		return ""
	}
	bvjm := &bvjMapping{}
	for _, b := range bs {
		b.fillBVJM(bvjm)
	}

	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(bvjm)
	if err != nil {
		panic(fmt.Sprintf("Encoding a bvjm failed: %s", err))
	}

	return strings.TrimSpace(buffer.String())
}

// BehaviourViaJSON makes up BehavioursViaJSON. Each of these should only
// specify one of its properties.
type BehaviourViaJSON struct {
	Run           string   `json:"run,omitempty"`
	CopyToManager []string `json:"copy_to_manager,omitempty"`
	Cleanup       bool     `json:"cleanup,omitempty"`
	CleanupAll    bool     `json:"cleanup_all,omitempty"`
	Remove        bool     `json:"remove,omitempty"`
	Nothing       bool     `json:"nothing,omitempty"`
}

// Behaviour converts the friendly BehaviourViaJSON struct to real Behaviour.
func (bj BehaviourViaJSON) Behaviour(when BehaviourTrigger) *Behaviour {
	var do BehaviourAction
	var arg interface{}

	switch {
	case bj.Run != "":
		do = Run
		arg = bj.Run
	case len(bj.CopyToManager) > 0:
		do = CopyToManager
		arg = bj.CopyToManager
	case bj.Cleanup:
		do = Cleanup
	case bj.CleanupAll:
		do = CleanupAll
	case bj.Remove:
		do = Remove
	default:
		do = Nothing
	}

	return &Behaviour{
		When: when,
		Do:   do,
		Arg:  arg,
	}
}

// BehavioursViaJSON is a slice of BehaviourViaJSON. It is a convenience to
// allow users to specify behaviours in a more natural way if they're trying to
// describe them in a JSON string. You'd have one of these per BehaviourTrigger.
type BehavioursViaJSON []BehaviourViaJSON

// Behaviours converts a BehavioursViaJSON to real Behaviours.
func (bjs BehavioursViaJSON) Behaviours(when BehaviourTrigger) Behaviours {
	bs := make(Behaviours, 0, len(bjs))
	for _, bj := range bjs {
		bs = append(bs, bj.Behaviour(when))
	}
	return bs
}

// bvjMapping struct is used by Behaviour*.String() to do its JSON conversion.
type bvjMapping struct {
	OnFailure BehavioursViaJSON `json:"on_failure,omitempty"`
	OnSuccess BehavioursViaJSON `json:"on_success,omitempty"`
	OnFS      BehavioursViaJSON `json:"on_failure|success,omitempty"`
	OnExit    BehavioursViaJSON `json:"on_exit,omitempty"`
}
