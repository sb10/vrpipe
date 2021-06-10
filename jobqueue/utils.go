// Copyright © 2016-2018 Genome Research Limited
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

// This file contains some general utility functions for use by client and
// server.

import (
	"bufio"
	"bytes"
	"compress/zlib"
	crand "crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/internal"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/dgryski/go-farm"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/shirou/gopsutil/process"
)

// AppName gets used in certain places like naming the base directory of created
// working directories during Client.Execute().
var AppName = "jobqueue"

// mkHashedLevels is the number of directory levels we create in mkHashedDirs
const mkHashedLevels = 4

// tokenLength is the fixed size of our authentication token
const tokenLength = 43

const reqSchedSpecialRAM = 924
const reqSchedExtraRAM = 100
const reqSchedTimeRound = 30 * time.Minute

var pss = []byte("Pss:")

// cr, lf and ellipses get used by stdFilter()
var cr = []byte("\r")
var lf = []byte("\n")
var ellipses = []byte("[...]\n")

// generateToken creates a cryptographically secure pseudorandom URL-safe base64
// encoded string 43 bytes long. Used by the server to create a token passed to
// to the caller for subsequent client authentication. If the given file exists
// and contains a single 43 byte string, then that is used as the token instead.
func generateToken(tokenFile string) ([]byte, error) {
	if token, err := os.ReadFile(tokenFile); err == nil && len(token) == tokenLength {
		return token, nil
	}

	b := make([]byte, 32)
	_, err := crand.Read(b)
	if err != nil {
		return nil, err
	}
	token := make([]byte, tokenLength)
	base64.URLEncoding.WithPadding(base64.NoPadding).Encode(token, b)
	return token, err
}

// tokenMatches compares a token supplied by a client with a server token (eg.
// generated by generateToken()) and tells you if they match. Does so in a
// cryptographically secure way (avoiding timing attacks).
func tokenMatches(input, expected []byte) bool {
	result := subtle.ConstantTimeCompare(input, expected)
	return result == 1
}

// byteKey calculates a unique key that describes a byte slice.
func byteKey(b []byte) string {
	l, h := farm.Hash128(b)
	return fmt.Sprintf("%016x%016x", l, h)
}

// copy a file *** should be updated to handle source being on a different
// machine or in an S3-style object store.
func copyFile(source string, dest string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		errc := in.Close()
		if errc != nil {
			if err == nil {
				err = errc
			} else {
				err = fmt.Errorf("%s (and closing source failed: %s)", err.Error(), errc)
			}
		}
	}()
	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer func() {
		errc := out.Close()
		if errc != nil {
			if err == nil {
				err = errc
			} else {
				err = fmt.Errorf("%s (and closing dest failed: %s)", err.Error(), errc)
			}
		}
	}()
	_, err = io.Copy(out, in)
	return err
}

// compress uses zlib to compress stuff, for transferring big stuff like
// stdout, stderr and environment variables over the network, and for storing
// of same on disk.
func compress(data []byte) ([]byte, error) {
	var compressed bytes.Buffer
	w, err := zlib.NewWriterLevel(&compressed, zlib.BestCompression)
	if err != nil {
		return nil, err
	}
	_, err = w.Write(data)
	if err != nil {
		return nil, err
	}
	err = w.Close()
	if err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

// decompress uses zlib to decompress stuff compressed by compress().
func decompress(compressed []byte) ([]byte, error) {
	b := bytes.NewReader(compressed)
	r, err := zlib.NewReader(b)
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), err
}

// get the current memory usage of a pid and all its children, relying on modern
// linux /proc/*/smaps (based on http://stackoverflow.com/a/31881979/675083).
func currentMemory(pid int) (int, error) {
	f, err := os.Open(fmt.Sprintf("/proc/%d/smaps", pid))
	if err != nil {
		return 0, err
	}
	defer func() {
		errc := f.Close()
		if errc != nil {
			if err == nil {
				err = errc
			} else {
				err = fmt.Errorf("%s (and closing smaps failed: %s)", err.Error(), errc)
			}
		}
	}()

	kb := uint64(0)
	r := bufio.NewScanner(f)
	for r.Scan() {
		line := r.Bytes()
		if bytes.HasPrefix(line, pss) {
			var size uint64
			_, err = fmt.Sscanf(string(line[4:]), "%d", &size)
			if err != nil {
				return 0, err
			}
			kb += size
		}
	}
	if err = r.Err(); err != nil {
		return 0, err
	}

	// convert kB to MB
	mem := int(kb / 1024)

	// recurse for children
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return mem, err
	}
	children, err := p.Children()
	if err != nil && err.Error() != "process does not have children" { // err != process.ErrorNoChildren
		return mem, err
	}
	for _, child := range children {
		childMem, errr := currentMemory(int(child.Pid))
		if errr != nil {
			continue
		}
		mem += childMem
	}

	return mem, nil
}

// get the current disk usage within a directory, in MBs. Optionally, provide a
// map of absolute paths to dirs (within path) that should not be checked.
func currentDisk(path string, ignore ...map[string]bool) (int64, error) {
	var disk int64

	skip := make(map[string]bool)
	if len(ignore) == 1 && len(ignore[0]) > 0 {
		skip = ignore[0]
	}

	dir, err := os.Open(path)
	if err != nil {
		return disk, err
	}
	defer func() {
		err = dir.Close()
	}()

	files, err := dir.Readdir(-1)
	if err != nil {
		return disk, err
	}

	for _, file := range files {
		if file.IsDir() {
			abs := filepath.Join(path, file.Name())
			if skip[abs] {
				continue
			}
			recurse, errr := currentDisk(abs, ignore...)
			if errr != nil {
				return disk, errr
			}
			disk += recurse
		} else {
			disk += file.Size() / (1024 * 1024)
		}
	}

	return disk, err
}

// getChildProcesses gets the child processes of the given pid, recursively.
func getChildProcesses(pid int32) ([]*process.Process, error) {
	var children []*process.Process
	p, err := process.NewProcess(pid)
	if err != nil {
		// we ignore errors, since we allow for working on processes that we're in
		// the process of killing
		return children, nil
	}

	children, err = p.Children()
	if err != nil && err.Error() != "process does not have children" {
		return children, err
	}

	for _, child := range children {
		theseKids, errk := getChildProcesses(child.Pid)
		if errk != nil {
			continue
		}
		if len(theseKids) > 0 {
			children = append(children, theseKids...)
		}
	}

	return children, nil
}

// this prefixSuffixSaver-related code is taken from os/exec, since they are not
// exported. prefixSuffixSaver is an io.Writer which retains the first N bytes
// and the last N bytes written to it. The Bytes() methods reconstructs it with
// a pretty error message.
type prefixSuffixSaver struct {
	N         int
	prefix    []byte
	suffix    []byte
	suffixOff int
	skipped   int64
}

func (w *prefixSuffixSaver) Write(p []byte) (int, error) {
	lenp := len(p)
	p = w.fill(&w.prefix, p)
	if overage := len(p) - w.N; overage > 0 {
		p = p[overage:]
		w.skipped += int64(overage)
	}
	p = w.fill(&w.suffix, p)
	for len(p) > 0 { // 0, 1, or 2 iterations.
		n := copy(w.suffix[w.suffixOff:], p)
		p = p[n:]
		w.skipped += int64(n)
		w.suffixOff += n
		if w.suffixOff == w.N {
			w.suffixOff = 0
		}
	}
	return lenp, nil
}
func (w *prefixSuffixSaver) fill(dst *[]byte, p []byte) []byte {
	if remain := w.N - len(*dst); remain > 0 {
		add := minInt(len(p), remain)
		*dst = append(*dst, p[:add]...)
		p = p[add:]
	}
	return p
}
func (w *prefixSuffixSaver) Bytes() []byte {
	if w.suffix == nil {
		return w.prefix
	}
	if w.skipped == 0 {
		return append(w.prefix, w.suffix...)
	}
	var buf bytes.Buffer
	buf.Grow(len(w.prefix) + len(w.suffix) + 50)
	buf.Write(w.prefix)
	buf.WriteString("\n... omitting ")
	buf.WriteString(strconv.FormatInt(w.skipped, 10))
	buf.WriteString(" bytes ...\n")
	buf.Write(w.suffix[w.suffixOff:])
	buf.Write(w.suffix[:w.suffixOff])
	return buf.Bytes()
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// stdFilter keeps only the first and last line of any contiguous block of \r
// terminated lines (to mostly eliminate progress bars), intended for use with
// stdout/err streaming input, outputting to a prefixSuffixSaver. Because you
// must finish reading from the input before continuing, it returns a channel
// that you should wait to receive an error from (nil if everything workd).
func stdFilter(std io.Reader, out io.Writer) chan error {
	reader := bufio.NewReader(std)
	done := make(chan error)
	go func() {
		var merr *multierror.Error
		for {
			p, err := reader.ReadBytes('\n')

			lines := bytes.Split(p, cr)
			_, errw := out.Write(lines[0])
			if errw != nil {
				merr = multierror.Append(merr, errw)
			}
			if len(lines) > 2 {
				_, errw = out.Write(lf)
				if errw != nil {
					merr = multierror.Append(merr, errw)
				}
				if len(lines) > 3 {
					_, errw = out.Write(ellipses)
					if errw != nil {
						merr = multierror.Append(merr, errw)
					}
				}
				_, errw = out.Write(lines[len(lines)-2])
				if errw != nil {
					merr = multierror.Append(merr, errw)
				}
				_, errw = out.Write(lf)
				if errw != nil {
					merr = multierror.Append(merr, errw)
				}
			}

			if err != nil {
				break
			}
		}
		done <- merr.ErrorOrNil()
	}()
	return done
}

// envOverride deals with values you get from os.Environ, overriding one set
// with values from another. Returns the new slice of environment variables.
func envOverride(orig []string, over []string) []string {
	override := make(map[string]string)
	for _, envvar := range over {
		pair := strings.Split(envvar, "=")
		override[pair[0]] = envvar
	}

	env := orig
	for i, envvar := range env {
		pair := strings.Split(envvar, "=")
		if replace, do := override[pair[0]]; do {
			env[i] = replace
			delete(override, pair[0])
		}
	}

	for _, envvar := range override {
		env = append(env, envvar)
	}
	return env
}

// calculateHashedDir returns the hashed directory structure corresponding to
// a given string. Returns dirs rooted at baseDir, and a leaf name.
func calculateHashedDir(baseDir, tohash string) (string, string) {
	dirs := strings.SplitN(tohash, "", mkHashedLevels)
	dirs, leaf := dirs[0:mkHashedLevels-1], dirs[mkHashedLevels-1]
	dirs = append([]string{baseDir}, dirs...)
	return filepath.Join(dirs...), leaf
}

// mkHashedDir uses tohash (which should be a 32 char long string from
// byteKey()) to create a folder nested within baseDir, and in that folder
// creates 2 folders called cwd and tmp, which it returns. Returns an error if
// there were problems making the directories.
func mkHashedDir(baseDir, tohash string) (cwd, tmpDir string, err error) {
	dir, leaf := calculateHashedDir(filepath.Join(baseDir, AppName+"_cwd"), tohash)
	holdFile := filepath.Join(dir, ".hold")
	defer func() {
		errr := os.Remove(holdFile)
		if errr != nil && !os.IsNotExist(errr) {
			if err == nil {
				err = errr
			} else {
				err = fmt.Errorf("%s (and removing the hold file failed: %s)", err.Error(), errr)
			}
		}
	}()
	tries := 0
	for {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			tries++
			if tries <= 3 {
				// we retry a few times in case another process is calling
				// rmEmptyDirs on the same baseDir and so conflicting with us
				continue
			}
			return cwd, tmpDir, err
		}

		// and drop a temp file in here so rmEmptyDirs will not immediately
		// remove these dirs
		tries = 0
		var f *os.File
		f, err = os.OpenFile(holdFile, os.O_RDONLY|os.O_CREATE, 0600)
		if err != nil {
			tries++
			if tries <= 3 {
				continue
			}
			return cwd, tmpDir, err
		}
		err = f.Close()
		if err != nil {
			return cwd, tmpDir, err
		}

		break
	}

	// if tohash is a job key then we expect that only 1 of that job is
	// running at any one time per jobqueue, but there could be multiple users
	// running the same cmd, or this user could be running the same command in
	// multiple queues, so we must still create a unique dir at the leaf of our
	// hashed dir structure, to avoid any conflict of multiple processes using
	// the same working directory
	dir, err = os.MkdirTemp(dir, leaf)
	if err != nil {
		return cwd, tmpDir, err
	}

	cwd = filepath.Join(dir, "cwd")
	err = os.Mkdir(cwd, os.ModePerm)
	if err != nil {
		return cwd, tmpDir, err
	}

	tmpDir = filepath.Join(dir, "tmp")
	return cwd, tmpDir, os.Mkdir(tmpDir, os.ModePerm)
}

// rmEmptyDirs deletes leafDir and it's parent directories if they are empty,
// stopping if it reaches baseDir (leaving that undeleted). It's ok if leafDir
// doesn't exist.
func rmEmptyDirs(leafDir, baseDir string) error {
	err := os.Remove(leafDir)
	if err != nil && !os.IsNotExist(err) {
		if strings.Contains(err.Error(), "directory not empty") { //*** not sure where this string comes; probably not cross platform!
			return nil
		}
		return err
	}
	current := leafDir
	parent := filepath.Dir(current)
	for ; parent != baseDir; parent = filepath.Dir(current) {
		thisErr := os.Remove(parent)
		if thisErr != nil {
			// it's expected that we might not be able to delete parents, since
			// some other Job may be running from the same Cwd, meaning this
			// parent dir is not empty
			break
		}
		current = parent
	}
	return nil
}

// removeAllExcept deletes the contents of a given directory (absolute path),
// except for the given folders (relative paths).
func removeAllExcept(path string, exceptions []string) error {
	keepDirs := make(map[string]bool)
	checkDirs := make(map[string]bool)
	path = filepath.Clean(path)
	for _, dir := range exceptions {
		abs := filepath.Join(path, dir)
		keepDirs[abs] = true
		parent := filepath.Dir(abs)
		for {
			if parent == path {
				break
			}
			checkDirs[parent] = true
			parent = filepath.Dir(parent)
		}
	}

	return removeWithExceptions(path, keepDirs, checkDirs)
}

// removeWithExceptions is the recursive part of removeAllExcept's
// implementation that does the real work of deleting stuff.
func removeWithExceptions(path string, keepDirs map[string]bool, checkDirs map[string]bool) error {
	entries, errr := os.ReadDir(path)
	if errr != nil {
		return errr
	}
	for _, entry := range entries {
		abs := filepath.Join(path, entry.Name())
		if !entry.IsDir() {
			err := os.Remove(abs)
			if err != nil {
				return err
			}
			continue
		}

		if keepDirs[abs] {
			continue
		}

		if checkDirs[abs] {
			err := removeWithExceptions(abs, keepDirs, checkDirs)
			if err != nil {
				return err
			}
		} else {
			err := os.RemoveAll(abs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// compressFile reads the content of the given file then compresses that. Since
// this happens in memory, only suitable for small files!
func compressFile(path string) ([]byte, error) {
	path = internal.TildaToHome(path)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return compress(content)
}

// reqForScheduler takes a job's Requirements and returns a possibly modified
// version if using less than 924MB memory to have +100MB memory to allow some
// leeway in case the job scheduler calculates used memory differently, and for
// other memory usage vagaries. It also rounds up the Time to the nearest half
// hour.
func reqForScheduler(req *scheduler.Requirements) *scheduler.Requirements {
	ram := req.RAM
	if ram < reqSchedSpecialRAM {
		ram += reqSchedExtraRAM
	}

	d := req.Time.Round(reqSchedTimeRound)
	if d < req.Time {
		d += reqSchedTimeRound
	}

	return &scheduler.Requirements{
		RAM:   ram,
		Time:  d,
		Cores: req.Cores,
		Disk:  req.Disk,
		Other: req.Other,
	}
}
