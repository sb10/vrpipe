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

package internal

// this file has general utility functions

import (
	"context"
	"crypto/md5" // #nosec not used for security purposes
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	infoblox "github.com/fanatic/go-infoblox"
	"github.com/shirou/gopsutil/mem"
	"github.com/wtsi-ssg/wr/clog"
)

// ZeroCoreMultiplier is the multipler of actual cores we use for the maximum of
// zero core jobs
const ZeroCoreMultiplier = 2

// for the RandomString implementation
const (
	randBytes   = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	randIdxBits = 6                  // 6 bits to represent a rand index
	randIdxMask = 1<<randIdxBits - 1 // All 1-bits, as many as letterIdxBits
	randIdxMax  = 63 / randIdxBits   // # of letter indices fitting in 63 bits
)

var CachedUsername string
var userid int

// SortMapKeysByIntValue sorts the keys of a map[string]int by its values,
// reversed if you supply true as the second arg.
func SortMapKeysByIntValue(imap map[string]int, reverse bool) []string {
	// from http://stackoverflow.com/a/18695428/675083 *** should also try the
	// idiomatic way to see if that's better in any way
	valToKeys := make(map[int][]string, len(imap))
	for key, val := range imap {
		valToKeys[val] = append(valToKeys[val], key)
	}
	vals := make([]int, 0, len(valToKeys))
	for val := range valToKeys {
		vals = append(vals, val)
	}

	if reverse {
		sort.Sort(sort.Reverse(sort.IntSlice(vals)))
	} else {
		sort.Sort(sort.IntSlice(vals))
	}

	sortedKeys := make([]string, 0, len(vals))
	for _, val := range vals {
		sortedKeys = append(sortedKeys, valToKeys[val]...)
	}
	return sortedKeys
}

// SortMapKeysByMapIntValue sorts the keys of a map[string]map[string]int by
// a the values found at a given sub value, reversed if you supply true as the
// second arg.
func SortMapKeysByMapIntValue(imap map[string]map[string]int, criterion string, reverse bool) []string {
	criterionValueToKeys := make(map[int][]string, len(imap))
	for key, submap := range imap {
		val := submap[criterion]
		criterionValueToKeys[val] = append(criterionValueToKeys[val], key)
	}
	criterionValues := make([]int, 0, len(criterionValueToKeys))
	for val := range criterionValueToKeys {
		criterionValues = append(criterionValues, val)
	}

	if reverse {
		sort.Sort(sort.Reverse(sort.IntSlice(criterionValues)))
	} else {
		sort.Sort(sort.IntSlice(criterionValues))
	}

	sortedKeys := make([]string, 0, len(criterionValues))
	for _, val := range criterionValues {
		sortedKeys = append(sortedKeys, criterionValueToKeys[val]...)
	}
	return sortedKeys
}

// DedupSortStrings removes duplicates and then sorts the given strings,
// returning a new slice.
func DedupSortStrings(s []string) []string {
	seen := make(map[string]struct{}, len(s))
	i := 0
	for _, v := range s {
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		s[i] = v
		i++
	}
	dedup := s[:i]
	sort.Strings(dedup)
	return dedup
}

// Username returns the username of the current user. This avoids problems
// with static compilation as it avoids the use of os/user. It will only work
// on linux-like systems where 'id -u -n' works.
func Username() (string, error) {
	if CachedUsername == "" {
		var err error
		CachedUsername, err = parseIDCmd("-u", "-n")
		if err != nil {
			return "", err
		}
	}
	return CachedUsername, nil
}

// Userid returns the user id of the current user. This avoids problems
// with static compilation as it avoids the use of os/user. It will only work
// on linux-like systems where 'id -u' works.
func Userid() (int, error) {
	if userid == 0 {
		uidStr, err := parseIDCmd("-u")
		if err != nil {
			return 0, err
		}
		userid, err = strconv.Atoi(uidStr)
		if err != nil {
			return 0, err
		}
	}
	return userid, nil
}

// parseIDCmd parses the output of the unix 'id' command.
func parseIDCmd(idopts ...string) (string, error) {
	idcmd := exec.Command("/usr/bin/id", idopts...) // #nosec
	idout, err := idcmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(string(idout), "\n"), err
}

// TildaToHome converts a path beginning with ~/ to the absolute path based in
// the current home directory. If that cannot be determined, path is returned
// unaltered.
func TildaToHome(path string) string {
	home, herr := os.UserHomeDir()
	if herr == nil && home != "" && strings.HasPrefix(path, "~/") {
		path = strings.TrimLeft(path, "~/")
		path = filepath.Join(home, path)
	}
	return path
}

// ProcMeminfoMBs uses gopsutil (amd64 freebsd, linux, windows, darwin, openbds
// only!) to find the total number of MBs of memory physically installed on the
// current system.
func ProcMeminfoMBs() (int, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}

	// convert bytes to MB
	return int((v.Total / 1024) / 1024), err
}

// LogClose is for use to Close() an object during a defer when you don't care
// if the Close() returns an error, but do want non-EOF errors logged. Extra
// args are passed as additional context for the logger.
func LogClose(ctx context.Context, obj io.Closer, msg string, extra ...interface{}) {
	err := obj.Close()
	if err != nil && err.Error() != "EOF" {
		extra = append(extra, "err", err)
		clog.Warn(ctx, "failed to close "+msg, extra...)
	}
}

// LogPanic is for use in a go routines, deferred at the start of them, to
// figure out what is causing runtime panics. If the die bool is true, the
// program exits, otherwise it continues, after logging the error message and
// stack trace. Desc string should be used to describe briefly what the
// goroutine you call this in does.
func LogPanic(ctx context.Context, desc string, die bool) {
	if err := recover(); err != nil {
		clog.Crit(ctx, desc+" panic", "err", err)
		if die {
			os.Exit(1)
		}
	}
}

// Which returns the full path to the executable with the given name that is
// found first in the set of $PATH directories, ignoring any path that is
// actually a symlink to ourselves.
func Which(exeName string) string {
	self, err := os.Executable()
	if err != nil {
		self = ""
	}
	self, err = filepath.EvalSymlinks(self)
	if err != nil {
		self = ""
	}

	for _, dir := range strings.Split(os.Getenv("PATH"), string(os.PathListSeparator)) {
		stat, err := os.Stat(dir)
		if err != nil || !stat.IsDir() {
			continue
		}

		exes, err := os.ReadDir(dir)
		if err != nil {
			continue
		}

		for _, exe := range exes {
			if exe.Name() != exeName {
				continue
			}
			path := filepath.Join(dir, exe.Name())

			// check that it's not a symlink to ourselves
			path, err := filepath.EvalSymlinks(path)
			if err != nil || path == self {
				continue
			}

			// check it's executable
			stat, err := os.Stat(path)
			if err == nil && (runtime.GOOS == "windows" || stat.Mode()&0111 != 0) {
				return path
			}
		}
	}

	return ""
}

// WaitForFile waits as long as timeout for the given file to exist. If the file
// has a timestamp from before the given after, however, waits until the file
// is touched to have a timestamp after after. When it exists with the right
// timestamp, returns true. Otherwise false.
func WaitForFile(file string, after time.Time, timeout time.Duration) bool {
	limit := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			info, err := os.Stat(file)
			if err == nil && info.ModTime().After(after) {
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

// InfobloxSetDomainIP uses infoblox to set the IP of a domain. Returns an error
// if INFOBLOX_HOST, INFOBLOX_USER or INFOBLOX_PASS env vars are not set.
func InfobloxSetDomainIP(domain, ip string) error {
	if domain == "localhost" {
		return fmt.Errorf("can't set domain IP when domain is configured as localhost")
	}

	// turn off logging built in to go-infoblox
	log.SetFlags(0)
	log.SetOutput(io.Discard)

	// check env vars are defined
	host := os.Getenv("INFOBLOX_HOST")
	if host == "" {
		return fmt.Errorf("INFOBLOX_HOST env var not set")
	}
	user := os.Getenv("INFOBLOX_USER")
	if user == "" {
		return fmt.Errorf("INFOBLOX_USER env var not set")
	}
	password := os.Getenv("INFOBLOX_PASS")
	if password == "" {
		return fmt.Errorf("INFOBLOX_PASS env var not set")
	}

	// create infoblox client
	ib := infoblox.NewClient("https://"+host+"/", user, password, true, false)

	// check if it's already set
	objs, err := ib.FindRecordA(domain)
	if err != nil {
		return fmt.Errorf("finding A records failed: %s", err)
	}

	if len(objs) == 1 {
		if objs[0].Ipv4Addr == ip {
			return nil
		}
	}

	// delete any existing entries
	for _, obj := range objs {
		err = ib.NetworkObject(obj.Ref).Delete(nil)
		if err != nil {
			return fmt.Errorf("delete of A record failed: %s", err)
		}
	}

	// now add an A record for domain pointing to ip
	d := url.Values{}
	d.Set("ipv4addr", ip)
	d.Set("name", domain)
	_, err = ib.RecordA().Create(d, nil, nil)
	if err != nil {
		return fmt.Errorf("create of A record failed: %s", err)
	}

	// wait a while for things to "really" work
	<-time.After(500 * time.Millisecond)

	return nil
}

// FileMD5 calculates the MD5 hash checksum of a file, returned as HEX encoded.
func FileMD5(ctx context.Context, path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer LogClose(ctx, file, "fileMD5", "path", path)

	h := md5.New() // #nosec not used for security purposes

	if _, err := io.Copy(h, file); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// RandomString generates a random string of length 8 characters.
func RandomString() string {
	// based on http://stackoverflow.com/a/31832326/675083
	b := make([]byte, 8)
	src := rand.NewSource(time.Now().UnixNano())
	for i, cache, remain := 7, src.Int63(), randIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), randIdxMax
		}
		if idx := int(cache & randIdxMask); idx < len(randBytes) {
			b[i] = randBytes[idx]
			i--
		}
		cache >>= randIdxBits
		remain--
	}
	return string(b)
}

// CurrentIP returns the IP address of the machine we're running on right now.
// The cidr argument can be an empty string, but if set to the CIDR of the
// machine's primary network, it helps us be sure of getting the correct IP
// address (for when there are multiple network interfaces on the machine).
func CurrentIP(cidr string) (string, error) {
	var ipNet *net.IPNet
	if cidr != "" {
		_, ipn, err := net.ParseCIDR(cidr)
		if err == nil {
			ipNet = ipn
		}
		// *** ignoring error since I don't want to change the return value of
		// this method...
	}

	conn, err := net.Dial("udp", "8.8.8.8:80") // doesn't actually connect, dest doesn't need to exist
	if err != nil {
		// fall-back on the old method we had...

		// first just hope http://stackoverflow.com/a/25851186/675083 gives us a
		// cross-linux&MacOS solution that works reliably...
		var out []byte
		out, err = exec.Command("sh", "-c", "ip -4 route get 8.8.8.8 | head -1 | cut -d' ' -f8 | tr -d '\\n'").Output() // #nosec
		var ip string
		if err != nil {
			ip = string(out)

			// paranoid confirmation this ip is in our CIDR
			if ip != "" && ipNet != nil {
				pip := net.ParseIP(ip)
				if pip != nil {
					if !ipNet.Contains(pip) {
						ip = ""
					}
				}
			}
		}

		// if the above fails, fall back on manually going through all our
		// network interfaces
		if ip == "" {
			ip, err = currentIPFallback(ipNet)
		}

		return ip, nil
	}

	defer func() {
		err = conn.Close()
	}()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	ip := localAddr.IP

	// paranoid confirmation this ip is in our CIDR
	if ipNet != nil {
		if ipNet.Contains(ip) {
			return ip.String(), err
		}
	} else {
		return ip.String(), err
	}

	return currentIPFallback(ipNet)
}

// currentIPFallback is an older fallback method for figuring out our IP
// address by going through all our network interfaces.
func currentIPFallback(ipNet *net.IPNet) (string, error) {
	var addrs []net.Addr
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	var ip string
	for _, address := range addrs {
		if thisIPNet, ok := address.(*net.IPNet); ok && !thisIPNet.IP.IsLoopback() {
			if thisIPNet.IP.To4() != nil {
				if ipNet != nil {
					if ipNet.Contains(thisIPNet.IP) {
						ip = thisIPNet.IP.String()
						break
					}
				} else {
					ip = thisIPNet.IP.String()
					break
				}
			}
		}
	}
	return ip, nil
}

// PathToContent takes the path to a file and returns its contents as a string.
// If path begins with a tilda, TildaToHome() is used to first convert the path
// to an absolute path, in order to find the file.
func PathToContent(path string) (string, error) {
	absPath := TildaToHome(path)
	contents, err := os.ReadFile(absPath)
	if err != nil {
		return "", fmt.Errorf("path [%s] could not be read: %s", absPath, err)
	}
	return string(contents), nil
}
