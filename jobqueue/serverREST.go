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

// This file contains the REST API code of the server. It is not used
// internally, but provides 3rd party non-go clients the ability to interact
// with the job queue using JSON over HTTP.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/VertebrateResequencing/wr/internal"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wr/clog"
)

const (
	restAPIVersion         = "1"
	restVersionEndpoint    = "/rest/version/"
	restJobsEndpoint       = "/rest/v" + restAPIVersion + "/jobs/"
	restWarningsEndpoint   = "/rest/v" + restAPIVersion + "/warnings/"
	restBadServersEndpoint = "/rest/v" + restAPIVersion + "/servers/"
	restFileUploadEndpoint = "/rest/v" + restAPIVersion + "/upload/"
	restInfoEndpoint       = "/rest/v" + restAPIVersion + "/info/"
	restFormTrue           = "true"
	bearerSchema           = "Bearer "
)

// JobViaJSON describes the properties of a JOB that a user wishes to add to the
// queue, convenient if they are supplying JSON.
type JobViaJSON struct {
	MountConfigs MountConfigs      `json:"mounts"`
	LimitGrps    []string          `json:"limit_grps"`
	DepGrps      []string          `json:"dep_grps"`
	Deps         []string          `json:"deps"`
	CmdDeps      Dependencies      `json:"cmd_deps"`
	OnFailure    BehavioursViaJSON `json:"on_failure"`
	OnSuccess    BehavioursViaJSON `json:"on_success"`
	OnExit       BehavioursViaJSON `json:"on_exit"`
	Env          []string          `json:"env"`
	Cmd          string            `json:"cmd"`
	Cwd          string            `json:"cwd"`
	ReqGrp       string            `json:"req_grp"`
	// Memory is a number and unit suffix, eg. 1G for 1 Gigabyte.
	Memory string `json:"memory"`
	// Time is a duration with a unit suffix, eg. 1h for 1 hour.
	Time             string   `json:"time"`
	RepGrp           string   `json:"rep_grp"`
	MonitorDocker    string   `json:"monitor_docker"`
	CloudOS          string   `json:"cloud_os"`
	CloudUser        string   `json:"cloud_username"`
	CloudScript      string   `json:"cloud_script"`
	CloudConfigFiles string   `json:"cloud_config_files"`
	CloudFlavor      string   `json:"cloud_flavor"`
	SchedulerQueue   string   `json:"queue"`
	SchedulerMisc    string   `json:"misc"`
	BsubMode         string   `json:"bsub_mode"`
	CPUs             *float64 `json:"cpus"`
	// Disk is the number of Gigabytes the cmd will use.
	Disk        *int `json:"disk"`
	Override    *int `json:"override"`
	Priority    *int `json:"priority"`
	Retries     *int `json:"retries"`
	CloudOSRam  *int `json:"cloud_ram"`
	RTimeout    *int `json:"reserve_timeout"`
	CwdMatters  bool `json:"cwd_matters"`
	ChangeHome  bool `json:"change_home"`
	CloudShared bool `json:"cloud_shared"`
}

// JobDefaults is supplied to JobViaJSON.Convert() to provide default values for
// the conversion.
type JobDefaults struct {
	LimitGroups   []string
	DepGroups     []string
	Deps          Dependencies
	OnFailure     Behaviours
	OnSuccess     Behaviours
	OnExit        Behaviours
	MountConfigs  MountConfigs
	compressedEnv []byte
	RepGrp        string
	// Cwd defaults to /tmp.
	Cwd    string
	ReqGrp string
	// Env is a comma separated list of key=val pairs.
	Env           string
	MonitorDocker string
	CloudOS       string
	CloudUser     string
	CloudFlavor   string
	// CloudScript is the local path to a script.
	CloudScript string
	// CloudConfigFiles is the config files to copy in cloud.Server.CopyOver() format
	CloudConfigFiles string
	SchedulerQueue   string
	SchedulerMisc    string
	BsubMode         string
	osRAM            string
	// CPUs is the number of CPU cores each cmd will use.
	CPUs   float64 // Memory is the number of Megabytes each cmd will use. Defaults to 1000.
	Memory int
	// Time is the amount of time each cmd will run for. Defaults to 1 hour.
	Time time.Duration
	// Disk is the number of Gigabytes cmds will use.
	Disk     int
	Override int
	Priority int
	Retries  int
	// CloudOSRam is the number of Megabytes that CloudOS needs to run. Defaults
	// to 1000.
	CloudOSRam int
	RTimeout   int
	CwdMatters bool
	ChangeHome bool
	// DiskSet is used to distinguish between Disk not being provided, and
	// being provided with a value of 0 or more.
	DiskSet     bool
	CloudShared bool
}

// DefaultCwd returns the Cwd value, defaulting to /tmp.
func (jd *JobDefaults) DefaultCwd() string {
	if jd.Cwd == "" {
		return "/tmp"
	}
	return jd.Cwd
}

// DefaultCPUs returns the CPUs value, but a minimum of 0.
func (jd *JobDefaults) DefaultCPUs() float64 {
	if jd.CPUs < 0 {
		return 0
	}
	return jd.CPUs
}

// DefaultMemory returns the Memory value, but if <1 returns 1000 instead.
func (jd *JobDefaults) DefaultMemory() int {
	if jd.Memory < 1 {
		return 1000
	}
	return jd.Memory
}

// DefaultTime returns the Time value, but if 0 returns 1 hour instead.
func (jd *JobDefaults) DefaultTime() time.Duration {
	if jd.Time == 0 {
		return 1 * time.Hour
	}
	return jd.Time
}

// DefaultEnv returns an encoded compressed version of the Env value.
func (jd *JobDefaults) DefaultEnv() ([]byte, error) {
	var err error
	if len(jd.compressedEnv) == 0 {
		jd.compressedEnv, err = compressEnv(strings.Split(jd.Env, ","))
	}
	return jd.compressedEnv, err
}

// DefaultCloudOSRam returns a string version of the CloudOSRam value, which is
// treated as 1000 if 0.
func (jd *JobDefaults) DefaultCloudOSRam() string {
	if jd.osRAM == "" {
		ram := jd.CloudOSRam
		if ram == 0 {
			ram = 1000
		}
		jd.osRAM = strconv.Itoa(ram)
	}
	return jd.osRAM
}

// Convert considers the supplied defaults and returns a *Job based on the
// properties of this JobViaJSON. The Job will not be in the queue until passed
// to a method that adds jobs to the queue.
func (jvj *JobViaJSON) Convert(jd *JobDefaults) (*Job, error) {
	var cmd, cwd, rg, repg, monitorDocker string
	var mb, disk, override, priority, retries int
	var diskSet bool
	var cpus float64
	var dur time.Duration
	var envOverride []byte
	var limitGroups, depGroups []string
	var deps Dependencies
	var behaviours Behaviours
	var mounts MountConfigs
	var bsubMode string

	if jvj.RepGrp == "" {
		repg = jd.RepGrp
	} else {
		repg = jvj.RepGrp
	}

	cmd = jvj.Cmd
	if cmd == "" {
		return nil, fmt.Errorf("cmd was not specified")
	}

	if jvj.Cwd == "" {
		cwd = jd.DefaultCwd()
	} else {
		cwd = jvj.Cwd
	}

	cwdMatters := jd.CwdMatters
	if jvj.CwdMatters {
		cwdMatters = true
	}

	changeHome := jd.ChangeHome
	if jvj.ChangeHome {
		changeHome = true
	}

	if jvj.ReqGrp == "" {
		if jd.ReqGrp != "" {
			rg = jd.ReqGrp
		} else {
			parts := strings.Split(cmd, " ")
			rg = filepath.Base(parts[0])
		}
	} else {
		rg = jvj.ReqGrp
	}

	if jvj.CPUs == nil {
		cpus = jd.DefaultCPUs()
	} else {
		cpus = *jvj.CPUs
	}

	if jvj.Memory == "" {
		mb = jd.DefaultMemory()
	} else {
		thismb, err := bytefmt.ToMegabytes(jvj.Memory)
		if err != nil {
			return nil, fmt.Errorf("memory value (%s) was not specified correctly: %s", jvj.Memory, err)
		}
		mb = int(thismb)
	}

	if jvj.Time == "" {
		dur = jd.DefaultTime()
	} else {
		var err error
		dur, err = time.ParseDuration(jvj.Time)
		if err != nil {
			return nil, fmt.Errorf("time value (%s) was not specified correctly: %s", jvj.Time, err)
		}
	}

	if jvj.Override == nil {
		override = jd.Override
	} else {
		override = *jvj.Override
	}
	if override < 0 || override > 2 {
		return nil, fmt.Errorf("override value (%d) is not in the range 0..2", override)
	}

	if jvj.Disk == nil {
		disk = jd.Disk
		diskSet = jd.DiskSet
	} else {
		disk = *jvj.Disk
		diskSet = true
	}

	if jvj.Priority == nil {
		priority = jd.Priority
	} else {
		priority = *jvj.Priority
	}
	if priority < 0 || priority > 255 {
		return nil, fmt.Errorf("priority value (%d) is not in the range 0..255", priority)
	}

	if jvj.Retries == nil {
		retries = jd.Retries
	} else {
		retries = *jvj.Retries
	}
	if retries < 0 || retries > 255 {
		return nil, fmt.Errorf("retries value (%d) is not in the range 0..255", retries)
	}

	if len(jvj.LimitGrps) == 0 {
		limitGroups = jd.LimitGroups
	} else {
		limitGroups = jvj.LimitGrps
	}

	if len(jvj.DepGrps) == 0 {
		depGroups = jd.DepGroups
	} else {
		depGroups = jvj.DepGrps
	}

	if len(jvj.Deps) == 0 && len(jvj.CmdDeps) == 0 {
		deps = jd.Deps
	} else {
		if len(jvj.CmdDeps) > 0 {
			deps = jvj.CmdDeps
		}
		if len(jvj.Deps) > 0 {
			for _, depgroup := range jvj.Deps {
				deps = append(deps, NewDepGroupDependency(depgroup))
			}
		}
	}

	if len(jvj.Env) > 0 {
		var err error
		envOverride, err = compressEnv(jvj.Env)
		if err != nil {
			return nil, err
		}
	} else if len(jd.Env) > 0 {
		var err error
		envOverride, err = jd.DefaultEnv()
		if err != nil {
			return nil, err
		}
	}

	if len(jvj.OnFailure) > 0 {
		behaviours = append(behaviours, jvj.OnFailure.Behaviours(OnFailure)...)
	} else if len(jd.OnFailure) > 0 {
		behaviours = append(behaviours, jd.OnFailure...)
	}
	if len(jvj.OnSuccess) > 0 {
		behaviours = append(behaviours, jvj.OnSuccess.Behaviours(OnSuccess)...)
	} else if len(jd.OnSuccess) > 0 {
		behaviours = append(behaviours, jd.OnSuccess...)
	}
	if len(jvj.OnExit) > 0 {
		behaviours = append(behaviours, jvj.OnExit.Behaviours(OnExit)...)
	} else if len(jd.OnExit) > 0 {
		behaviours = append(behaviours, jd.OnExit...)
	}

	if len(jvj.MountConfigs) > 0 {
		mounts = jvj.MountConfigs
	} else if len(jd.MountConfigs) > 0 {
		mounts = jd.MountConfigs
	}

	bsubMode = jvj.BsubMode
	if bsubMode == "" && jd.BsubMode != "" {
		bsubMode = jd.BsubMode
	}

	if jvj.MonitorDocker == "" {
		monitorDocker = jd.MonitorDocker
	} else {
		monitorDocker = jvj.MonitorDocker
	}

	// scheduler-specific options
	other := make(map[string]string)
	if jvj.CloudOS != "" {
		other["cloud_os"] = jvj.CloudOS
	} else if jd.CloudOS != "" {
		other["cloud_os"] = jd.CloudOS
	}

	if jvj.CloudUser != "" {
		other["cloud_user"] = jvj.CloudUser
	} else if jd.CloudUser != "" {
		other["cloud_user"] = jd.CloudUser
	}

	if jvj.CloudFlavor != "" {
		other["cloud_flavor"] = jvj.CloudFlavor
	} else if jd.CloudFlavor != "" {
		other["cloud_flavor"] = jd.CloudFlavor
	}

	var cloudScriptPath string
	if jvj.CloudScript != "" {
		cloudScriptPath = jvj.CloudScript
	} else if jd.CloudScript != "" {
		cloudScriptPath = jd.CloudScript
	}
	if cloudScriptPath != "" {
		scriptContent, err := internal.PathToContent(cloudScriptPath)
		if err != nil {
			return nil, err
		}
		other["cloud_script"] = scriptContent
	}

	if jvj.CloudConfigFiles != "" {
		other["cloud_config_files"] = jvj.CloudConfigFiles
	} else if jd.CloudConfigFiles != "" {
		other["cloud_config_files"] = jd.CloudConfigFiles
	}

	if jvj.CloudOSRam != nil {
		ram := *jvj.CloudOSRam
		other["cloud_os_ram"] = strconv.Itoa(ram)
	} else if jd.CloudOSRam != 0 {
		other["cloud_os_ram"] = jd.DefaultCloudOSRam()
	}

	if jvj.CloudShared || jd.CloudShared {
		other["cloud_shared"] = "true"
	}

	if jvj.SchedulerQueue != "" {
		other["scheduler_queue"] = jvj.SchedulerQueue
	} else if jd.SchedulerQueue != "" {
		other["scheduler_queue"] = jd.SchedulerQueue
	}

	if jvj.SchedulerMisc != "" {
		other["scheduler_misc"] = jvj.SchedulerMisc
	} else if jd.SchedulerMisc != "" {
		other["scheduler_misc"] = jd.SchedulerMisc
	}

	if jvj.RTimeout != nil {
		rtimeout := *jvj.RTimeout
		other["rtimeout"] = strconv.Itoa(rtimeout)
	} else if jd.RTimeout != 0 {
		other["rtimeout"] = strconv.Itoa(jd.RTimeout)
	}

	return &Job{
		RepGroup:      repg,
		Cmd:           cmd,
		Cwd:           cwd,
		CwdMatters:    cwdMatters,
		ChangeHome:    changeHome,
		ReqGroup:      rg,
		Requirements:  &jqs.Requirements{RAM: mb, Time: dur, Cores: cpus, Disk: disk, DiskSet: diskSet, Other: other},
		Override:      uint8(override),
		Priority:      uint8(priority),
		Retries:       uint8(retries),
		LimitGroups:   limitGroups,
		DepGroups:     depGroups,
		Dependencies:  deps,
		EnvOverride:   envOverride,
		Behaviours:    behaviours,
		MountConfigs:  mounts,
		MonitorDocker: monitorDocker,
		BsubMode:      bsubMode,
	}, nil
}

// httpAuthorized checks for parameter 'token' and for Authorization header for
// Bearer token; if not supplied, or the token is wrong, writes out an error to
// w, otherwise returns true.
func (s *Server) httpAuthorized(w http.ResponseWriter, r *http.Request) bool {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, fmt.Sprintf("form parsing error: %s", err), http.StatusBadRequest)
		return false
	}

	// try token parameter
	token := r.Form.Get("token")
	if token == "" {
		// try auth header
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return false
		}

		if !strings.HasPrefix(authHeader, bearerSchema) {
			http.Error(w, "Authorization requires Bearer scheme", http.StatusUnauthorized)
			return false
		}

		token = authHeader[len(bearerSchema):]
	}

	if !tokenMatches([]byte(token), s.token) {
		http.Error(w, "Invalid token", http.StatusUnauthorized)
		return false
	}
	return true
}

// restJobs lets you do CRUD on jobs in the queue.
func restJobs(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue web server restJobs", false)

		ok := s.httpAuthorized(w, r)
		if !ok {
			return
		}

		// carry out a different action based on the HTTP Verb
		var jobs []*Job
		var status int
		var err error
		switch r.Method {
		case http.MethodGet:
			jobs, status, err = restJobsStatus(ctx, r, s)
		case http.MethodPost:
			jobs, status, err = restJobsAdd(ctx, r, s)
		case http.MethodDelete:
			jobs, status, err = restJobsCancel(ctx, r, s)
		default:
			http.Error(w, "So far only GET, POST and DELETE are supported", http.StatusBadRequest)
			return
		}

		if status >= 400 || err != nil {
			http.Error(w, err.Error(), status)
			return
		}

		// convert jobs to jstatus
		jstati := make([]JStatus, len(jobs))
		for i, job := range jobs {
			jstati[i], err = job.ToStatus()
			if err != nil && err != io.ErrUnexpectedEOF {
				http.Error(w, err.Error(), status)
				return
			}
		}

		// return job details as JSON
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(status)
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		erre := encoder.Encode(jstati)
		if erre != nil {
			clog.Warn(ctx, "restJobs failed to encode job statuses", "err", erre)
		}
	}
}

// restJobsStatus gets the status of the requested jobs in the queue. The
// request url can be suffixed with comma separated job keys or RepGroups.
// Possible query parameters are search, std, env (which can take a "true"
// value), limit (a number) and state (one of
// delayed|ready|reserved|running|lost|buried|dependent|complete|deletable),
// where deletable == !(running|complete). Returns the Jobs, a http.Status*
// value and error.
func restJobsStatus(ctx context.Context, r *http.Request, s *Server) ([]*Job, int, error) {
	// handle possible ?query parameters
	var search, getStd, getEnv bool
	var limit int
	var state JobState
	var err error

	if r.Form.Get("search") == restFormTrue {
		search = true
	}
	if r.Form.Get("std") == restFormTrue {
		getStd = true
	}
	if r.Form.Get("env") == restFormTrue {
		getEnv = true
	}
	if r.Form.Get("limit") != "" {
		limit, err = strconv.Atoi(r.Form.Get("limit"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
	}
	if r.Form.Get("state") != "" {
		switch r.Form.Get("state") {
		case "delayed":
			state = JobStateDelayed
		case "ready":
			state = JobStateReady
		case "reserved":
			state = JobStateReserved
		case "running":
			state = JobStateRunning
		case "lost":
			state = JobStateLost
		case "buried":
			state = JobStateBuried
		case "dependent":
			state = JobStateDependent
		case "complete":
			state = JobStateComplete
		case "deletable":
			state = JobStateDeletable
		}
	}

	if len(r.URL.Path) > len(restJobsEndpoint) {
		// get the requested jobs
		ids := r.URL.Path[len(restJobsEndpoint):]
		var jobs []*Job
		for _, id := range strings.Split(ids, ",") {
			if len(id) == 32 {
				// id might be a Job.key()
				theseJobs, _, qerr := s.getJobsByKeys(ctx, []string{id}, getStd, getEnv)
				if qerr == "" && len(theseJobs) > 0 {
					jobs = append(jobs, theseJobs...)
					continue
				}
			}

			// id might be a Job.RepGroup
			theseJobs, _, qerr := s.getJobsByRepGroup(ctx, id, search, limit, state, getStd, getEnv)
			if qerr != "" {
				return nil, http.StatusInternalServerError, fmt.Errorf(qerr)
			}
			if len(theseJobs) > 0 {
				jobs = append(jobs, theseJobs...)
			}
		}
		return jobs, http.StatusOK, err
	}

	// get all current jobs
	return s.getJobsCurrent(ctx, limit, state, getStd, getEnv), http.StatusOK, err
}

// restJobsAdd creates and adds jobs to the queue and returns them on success.
// The request must have some POSTed JSON that is a []*JobViaJSON.
//
// It optionally takes parameters to use as defaults for the job properties,
// which correspond to the json properties of a JobViaJSON (except for cmd and
// cmd_deps). For dep_grps, deps and env, which normally take []string, provide
// a comma-separated list. mounts, on_failure, on_success and on_exit values
// should be supplied as url query escaped JSON strings.
//
// The returned int is a http.Status* variable.
func restJobsAdd(ctx context.Context, r *http.Request, s *Server) ([]*Job, int, error) {
	// handle possible ?query parameters
	_, diskSet := r.Form["disk"]
	jd := &JobDefaults{
		Cwd:           r.Form.Get("cwd"),
		RepGrp:        r.Form.Get("rep_grp"),
		LimitGroups:   urlStringToSlice(r.Form.Get("limit_grps")),
		ReqGrp:        r.Form.Get("req_grp"),
		CPUs:          urlStringToFloat(r.Form.Get("cpus")),
		Disk:          urlStringToInt(r.Form.Get("disk")),
		DiskSet:       diskSet,
		Override:      urlStringToInt(r.Form.Get("override")),
		Priority:      urlStringToInt(r.Form.Get("priority")),
		Retries:       urlStringToInt(r.Form.Get("retries")),
		DepGroups:     urlStringToSlice(r.Form.Get("dep_grps")),
		Env:           r.Form.Get("env"),
		MonitorDocker: r.Form.Get("monitor_docker"),
		CloudOS:       r.Form.Get("cloud_os"),
		CloudUser:     r.Form.Get("cloud_username"),
		CloudScript:   r.Form.Get("cloud_script"),
		CloudFlavor:   r.Form.Get("cloud_flavor"),
		CloudOSRam:    urlStringToInt(r.Form.Get("cloud_ram")),
		BsubMode:      r.Form.Get("bsub_mode"),
	}
	if jd.RepGrp == "" {
		jd.RepGrp = "manually_added"
	}
	if r.Form.Get("cwd_matters") == restFormTrue {
		jd.CwdMatters = true
	}
	if r.Form.Get("change_home") == restFormTrue {
		jd.ChangeHome = true
	}
	if r.Form.Get("cloud_shared") == restFormTrue {
		jd.CloudShared = true
	}
	if r.Form.Get("memory") != "" {
		mb, err := bytefmt.ToMegabytes(r.Form.Get("memory"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		jd.Memory = int(mb)
	}
	if r.Form.Get("time") != "" {
		var err error
		jd.Time, err = time.ParseDuration(r.Form.Get("time"))
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
	}
	var rerun bool
	if r.Form.Get("rerun") == restFormTrue {
		rerun = true
	}
	defaultDeps := urlStringToSlice(r.Form.Get("deps"))
	if len(defaultDeps) > 0 {
		for _, depgroup := range defaultDeps {
			jd.Deps = append(jd.Deps, NewDepGroupDependency(depgroup))
		}
	}
	if r.Form.Get("on_failure") != "" {
		var bvj BehavioursViaJSON
		err := urlStringToStruct(r.Form.Get("on_failure"), &bvj)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		if bvj != nil {
			jd.OnFailure = bvj.Behaviours(OnFailure)
		}
	}
	if r.Form.Get("on_success") != "" {
		var bvj BehavioursViaJSON
		err := urlStringToStruct(r.Form.Get("on_success"), &bvj)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		if bvj != nil {
			jd.OnSuccess = bvj.Behaviours(OnSuccess)
		}
	}
	if r.Form.Get("on_exit") != "" {
		var bvj BehavioursViaJSON
		err := urlStringToStruct(r.Form.Get("on_exit"), &bvj)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		if bvj != nil {
			jd.OnExit = bvj.Behaviours(OnExit)
		}
	}
	if r.Form.Get("mounts") != "" {
		var mcs MountConfigs
		err := urlStringToStruct(r.Form.Get("mounts"), &mcs)
		if err != nil {
			return nil, http.StatusBadRequest, err
		}
		if mcs != nil {
			jd.MountConfigs = mcs
		}
	}

	// decode the posted JSON
	var jvjs []*JobViaJSON
	err := json.NewDecoder(r.Body).Decode(&jvjs)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	// convert to real Job structs with default values filled in
	inputJobs := make([]*Job, 0, len(jvjs))
	for _, jvj := range jvjs {
		job, errf := jvj.Convert(jd)
		if errf != nil {
			return nil, http.StatusBadRequest, fmt.Errorf("there was a problem interpreting your job: %s", errf)
		}
		inputJobs = append(inputJobs, job)
	}

	envkey, err := s.db.storeEnv([]byte{})
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	_, _, _, _, err = s.createJobs(ctx, inputJobs, envkey, !rerun)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	// see which of the inputJobs are now actually in the queue
	jobs := s.inputToQueuedJobs(ctx, inputJobs)

	return jobs, http.StatusCreated, err
}

// restJobsCancel kills running jobs, confirms lost jobs as dead, or deletes
// incomplete jobs. You identify the jobs to operate on in the same way as for
// restJobsStatus(). However state must be specified, and only one of:
// (running|lost|deletable) are allowed. Returns the affected Jobs, a
// http.Status* value and error.
func restJobsCancel(ctx context.Context, r *http.Request, s *Server) ([]*Job, int, error) {
	var state JobState
	if r.Form.Get("state") != "" {
		switch r.Form.Get("state") {
		case "running":
			state = JobStateRunning
		case "lost":
			state = JobStateLost
		case "deletable":
			state = JobStateDeletable
		}
	}
	if state == "" {
		return nil, http.StatusBadRequest, fmt.Errorf("state must be supplied as one of running|lost|deletable")
	}

	jobs, status, err := restJobsStatus(ctx, r, s)
	if err != nil || status != http.StatusOK {
		return nil, status, err
	}

	var handled []*Job
	returnStatus := http.StatusAccepted
	if state == JobStateDeletable {
		returnStatus = http.StatusOK
		keys := make([]string, len(jobs))
		for i, job := range jobs {
			keys[i] = job.Key()
		}
		deleted := s.deleteJobs(ctx, keys)
		d := make(map[string]bool, len(deleted))
		for _, key := range deleted {
			d[key] = true
		}
		for _, job := range jobs {
			if d[job.Key()] {
				job.State = JobStateDeleted
				handled = append(handled, job)
			}
		}
	} else {
		for _, job := range jobs {
			k, err := s.killJob(ctx, job.Key())
			if err != nil {
				return handled, http.StatusInternalServerError, err
			}
			if k {
				handled = append(handled, job)
			}
		}
	}
	return handled, returnStatus, nil
}

// restWarnings lets you read warnings from the scheduler, and auto-"dismisses"
// (deletes) them.
func restWarnings(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue web server restWarnings", false)

		ok := s.httpAuthorized(w, r)
		if !ok {
			return
		}

		// carry out a different action based on the HTTP Verb
		sis := []*schedulerIssue{}
		switch r.Method {
		case http.MethodGet:
			s.simutex.Lock()
			for key, si := range s.schedIssues {
				sis = append(sis, si)
				delete(s.schedIssues, key)
			}
			s.simutex.Unlock()
		default:
			http.Error(w, "Only GET is supported", http.StatusBadRequest)
			return
		}

		// return schedulerIssues as JSON
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		erre := encoder.Encode(sis)
		if erre != nil {
			clog.Warn(ctx, "restWarnings failed to encode scheduler issues", "err", erre)
		}
	}
}

// restBadServers lets you do CRUD on cloud servers that have gone bad. The
// DELETE verb has a required 'id' parameter, being the ID of a server you wish
// to confirm as bad and have terminated if it still exists.
func restBadServers(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue web server restBadServers", false)

		ok := s.httpAuthorized(w, r)
		if !ok {
			return
		}

		// carry out a different action based on the HTTP Verb
		switch r.Method {
		case http.MethodGet:
			servers := s.getBadServers()
			if len(servers) == 0 {
				servers = []*BadServer{}
			}
			w.Header().Set("Content-Type", "application/json; charset=UTF-8")
			w.WriteHeader(http.StatusOK)
			encoder := json.NewEncoder(w)
			encoder.SetEscapeHTML(false)
			erre := encoder.Encode(servers)
			if erre != nil {
				clog.Warn(ctx, "restBadServers failed to encode servers", "err", erre)
			}
			return
		case http.MethodDelete:
			serverID := r.Form.Get("id")
			if serverID == "" {
				http.Error(w, "id parameter is required", http.StatusBadRequest)
				return
			}
			s.bsmutex.Lock()
			server := s.badServers[serverID]
			delete(s.badServers, serverID)
			s.bsmutex.Unlock()
			if server == nil {
				http.Error(w, "Server was not known to be bad", http.StatusNotFound)
				return
			}
			if server.IsBad() {
				err := server.Destroy(ctx)
				if err != nil {
					http.Error(w, fmt.Sprintf("Server was bad but could not be destroyed: %s", err), http.StatusNotModified)
					return
				}
			}
			w.WriteHeader(http.StatusOK)
			return
		default:
			http.Error(w, "Only GET and DELETE are supported", http.StatusBadRequest)
			return
		}
	}
}

// restFileUpload lets you upload files from a client to the server. The only
// method supported is PUT.
func restFileUpload(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue web server restFileUpload", false)

		ok := s.httpAuthorized(w, r)
		if !ok {
			return
		}

		if r.Method != http.MethodPut {
			http.Error(w, "Only PUT is supported", http.StatusBadRequest)
			return
		}

		savePath, err := s.uploadFile(ctx, r.Body, r.Form.Get("path"))
		if err != nil {
			http.Error(w, "file upload failed", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		msg := make(map[string]string)
		msg["path"] = savePath
		err = encoder.Encode(msg)
		if err != nil {
			clog.Warn(ctx, "restFileUpload failed to encode success msg", "err", err)
		}
	}
}

// restInfo lets you get info on self.
func restInfo(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue server status", false)

		ok := s.httpAuthorized(w, r)
		if !ok {
			return
		}

		if r.Method != http.MethodGet {
			http.Error(w, "Only GET is supported", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		err := encoder.Encode(s.ServerInfo)
		if err != nil {
			clog.Warn(ctx, "restInfo failed to encode ServerInfo", "err", err)
		}
	}
}

// restVersion lets you get info on the version of the server and the supported
// API version (we only support 1 API version at a time). This is the only
// end point that doesn't need authentication.
func restVersion(ctx context.Context, s *Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer internal.LogPanic(ctx, "jobqueue server version", false)

		if r.Method != http.MethodGet {
			http.Error(w, "Only GET is supported", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		err := encoder.Encode(s.ServerVersions)
		if err != nil {
			clog.Warn(ctx, "restVersion failed to encode ServerVersions", "err", err)
		}
	}
}

// urlStringToInt takes a possible string from a url parameter value and
// converts it to an int. If the value is "", or if the value isn't a number,
// returns 0.
func urlStringToInt(value string) int {
	if value == "" {
		return 0
	}
	num, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return num
}

// urlStringToFloat takes a possible string from a url parameter value and
// converts it to a float64. If the value is "", or if the value isn't a number,
// returns 0.
func urlStringToFloat(value string) float64 {
	if value == "" {
		return 0
	}
	num, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return num
}

// urlStringToSlice takes a possible comma-delimited string from a url parameter
// value and converts it to []string. If the value is "", returns an empty
// slice.
func urlStringToSlice(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// urlStringToStruct takes a possible query escaped JSON string from a url
// parameter value and unmarshals it in to the pointed to struct. If the value
// is "", does nothing.
func urlStringToStruct(value string, v interface{}) error {
	if value == "" {
		return nil
	}
	jsonString, err := url.QueryUnescape(value)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(jsonString), v)
}

// compressEnv is a slower (?) version of Client.CompressEnv since we have to
// make a new codec each time
func compressEnv(envars []string) ([]byte, error) {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, new(codec.BincHandle))
	err := enc.Encode(&envStr{envars})
	if err != nil {
		return nil, err
	}
	return compress(encoded)
}
