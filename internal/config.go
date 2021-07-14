/*******************************************************************************
 * Copyright (c) 2020, 2021 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>, Ashwini Chhipa <ac55@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 ******************************************************************************/

package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/creasty/defaults"
	"github.com/jinzhu/configor"
	"github.com/olekukonko/tablewriter"
	"github.com/wtsi-ssg/wr/clog"
	fsd "github.com/wtsi-ssg/wr/fs/dir"
	fp "github.com/wtsi-ssg/wr/fs/filepath"
	"github.com/wtsi-ssg/wr/network/port"
)

const (
	// configCommonBasename is the basename of a wr config file.
	configCommonBasename = ".wr_config.yml"

	// S3Prefix is the prefix used by S3 paths.
	S3Prefix = "s3://"

	// Production is the name of the main deployment.
	Production = "production"

	// Development is the name of the development deployment, used during
	// testing.
	Development = "development"

	// ConfigSourceEnvVar is a config value source.
	ConfigSourceEnvVar = "env var"

	// ConfigSourceDefault is a config value source.
	ConfigSourceDefault = "default"

	// sourcesProperty is a source property.
	sourcesProperty = "sources"

	// portsNeeded is the number of ports used for each user.
	portsNeeded = 4

	// portsProdDevDiff is the number of difference in ports for different deployments.
	portsProdDevDiff = 2

	// portsCmdWebDiff is the number of difference in ports for Cmd or Web interface.
	portsCmdWebDiff = 1

	// portsMinPort is the minimum port used.
	portsMinPort = 1021

	// portsMaxPort is the maximum port available.
	portsMaxPort = 65535

	// promptStatement is used by findPorts.
	promptStatement = `The default ports couldn't be used for you, but ports %d..%d are available right now.
 You could use these for your managerport and managerweb config options in production and development.`

	// promptQuestion is used by findPorts.
	promptQuestion = "Write them to your config files at ~/.wr_config.production.yml and " +
		"~/.wr_config.development.yml (y/n)?"

	// localhost is the name of the host that we check ports on.
	localhost = "localhost"

	// noPortFoundErr is the error returned when no available port was found.
	noPortFoundErr = "The default ports couldn't be used for you; " +
		"please manually set your manager_port and manager_web config options."
)

// Config holds the configuration options for jobqueue server and client.
type Config struct {
	ManagerPort          string `default:""`
	ManagerWeb           string `default:""`
	ManagerHost          string `default:"localhost"`
	ManagerDir           string `default:"~/.wr"`
	ManagerPidFile       string `default:"pid"`
	ManagerLogFile       string `default:"log"`
	ManagerDBFile        string `default:"db"`
	ManagerDBBkFile      string `default:"db_bk"`
	ManagerTokenFile     string `default:"client.token"`
	ManagerUploadDir     string `default:"uploads"`
	ManagerUmask         int    `default:"007"`
	ManagerScheduler     string `default:"local"`
	ManagerCAFile        string `default:"ca.pem"`
	ManagerCertFile      string `default:"cert.pem"`
	ManagerKeyFile       string `default:"key.pem"`
	ManagerCertDomain    string `default:"localhost"`
	ManagerSetDomainIP   bool   `default:"false"`
	RunnerExecShell      string `default:"bash"`
	PrivateKeyPath       string `default:"~/.ssh/id_rsa"`
	Deployment           string `default:"production"`
	CloudFlavor          string `default:""`
	CloudFlavorManager   string `default:""`
	CloudFlavorSets      string `default:""`
	CloudKeepAlive       int    `default:"120"`
	CloudServers         int    `default:"-1"`
	CloudCIDR            string `default:"192.168.64.0/18"`
	CloudGateway         string `default:"192.168.64.1"`
	CloudDNS             string `default:"8.8.4.4,8.8.8.8"`
	CloudOS              string `default:"bionic-server"`
	ContainerImage       string `default:"ubuntu:latest"`
	CloudUser            string `default:"ubuntu"`
	CloudRAM             int    `default:"2048"`
	CloudDisk            int    `default:"1"`
	CloudScript          string `default:""`
	CloudConfigFiles     string `default:"~/.s3cfg,~/.aws/credentials,~/.aws/config"`
	CloudSpawns          int    `default:"10"`
	CloudAutoConfirmDead int    `default:"30"`
	DeploySuccessScript  string `default:""`
	sources              map[string]string
}

// FileExistsError records a file exist error.
type FileExistsError struct {
	Path string
	Err  error
}

// Error returns error when a file already exists.
func (f *FileExistsError) Error() string {
	return fmt.Sprintf("file [%s] already exists: %s", f.Path, f.Err)
}

// Source returns where the value of a Config field was defined.
func (c Config) Source(field string) string {
	if c.sources == nil {
		return ConfigSourceDefault
	}

	source, set := c.sources[field]
	if !set {
		return ConfigSourceDefault
	}

	return source
}

// IsProduction tells you if we're in the production deployment.
func (c Config) IsProduction() bool {
	return c.Deployment == Production
}

// String retruns the string value of property.
func (c Config) String() string {
	vals := reflect.ValueOf(c)
	typeOfC := vals.Type()

	tableString := &strings.Builder{}
	table := tablewriter.NewWriter(tableString)
	table.SetHeader([]string{"Config", "Value", "Source"})
	table.SetAlignment(tablewriter.ALIGN_LEFT)

	for i := 0; i < vals.NumField(); i++ {
		property := typeOfC.Field(i).Name
		if property == sourcesProperty {
			continue
		}

		source := c.sources[property]
		if source == "" {
			source = ConfigSourceDefault
		}

		table.Append([]string{property, fmt.Sprintf("%v", vals.Field(i).Interface()), source})
	}

	table.Render()

	return tableString.String()
}

// ConfigLoadFromCurrentDir loads and returns the config from current directory.
func ConfigLoadFromCurrentDir(ctx context.Context, deployment string) *Config {
	pwd, uid := getPWDAndUID(ctx)

	return mergeAllConfigs(ctx, uid, deployment, pwd)
}

// ConfigLoadFromParentDir loads and returns the config from a parent directory
// for testing purposes.
func ConfigLoadFromParentDir(ctx context.Context, deployment string) *Config {
	pwd, uid := getPWDAndUID(ctx)
	pwd = filepath.Dir(pwd)

	return mergeAllConfigs(ctx, uid, deployment, pwd)
}

// getPWDAndUID returns present working directory and user id.
func getPWDAndUID(ctx context.Context) (string, int) {
	pwd := fsd.GetPWD(ctx)
	uid := os.Getuid()

	return pwd, uid
}

// mergeAllConfigs function loads and merges all the configs and returns a final
// config.
func mergeAllConfigs(ctx context.Context, uid int, deployment string, pwd string) *Config {
	if deployment != Development && deployment != Production {
		deployment = DefaultDeployment(ctx)
	}

	configDef := mergeDefaultAndEnvVarsConfigs(ctx)

	configDef.mergeAllConfigFiles(ctx, uid, deployment, pwd)

	return configDef
}

// DefaultDeployment works out the default deployment.
func DefaultDeployment(ctx context.Context) string {
	deploymentEnv := os.Getenv("WR_DEPLOYMENT")
	if deploymentEnv != "" && (deploymentEnv == Development || deploymentEnv == Production) {
		return deploymentEnv
	}

	return defaultDeploymentBasedOnRepo(ctx)
}

// defaultDeploymentBasedOnRepo returns the deployment type based on repo.
func defaultDeploymentBasedOnRepo(ctx context.Context) string {
	pwd := fsd.GetPWD(ctx)
	if _, err := os.Stat(filepath.Join(pwd, "jobqueue", "server.go")); err == nil {
		return Development
	}

	return Production
}

// mergeDefaultAndEnvVarsConfigs returns a merged Default and EnvVar config.
func mergeDefaultAndEnvVarsConfigs(ctx context.Context) *Config {
	configDef := loadDefaultConfig(ctx)

	fixEnvManagerUmask()

	configEnv := getEnvVarsConfig(ctx)

	configDef.merge(configEnv, ConfigSourceEnvVar)

	return configDef
}

// loadDefaultConfig loads and return the default configs.
func loadDefaultConfig(ctx context.Context) *Config {
	config := &Config{}
	if cerr := defaults.Set(config); cerr != nil {
		clog.Fatal(ctx, cerr.Error())
	}

	return config
}

// fixEnvManagerUmask fixes the WR_MANAGERUMASK env variable. ManagerUmask is
// likely to be zero prefixed by user, but that is not converted to int
// correctly, so it fixes that.
func fixEnvManagerUmask() {
	umask := os.Getenv("WR_MANAGERUMASK")
	if umask != "" && strings.HasPrefix(umask, "0") {
		umask = strings.TrimLeft(umask, "0")
		os.Setenv("WR_MANAGERUMASK", umask)
	}
}

// getEnvVarsConfig loads the env variables into a empty config and returns
// the final config.
func getEnvVarsConfig(ctx context.Context) *Config {
	// we don't os.Setenv("CONFIGOR_ENV", deployment) to stop configor loading
	// files before we want it to
	err := os.Setenv("CONFIGOR_ENV_PREFIX", "WR")
	if err != nil {
		clog.Fatal(ctx, err.Error())
	}

	configEnv := &Config{}

	err = configor.Load(configEnv)
	if err != nil {
		clog.Fatal(ctx, err.Error())
	}

	return configEnv
}

// merge compares existing to new Config values, and for each one that has
// changed, sets the given source on the changed property in our sources, and
// sets the new value on ourselves.
func (c *Config) merge(newC *Config, source string) {
	v := reflect.ValueOf(*c)
	typeOfC := v.Type()
	vNew := reflect.ValueOf(*newC)

	if c.sources == nil {
		c.sources = make(map[string]string)
	}

	for i := 0; i < v.NumField(); i++ {
		property := typeOfC.Field(i).Name
		if property == sourcesProperty {
			continue
		}

		if vNew.Field(i).Interface() != v.Field(i).Interface() {
			c.sources[property] = source

			adrField := reflect.ValueOf(c).Elem().Field(i)
			setSourceOnChangeProp(typeOfC, adrField, vNew, i)
		}
	}
}

// mergeAllConfigFiles merges all the config files and adjusts config
// properties.
func (c *Config) mergeAllConfigFiles(ctx context.Context, uid int, deployment string, pwd string) {
	configDeploymentBasename := ".wr_config." + deployment + ".yml"

	if configDir := os.Getenv("WR_CONFIG_DIR"); configDir != "" {
		c.configLoadFromFile(ctx, filepath.Join(configDir, configCommonBasename))
		c.configLoadFromFile(ctx, filepath.Join(configDir, configDeploymentBasename))
	}

	home := fsd.GetHome(ctx)

	c.configLoadFromFile(ctx, filepath.Join(home, configCommonBasename))
	c.configLoadFromFile(ctx, filepath.Join(home, configDeploymentBasename))

	c.configLoadFromFile(ctx, filepath.Join(pwd, configCommonBasename))
	c.configLoadFromFile(ctx, filepath.Join(pwd, configDeploymentBasename))

	c.adjustConfigProperties(ctx, uid, deployment)
}

// configLoadFromFile loads a config from a file and merges into the current
// config.
func (c *Config) configLoadFromFile(ctx context.Context, path string) {
	if _, err := os.Stat(path); err != nil {
		return
	}

	configFile := c.clone()

	if err := configor.Load(configFile, path); err != nil {
		clog.Fatal(ctx, err.Error())
	}

	c.merge(configFile, path)
}

// clone makes a new Config with our values.
func (c *Config) clone() *Config {
	newC := &Config{}

	v := reflect.ValueOf(*c)
	typeOfC := v.Type()

	for i := 0; i < v.NumField(); i++ {
		property := typeOfC.Field(i).Name
		if property == sourcesProperty {
			continue
		}

		adrField := reflect.ValueOf(newC).Elem().Field(i)
		setSourceOnChangeProp(typeOfC, adrField, v, i)
	}

	newC.sources = make(map[string]string)
	for key, val := range c.sources {
		newC.sources[key] = val
	}

	return newC
}

// setSourceOnChangeProp sets the source of a property, when its value is
// changed.
func setSourceOnChangeProp(typeOfC reflect.Type, adrField reflect.Value,
	newVal reflect.Value, idx int) {
	switch typeOfC.Field(idx).Type.Kind() {
	case reflect.String:
		adrField.SetString(newVal.Field(idx).String())
	case reflect.Int:
		adrField.SetInt(newVal.Field(idx).Int())
	case reflect.Bool:
		adrField.SetBool(newVal.Field(idx).Bool())
	default:
		return
	}
}

// adjustConfigProperties adjusts the config properties for pid, log file,
// upload dir paths, certs and db files.
func (c *Config) adjustConfigProperties(ctx context.Context, uid int, deployment string) {
	c.Deployment = deployment

	c.ManagerDir = fp.TildaToHome(c.ManagerDir)
	c.ManagerDir += "_" + deployment

	c.convRelativeToAbsPaths()
	c.setManagerPort(ctx, uid)
}

// convRelativeToAbsPaths converts the possible relative paths of various
// properties to abs paths in ManagerDir.
func (c *Config) convRelativeToAbsPaths() {
	c.convRelativeToAbsPath(&c.ManagerPidFile)
	c.convRelativeToAbsPath(&c.ManagerLogFile)
	c.convRelativeToAbsPath(&c.ManagerUploadDir)

	c.convRelativeToAbsPath(&c.ManagerCAFile)
	c.convRelativeToAbsPath(&c.ManagerCertFile)
	c.convRelativeToAbsPath(&c.ManagerKeyFile)
	c.convRelativeToAbsPath(&c.ManagerTokenFile)
	c.convRelativeToAbsPath(&c.ManagerDBFile)

	if !IsRemote(c.ManagerDBBkFile) {
		c.convRelativeToAbsPath(&c.ManagerDBBkFile)
	}
}

// convRelativeToAbsPath converts the possible relative path to abs path of a
// property in ManagerDir.
func (c *Config) convRelativeToAbsPath(property *string) {
	if !filepath.IsAbs(*property) {
		*property = filepath.Join(c.ManagerDir, *property)
	}
}

// setManagerPort sets the cli and web interface ports for manager.
func (c *Config) setManagerPort(ctx context.Context, uid int) {
	if c.ManagerPort == "" {
		c.ManagerPort = calculatePort(ctx, c, uid, localhost, "cli")
	}

	if c.ManagerWeb == "" {
		c.ManagerWeb = calculatePort(ctx, c, uid, localhost, "webi")
	}
}

// calculatePort calculates a port number that will be unique to this user,
// deployment and ptype ("cli" or "webi"). It returns a string because port is
// usually used as a part of a connection string.
func calculatePort(ctx context.Context, config *Config, uid int, hostname string, ptype string) string {
	pn, found := getMinPort(ctx, hostname, uid)

	if pn == 0 {
		return ""
	}

	if config.Deployment == Development {
		pn += portsProdDevDiff
	}

	if ptype == "webi" {
		pn += portsCmdWebDiff
	} else if found {
		config.ManagerWeb = strconv.Itoa(pn + portsCmdWebDiff)
	}

	return strconv.Itoa(pn)
}

// getMinPort calculates and returns the minimum port available. Our port must
// be greater than 1024, and by basing on user id we can avoid conflicts with
// other users of wr on the same machine; we multiply by 4 because we have to
// reserve 4 ports for each user. If that port would be too high, then we get an
// available port.
func getMinPort(ctx context.Context, hostname string, uid int) (int, bool) {
	pn := portsMinPort + (uid * portsNeeded)

	if pn+portsNeeded-1 > portsMaxPort {
		checker := getPortChecker(ctx, hostname)
		if checker == nil {
			return 0, false
		}

		pn = findPorts(ctx, checker)
	}

	return pn, true
}

// getPortChecker returns a port checker given a hostname and exits on error.
func getPortChecker(ctx context.Context, hostname string) *port.Checker {
	checker, err := port.NewChecker(hostname)
	if err != nil {
		exitDueToNoPorts(ctx, err, "localhost couldn't be connected to")

		return nil
	}

	return checker
}

// findPorts asks the OS for an available port range, then asks the user if
// they'd like to use it and writes it to their config file.
func findPorts(ctx context.Context, checker *port.Checker) int {
	min, max := getAvailableRange(ctx, checker)
	if min == 0 || max == 0 {
		return 0
	}

	fmt.Printf(promptStatement, min, max)

	var response string

	fmt.Println(promptQuestion)
	fmt.Scanf("%s\n", &response)

	if response == "y" {
		writePortsToConfigFiles(ctx, min)
	} else {
		exitDueToNoPorts(ctx, nil, "you chose not to use suggested ports")

		return 0
	}

	return min
}

// getAvailableRange returns the min and max port numbers available, else
// returns 0, 0 and exits on error.
func getAvailableRange(ctx context.Context, checker *port.Checker) (int, int) {
	min, max, err := checker.AvailableRange(portsNeeded)
	if err != nil {
		exitDueToNoPorts(ctx, err, "available localhost ports couldn't be checked")

		return 0, 0
	}

	return min, max
}

// exitDueToNoPorts exits non-zero with an error because we can't continue
// without knowing our ports.
func exitDueToNoPorts(ctx context.Context, err error, msg string) {
	clog.Fatal(ctx, noPortFoundErr, "msg", msg, "err", err)
}

// writePortsToConfigFiles writes ports to production and development config
// files.
func writePortsToConfigFiles(ctx context.Context, pn int) {
	home := fsd.GetHome(ctx)

	writePortsToConfigFile(ctx, pn, home, Production)
	writePortsToConfigFile(ctx, pn+portsProdDevDiff, home, Development)
}

// writePortsToConfigFile writes ports to a config file given the deployment
// type.
func writePortsToConfigFile(ctx context.Context, pn int, home, deployment string) {
	f, err := os.OpenFile(filepath.Join(home, ".wr_config."+deployment+".yml"),
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		clog.Fatal(ctx, "could not open config file", "err", err)

		return
	}

	writeString := fmt.Sprintf("managerport: \"%d\"\nmanagerweb: \"%d\"\n", pn, pn+portsCmdWebDiff)

	writePorts(ctx, f, writeString)

	f.Close()
}

// writePorts will write a port string to a file given its file handler.
func writePorts(ctx context.Context, filep *os.File, writeString string) {
	if _, err := filep.WriteString(writeString); err != nil {
		filep.Close()
		clog.Fatal(ctx, "could not write to config file", "err", err)
	}
}

// DefaultConfig works out the default config for when we need to be able to
// report the default before we know what deployment the user has actually
// chosen, ie. before we have a final config.
func DefaultConfig(ctx context.Context) *Config {
	return ConfigLoadFromCurrentDir(ctx, DefaultDeployment(ctx))
}

// DefaultServer works out the default server (we need this to be able to report
// this default before we know what deployment the user has actually chosen, ie.
// before we have a final config).
func DefaultServer(ctx context.Context) string {
	config := DefaultConfig(ctx)

	return config.ManagerHost + ":" + config.ManagerPort
}

// InS3 tells you if a path is to a file in S3.
func InS3(path string) bool {
	return strings.HasPrefix(path, S3Prefix)
}

// IsRemote tells you if a path is to a remote file system or object store,
// based on its URI.
func IsRemote(path string) bool {
	// (right now we only support S3, but IsRemote is to future-proof us and
	// avoid calling InS3() directly)
	return InS3(path)
}
