/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
 *
 * Author: Ashwini Chhipa <ac55@sanger.ac.uk>
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
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wr/clog"
	fsd "github.com/wtsi-ssg/wr/fs/dir"
	fl "github.com/wtsi-ssg/wr/fs/file"
	ft "github.com/wtsi-ssg/wr/fs/test"
)

// expectedManagerPort is the manager port string used to verify tests.
const expectedManagerPort = `managerport: "1000"`

func fileTestSetup(dir, mport, mweb1, mweb2 string) (string, string, error) {
	path := filepath.Join(dir, ".wr_config.yml")

	file, err := createFile(path)
	if err != nil {
		return path, "", err
	}

	path2 := filepath.Join(dir, ".wr_config.development.yml")

	file2, err := createFile(path2)
	if err != nil {
		return path, path2, err
	}

	writeStringToFile(file, fmt.Sprintf("managerport: \"%s\"\n", mport))
	writeStringToFile(file, fmt.Sprintf("managerweb: \"%s\"\n", mweb1))
	file.Close()

	writeStringToFile(file2, fmt.Sprintf("managerweb: \"%s\"\n", mweb2))
	file2.Close()

	return path, path2, nil
}

func createFile(path string) (*os.File, error) {
	if _, err := os.Stat(path); err == nil {
		return nil, &FileExistsError{Path: path, Err: nil}
	}

	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return file, nil
}

func writeStringToFile(filep *os.File, data string) {
	_, err := filep.WriteString(data)
	So(err, ShouldBeNil)
}

func fileTestTeardown(path, path2 string) {
	if err := os.Remove(path); err != nil {
		fmt.Printf("\nfailed to delete %s: %s\n", path, err)
	}

	if err := os.Remove(path2); err != nil {
		fmt.Printf("\nfailed to delete %s: %s\n", path2, err)
	}
}

func setBufferLevel() *bytes.Buffer {
	buff := clog.ToBufferAtLevel("fatal")

	os.Setenv("WR_FATAL_EXIT_TEST", "1")

	return buff
}

func checkErrorFromBuffer(buff *bytes.Buffer, subStr string) {
	defer clog.ToDefault()
	defer os.Unsetenv("WR_FATAL_EXIT_TEST")

	bufferStr := buff.String()
	So(bufferStr, ShouldContainSubstring, "fatal=true")
	So(bufferStr, ShouldNotContainSubstring, "caller=clog")
	So(bufferStr, ShouldContainSubstring, "stack=")
	So(bufferStr, ShouldContainSubstring, subStr)
}

func getTempHome(ctx context.Context) (string, func()) {
	origHome := fsd.GetHome(ctx)
	tempHome, err := os.MkdirTemp("", "tempHome")
	So(err, ShouldBeNil)
	os.Setenv("HOME", tempHome)

	return tempHome, func() {
		os.Setenv("HOME", origHome)
	}
}

func checkPortRange(dir string, deployment string, expected string) {
	path := getDeploymentConfigFile(dir, deployment)
	content, err := fl.ToString(path)
	So(err, ShouldBeNil)
	So(content, ShouldContainSubstring, expected)
}

func getDeploymentConfigFile(dir string, deployment string) string {
	return filepath.Join(dir, ".wr_config."+deployment+".yml")
}

func TestConfig(t *testing.T) {
	ctx := context.Background()

	Convey("Given a path", t, func() {
		pathS3 := "s3://test1"
		pathNotS3 := "/tmp/test2"

		Convey("we can see if it's in an S3 bucket", func() {
			So(InS3(pathS3), ShouldEqual, true)
			So(InS3(pathNotS3), ShouldEqual, false)
		})

		Convey("we can see if it's a remote file", func() {
			So(IsRemote(pathS3), ShouldEqual, true)
			So(IsRemote(pathNotS3), ShouldEqual, false)
		})
	})

	Convey("We can get the present working directory and user id", t, func() {
		pwd, uid := getPWDAndUID(ctx)
		So(pwd, ShouldNotBeNil)
		So(uid, ShouldNotBeNil)
	})

	Convey("We can write port to a file", t, func() {
		path := ft.FilePathInTempDir(t, "testWrite")
		file, err := os.Create(path)
		So(err, ShouldBeNil)
		wrtString := fmt.Sprintf("managerport: \"%d\"\nmanagerweb: \"%d\"\n", 1000, 1002)
		writePorts(ctx, file, wrtString)
		expected := expectedManagerPort
		content, err := fl.ToString(path)
		So(err, ShouldBeNil)
		So(content, ShouldContainSubstring, expected)

		Convey("not when file is not writable", func() {
			f, err := os.OpenFile(filepath.Join(os.TempDir(), "testNoWrite"),
				os.O_APPEND|os.O_CREATE|os.O_RDONLY, 0444)
			So(err, ShouldBeNil)

			buff := setBufferLevel()
			writePorts(ctx, f, wrtString)
			checkErrorFromBuffer(buff, "could not write to config file")
		})
	})

	Convey("Given a user id", t, func() {
		uid := 1000
		Convey("We can get the minimum port number for it", func() {
			pn, found := getMinPort(ctx, localhost, uid)
			So(pn, ShouldEqual, 5021)
			So(found, ShouldBeTrue)
		})

		Convey("We will exit if no available ports found", func() {
			buff := setBufferLevel()
			exitDueToNoPorts(ctx, nil, "no ports found")
			subString := "no ports found"
			checkErrorFromBuffer(buff, subString)
		})
	})

	Convey("Given a minimum port number and deployment", t, func() {
		deployment := Development
		pn := 1000

		Convey("and a directory", func() {
			tempHome, err := os.MkdirTemp("", "temp_home")
			if err != nil {
				clog.Fatal(ctx, "err", err)
			}

			defer os.RemoveAll(tempHome)

			Convey("we can write a port to a config file in that directory", func() {
				writePortsToConfigFile(ctx, pn, tempHome, deployment)

				path := filepath.Join(tempHome, ".wr_config."+deployment+".yml")
				content, err := fl.ToString(path)
				expected := expectedManagerPort
				So(err, ShouldBeNil)
				So(content, ShouldContainSubstring, expected)

				Convey("and it still works with an existing config file", func() {
					writePortsToConfigFile(ctx, pn, tempHome, deployment)
					content, err := fl.ToString(path)
					expected := expectedManagerPort
					So(err, ShouldBeNil)
					So(content, ShouldContainSubstring, expected)
				})
			})
		})

		Convey("we can't write a port to a config file when given an invalid directory", func() {
			buff := setBufferLevel()
			writePortsToConfigFile(ctx, pn, "/root", deployment)
			checkErrorFromBuffer(buff, "could not open config file")
		})

		Convey("we can write dev and prod ports to config files in our home directory", func() {
			origHome := fsd.GetHome(ctx)
			tempHome, err := os.MkdirTemp("", "temp_home")
			if err != nil {
				clog.Fatal(ctx, "err", err)
			}
			os.Setenv("HOME", tempHome)
			defer os.Setenv("HOME", origHome)

			writePortsToConfigFiles(ctx, pn)

			prodDep := Production
			pathProd := filepath.Join(tempHome, ".wr_config."+prodDep+".yml")
			content, err := fl.ToString(pathProd)
			expected := expectedManagerPort
			So(err, ShouldBeNil)
			So(content, ShouldContainSubstring, expected)

			pathDev := filepath.Join(tempHome, ".wr_config."+deployment+".yml")
			content, err = fl.ToString(pathDev)
			expected = `managerport: "1002"`
			So(err, ShouldBeNil)
			So(content, ShouldContainSubstring, expected)
		})

		Convey("it cannot get a port checker if hostname is not localhost", func() {
			buff := setBufferLevel()
			checker := getPortChecker(ctx, "wr_test_localhost")
			So(checker, ShouldBeNil)
			checkErrorFromBuffer(buff, "localhost couldn't be connected to")
		})

		Convey("Given a checker", func() {
			checker := getPortChecker(ctx, localhost)
			Convey("it can find a port range", func() {
				tempHome, d1 := getTempHome(ctx)
				defer d1()

				fsi, err := ft.NewMockStdIn("y")
				So(err, ShouldBeNil)

				min := findPorts(ctx, checker)
				So(min, ShouldNotBeEmpty)

				fsi.RestoreStdIn()

				checkPortRange(tempHome, Development, fmt.Sprintf(`managerport: "%d"`, min+2))
			})

			Convey("it cannot update config file when user chooses to not use suggested ports", func() {
				tempHome, d1 := getTempHome(ctx)
				defer d1()

				fsi, err := ft.NewMockStdIn("n")
				So(err, ShouldBeNil)

				os.Setenv("WR_FATAL_EXIT_TEST", "1")
				defer os.Unsetenv("WR_FATAL_EXIT_TEST")
				min := findPorts(ctx, checker)
				So(min, ShouldEqual, 0)

				fsi.RestoreStdIn()

				_, err = os.Stat(getDeploymentConfigFile(tempHome, Development))
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Given a checker that isn't listening on hostname", func() {
			checker := getPortChecker(ctx, localhost)
			addr, err := net.ResolveTCPAddr("tcp", "localhost:1")
			So(err, ShouldBeNil)
			checker.Addr = addr

			Convey("it cannot get available port range", func() {
				buff := setBufferLevel()
				min, max := getAvailableRange(ctx, checker)
				So(min, ShouldBeZeroValue)
				So(max, ShouldBeZeroValue)
				checkErrorFromBuffer(buff, "localhost ports couldn't be checked")

				Convey("it cannot get port if available port range is not found", func() {
					buff := setBufferLevel()
					noPort := findPorts(ctx, checker)
					So(noPort, ShouldBeZeroValue)
					checkErrorFromBuffer(buff, "available localhost ports couldn't be checked")
				})
			})
		})
	})

	Convey("We can fix WR Manager umask", t, func() {
		Convey("When env variable is not set", func() {
			os.Unsetenv("WR_MANAGERUMASK")
			fixEnvManagerUmask()
			So(os.Getenv("WR_MANAGERUMASK"), ShouldBeEmpty)
		})

		Convey("When env variable is set but umask doesn't have 0 prefix", func() {
			os.Setenv("WR_MANAGERUMASK", "666")
			defer os.Unsetenv("WR_MANAGERUMASK")

			fixEnvManagerUmask()
			So(os.Getenv("WR_MANAGERUMASK"), ShouldEqual, "666")
		})

		Convey("When env variable is set but umask has 0 prefix", func() {
			os.Setenv("WR_MANAGERUMASK", "0666")
			defer os.Unsetenv("WR_MANAGERUMASK")

			fixEnvManagerUmask()
			So(os.Getenv("WR_MANAGERUMASK"), ShouldEqual, "666")
		})
	})

	Convey("Given a default wr config", t, func() {
		defConfig := loadDefaultConfig(ctx)
		So(defConfig, ShouldNotBeNil)
		So(defConfig.ManagerPort, ShouldBeEmpty)
		So(defConfig.Source("ManagerPort"), ShouldEqual, "default")
		So(defConfig.ManagerWeb, ShouldBeEmpty)

		Convey("it can check if deployment is production", func() {
			So(defConfig.IsProduction(), ShouldBeTrue)
		})

		Convey("it can calculate a unique port for the user", func() {
			Convey("for different deployments and port types", func() {
				uid := 1000
				So(calculatePort(ctx, defConfig, uid, localhost, "webi"), ShouldEqual, "5022")
				So(calculatePort(ctx, defConfig, uid, localhost, "cli"), ShouldEqual, "5021")
				So(calculatePort(ctx, defConfig, uid, localhost, "webi"), ShouldEqual, "5022")
				So(calculatePort(ctx, defConfig, uid, localhost, "cli"), ShouldEqual, "5021")
			})

			Convey("if the user id is a big number", func() {
				uid := 65534
				os.Setenv("WR_FATAL_EXIT_TEST", "1")
				defer os.Unsetenv("WR_FATAL_EXIT_TEST")

				Convey("and user chooses not to save ports to config file", func() {
					buff := clog.ToBufferAtLevel("crit")
					defer clog.ToDefault()

					_ = calculatePort(ctx, defConfig, uid, localhost, "webi")
					bufferStr := buff.String()
					So(bufferStr, ShouldContainSubstring, "fatal=true")
					So(bufferStr, ShouldNotContainSubstring, "caller=clog")
					So(bufferStr, ShouldContainSubstring, "stack=")
					So(bufferStr, ShouldContainSubstring, "you chose not to use suggested ports")
				})

				Convey("user chooses to save ports to config file", func() {
					_, d1 := getTempHome(ctx)
					defer d1()

					fsi, err := ft.NewMockStdIn("y")
					So(err, ShouldBeNil)

					portCli := calculatePort(ctx, defConfig, uid, localhost, "cli")

					fsi.RestoreStdIn()

					So(portCli, ShouldNotBeEmpty)
					cliPort, err := strconv.Atoi(portCli)
					So(err, ShouldBeNil)

					mgrweb, err := strconv.Atoi(defConfig.ManagerWeb)
					So(err, ShouldBeNil)

					So(mgrweb, ShouldEqual, cliPort+portsCmdWebDiff)
				})

				Convey("but not with a checker that isn't listening on hostname", func() {
					buff := clog.ToBufferAtLevel("crit")
					defer clog.ToDefault()
					_ = calculatePort(ctx, defConfig, uid, "wr_wrong_hostname", "webi")
					bufferStr := buff.String()
					So(bufferStr, ShouldContainSubstring, "fatal=true")
					So(bufferStr, ShouldContainSubstring, "localhost couldn't be connected to")
				})
			})
		})

		Convey("it can clone it", func() {
			clonedConfig := defConfig.clone()
			So(defConfig.ManagerHost, ShouldEqual, clonedConfig.ManagerHost)
			So(defConfig.CloudCIDR, ShouldEqual, clonedConfig.CloudCIDR)
			So(defConfig.CloudDNS, ShouldEqual, clonedConfig.CloudDNS)
			So(defConfig.CloudRAM, ShouldEqual, clonedConfig.CloudRAM)
			So(defConfig.CloudAutoConfirmDead, ShouldEqual, clonedConfig.CloudAutoConfirmDead)
		})

		Convey("and a user id, it can set the manager port", func() {
			uid := 1000
			So(defConfig.ManagerPort, ShouldBeEmpty)
			So(defConfig.ManagerWeb, ShouldBeEmpty)

			defConfig.setManagerPort(ctx, uid)

			So(defConfig.ManagerPort, ShouldEqual, "5021")
			So(defConfig.ManagerWeb, ShouldEqual, "5022")
		})

		Convey("it can convert the relative to Abs path for a config properties", func() {
			defConfig.convRelativeToAbsPath(&defConfig.ManagerPidFile)
			So(defConfig.ManagerPidFile, ShouldEqual, "~/.wr/pid")
		})

		Convey("it can convert the relative to Abs path for various properties", func() {
			So(defConfig.ManagerDir, ShouldEqual, "~/.wr")
			So(defConfig.ManagerDBFile, ShouldEqual, "db")
			So(defConfig.ManagerDBBkFile, ShouldEqual, "db_bk")
			So(defConfig.ManagerCAFile, ShouldEqual, "ca.pem")
			So(defConfig.ManagerCertFile, ShouldEqual, "cert.pem")
			So(defConfig.ManagerKeyFile, ShouldEqual, "key.pem")
			So(defConfig.ManagerTokenFile, ShouldEqual, "client.token")
			So(defConfig.ManagerLogFile, ShouldEqual, "log")
			So(defConfig.ManagerUploadDir, ShouldEqual, "uploads")

			defConfig.convRelativeToAbsPaths()

			So(defConfig.ManagerDBFile, ShouldEqual, "~/.wr/db")
			So(defConfig.ManagerDBBkFile, ShouldEqual, "~/.wr/db_bk")
			So(defConfig.ManagerCAFile, ShouldEqual, "~/.wr/ca.pem")
			So(defConfig.ManagerCertFile, ShouldEqual, "~/.wr/cert.pem")
			So(defConfig.ManagerKeyFile, ShouldEqual, "~/.wr/key.pem")
			So(defConfig.ManagerTokenFile, ShouldEqual, "~/.wr/client.token")
			So(defConfig.ManagerLogFile, ShouldEqual, "~/.wr/log")
			So(defConfig.ManagerUploadDir, ShouldEqual, "~/.wr/uploads")
		})

		Convey("and user id and deployment type, it can adjust config properties", func() {
			uid := 1000
			userHomeDir := fsd.GetHome(ctx)
			expectedManageDir := filepath.Join(userHomeDir, ".wr_"+Development)

			So(defConfig.ManagerDir, ShouldEqual, "~/.wr")

			defConfig.adjustConfigProperties(ctx, uid, Development)

			So(defConfig.ManagerDir, ShouldEqual, expectedManageDir)
			So(defConfig.ManagerPidFile, ShouldEqual, filepath.Join(expectedManageDir, "pid"))
			So(defConfig.ManagerCAFile, ShouldEqual, filepath.Join(expectedManageDir, "ca.pem"))
			So(defConfig.ManagerDBFile, ShouldEqual, filepath.Join(expectedManageDir, "db"))
			So(defConfig.ManagerPort, ShouldEqual, "5023")
		})

		Convey("it can also merge with another config", func() {
			otherConfig := loadDefaultConfig(ctx)
			otherConfig.ManagerPort = "2000"

			defConfig.merge(otherConfig, "default")
			So(otherConfig.ManagerPort, ShouldEqual, "2000")
			So(defConfig.ManagerPort, ShouldEqual, "2000")
		})

		Convey("it can be overridden with a config file given its path", func() {
			dir, err := os.MkdirTemp("", "wr_conf_test")
			So(err, ShouldBeNil)
			defer os.RemoveAll(dir)

			mport := "1234"
			mweb1 := "1235"
			mweb2 := "1236"
			path, path2, err := fileTestSetup(dir, mport, mweb1, mweb2)
			defer fileTestTeardown(path, path2)
			So(err, ShouldBeNil)

			defConfig.configLoadFromFile(ctx, path)
			So(defConfig.ManagerPort, ShouldEqual, mport)
			So(defConfig.Source("ManagerPort"), ShouldEqual, path)

			So(defConfig.ManagerWeb, ShouldEqual, mweb1)
			So(defConfig.Source("ManagerWeb"), ShouldEqual, path)

			defConfig.configLoadFromFile(ctx, path2)
			So(defConfig.ManagerPort, ShouldEqual, mport)
			So(defConfig.Source("ManagerPort"), ShouldEqual, path)

			So(defConfig.ManagerWeb, ShouldEqual, mweb2)
			So(defConfig.Source("ManagerWeb"), ShouldEqual, path2)

			_, _, err = fileTestSetup(dir, mport, mweb1, mweb2)
			So(err, ShouldNotBeNil)
		})

		Convey("these can be overridden with config files in WR_CONFIG_DIR", func() {
			uid := 1000
			dir, err := os.MkdirTemp("", "wr_conf_test")
			So(err, ShouldBeNil)
			defer os.RemoveAll(dir)

			mport := "1234"
			mweb1 := "1235"
			mweb2 := "1236"
			path, path2, err := fileTestSetup(dir, mport, mweb1, mweb2)
			defer fileTestTeardown(path, path2)
			So(err, ShouldBeNil)

			os.Setenv("WR_CONFIG_DIR", dir)
			defer func() {
				os.Unsetenv("WR_CONFIG_DIR")
			}()

			defConfig.mergeAllConfigFiles(ctx, uid, Development, "")

			So(defConfig.ManagerPort, ShouldEqual, mport)
			So(defConfig.Source("ManagerPort"), ShouldEqual, path)
			So(defConfig.ManagerWeb, ShouldEqual, mweb2)
			So(defConfig.Source("ManagerWeb"), ShouldEqual, path2)

			Convey("these can be overridden with config files in home dir", func() {
				tempHome, d1 := getTempHome(ctx)
				defer d1()

				mport := "1334"
				mweb1 := "1335"
				mweb2 := "1336"
				path3, path4, err := fileTestSetup(tempHome, mport, mweb1, mweb2)
				defer fileTestTeardown(path3, path4)
				So(err, ShouldBeNil)

				defConfig.mergeAllConfigFiles(ctx, uid, Development, "")
				So(defConfig.ManagerPort, ShouldEqual, mport)
				So(defConfig.Source("ManagerPort"), ShouldEqual, path3)
				So(defConfig.ManagerWeb, ShouldEqual, mweb2)
				So(defConfig.Source("ManagerWeb"), ShouldEqual, path4)

				Convey("not if home directory is empty", func() {
					defer func() {
						os.Setenv("HOME", os.Getenv("HOME"))
					}()
					os.Unsetenv("HOME")

					buff := setBufferLevel()
					defConfig.mergeAllConfigFiles(ctx, uid, Development, "")
					checkErrorFromBuffer(buff, "")
				})

				Convey("these can be overridden with config files in current dir", func() {
					pwd, err := os.MkdirTemp("", "temp_pwd")
					So(err, ShouldBeNil)
					mport = "1434"
					mweb1 = "1435"
					mweb2 = "1436"
					path5, path6, err := fileTestSetup(pwd, mport, mweb1, mweb2)
					defer fileTestTeardown(path5, path6)
					So(err, ShouldBeNil)

					defConfig.mergeAllConfigFiles(ctx, uid, Development, pwd)
					So(defConfig.ManagerPort, ShouldEqual, mport)
					So(defConfig.Source("ManagerPort"), ShouldEqual, path5)
					So(defConfig.ManagerWeb, ShouldEqual, mweb2)
					So(defConfig.Source("ManagerWeb"), ShouldEqual, path6)
				})
			})
		})
	})

	Convey("Set source on the change of a config field property", t, func() {
		type testConfig struct {
			ManagerHost        string  `default:"localhost"`
			ManagerUmask       int     `default:"7"`
			RandomFloatValue   float32 `default:"1.1"`
			ManagerSetDomainIP bool    `default:"false"`
		}

		oldC := &testConfig{}
		oldC.ManagerUmask = 4

		newC := &testConfig{}
		So(newC.ManagerUmask, ShouldEqual, 0)

		v := reflect.ValueOf(*oldC)
		typeOfC := v.Type()

		adrFieldString := reflect.ValueOf(newC).Elem().Field(0)
		adrFieldBool := reflect.ValueOf(newC).Elem().Field(1)
		adrFieldInt := reflect.ValueOf(newC).Elem().Field(2)
		adrFieldFloat := reflect.ValueOf(newC).Elem().Field(3)

		setSourceOnChangeProp(typeOfC, adrFieldString, v, 0)
		setSourceOnChangeProp(typeOfC, adrFieldBool, v, 1)
		setSourceOnChangeProp(typeOfC, adrFieldInt, v, 2)
		setSourceOnChangeProp(typeOfC, adrFieldFloat, v, 3)

		So(newC.ManagerUmask, ShouldEqual, 4)
		So(newC.ManagerHost, ShouldEqual, "")
	})

	Convey("It can get the default deployment", t, func() {
		Convey("when it is not running in the repo", func() {
			defDeployment := DefaultDeployment(ctx)
			So(defDeployment, ShouldEqual, Production)

			Convey("it can get overridden if WR_DEPLOYMENT env variable is set", func() {
				os.Setenv("WR_DEPLOYMENT", Development)
				defer os.Unsetenv("WR_DEPLOYMENT")

				defDeployment := DefaultDeployment(ctx)
				So(defDeployment, ShouldEqual, Development)

				Convey("not when env var is set to a wrong value", func() {
					os.Setenv("WR_DEPLOYMENT", "wrongDevelopment")
					defer os.Unsetenv("WR_DEPLOYMENT")

					defDeployment := DefaultDeployment(ctx)
					So(defDeployment, ShouldEqual, Production)
				})
			})
		})

		Convey("when it is running in the repo", func() {
			orgPWD, err := os.Getwd()
			So(err, ShouldBeNil)

			dir, err := os.MkdirTemp("", "wr_conf_test")
			So(err, ShouldBeNil)
			path := dir + "/jobqueue/server.go"
			err = os.MkdirAll(path, 0777)
			So(err, ShouldBeNil)
			defer func() {
				defer os.RemoveAll(dir)
			}()

			err = os.Chdir(dir)
			So(err, ShouldBeNil)
			defer func() {
				err = os.Chdir(orgPWD)
			}()

			defDeployment := DefaultDeployment(ctx)
			So(defDeployment, ShouldEqual, Development)

			Convey("it can get overridden if WR_DEPLOYMENT env variable is set", func() {
				os.Setenv("WR_DEPLOYMENT", Production)
				defer func() {
					os.Unsetenv("WR_DEPLOYMENT")
				}()

				defDeployment := DefaultDeployment(ctx)
				So(defDeployment, ShouldEqual, Production)
			})
		})
	})

	Convey("It can create a config with env vars", t, func() {
		os.Setenv("WR_MANAGERPORT", "1234")
		os.Setenv("WR_MANAGERUMASK", "77")
		os.Setenv("WR_MANAGERSETDOMAINIP", "true")
		defer func() {
			os.Unsetenv("WR_MANAGERPORT")
			os.Unsetenv("WR_MANAGERUMASK")
			os.Unsetenv("WR_MANAGERSETDOMAINIP")
		}()

		envVarConfig := getEnvVarsConfig(ctx)

		So(envVarConfig.ManagerPort, ShouldEqual, "1234")
		So(envVarConfig.ManagerWeb, ShouldBeEmpty)
		So(envVarConfig.ManagerUmask, ShouldEqual, 77)
		So(envVarConfig.ManagerSetDomainIP, ShouldBeTrue)
	})

	Convey("It can merge Default Config and Env Var config", t, func() {
		os.Setenv("WR_MANAGERPORT", "1234")
		os.Setenv("WR_MANAGERUMASK", "077")
		os.Setenv("WR_MANAGERSETDOMAINIP", "true")
		defer func() {
			os.Unsetenv("WR_MANAGERPORT")
			os.Unsetenv("WR_MANAGERUMASK")
			os.Unsetenv("WR_MANAGERSETDOMAINIP")
		}()

		mergedConfig := mergeDefaultAndEnvVarsConfigs(ctx)
		So(mergedConfig.ManagerPort, ShouldEqual, "1234")
		So(mergedConfig.Source("ManagerPort"), ShouldEqual, ConfigSourceEnvVar)
		So(mergedConfig.ManagerWeb, ShouldBeEmpty)
		So(mergedConfig.Source("ManagerWeb"), ShouldEqual, ConfigSourceDefault)
		So(mergedConfig.ManagerUmask, ShouldEqual, 77)
		So(mergedConfig.Source("ManagerUmask"), ShouldEqual, ConfigSourceEnvVar)
		So(mergedConfig.ManagerSetDomainIP, ShouldBeTrue)
		So(mergedConfig.Source("ManagerSetDomainIP"), ShouldEqual, ConfigSourceEnvVar)
	})

	Convey("It can merge all the configs and return a final config", t, func() {
		os.Setenv("WR_MANAGERUMASK", "077")
		defer os.Unsetenv("WR_MANAGERUMASK")

		uid := 1000
		pwd, err := os.MkdirTemp("", "temp_wd")
		So(err, ShouldBeNil)
		mport := "5434"
		mweb1 := "5435"
		mweb2 := "5436"
		path7, path8, err := fileTestSetup(pwd, mport, mweb1, mweb2)
		defer fileTestTeardown(path7, path8)
		So(err, ShouldBeNil)

		finalConfig := mergeAllConfigs(ctx, uid, Development, pwd)
		So(finalConfig.ManagerPort, ShouldEqual, mport)
		So(finalConfig.Source("ManagerPort"), ShouldEqual, path7)
		So(finalConfig.ManagerWeb, ShouldEqual, mweb2)
		So(finalConfig.Source("ManagerWeb"), ShouldEqual, path8)
		So(finalConfig.ManagerUmask, ShouldEqual, 77)
		So(finalConfig.Source("ManagerUmask"), ShouldEqual, ConfigSourceEnvVar)

		Convey("it can override deployment to default deployment if it's not development or production", func() {
			finalConfig := mergeAllConfigs(ctx, uid, "testDeployment", pwd)
			So(finalConfig.IsProduction(), ShouldBeTrue)
		})
	})

	Convey("ConfigLoad* gives default values to start with", t, func() {
		Convey("when loaded from parent directory", func() {
			config := ConfigLoadFromParentDir(ctx, Development)
			So(config, ShouldNotBeNil)
			So(config.IsProduction(), ShouldBeFalse)
		})

		Convey("when loaded from current directory", func() {
			config := ConfigLoadFromCurrentDir(ctx, "testing")
			So(config, ShouldNotBeNil)
			So(config.IsProduction(), ShouldBeTrue)
		})

		Convey("It can get the default config", func() {
			config := DefaultConfig(ctx)

			So(config, ShouldNotBeNil)
			So(config.IsProduction(), ShouldBeTrue)

			Convey("It can get the Default server", func() {
				server := DefaultServer(ctx)
				So(server, ShouldEqual, config.ManagerHost+":"+config.ManagerPort)
			})
		})
	})
}
