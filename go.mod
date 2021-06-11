module github.com/VertebrateResequencing/wr

go 1.16

require (
	cloud.google.com/go v0.81.0 // indirect
	code.cloudfoundry.org/bytefmt v0.0.0-20200131002437-cf55d5288a48
	github.com/StackExchange/wmi v0.0.0-20210224194228-fe8f1750fd46 // indirect
	github.com/VertebrateResequencing/muxfys/v4 v4.0.2
	github.com/VividCortex/ewma v1.2.0
	github.com/alexflint/go-filemutex v1.1.0 // indirect
	github.com/carbocation/runningvariance v0.0.0-20150817162428-fdcce8a03b6b
	github.com/creasty/defaults v1.5.1
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/docker/docker v20.10.6+incompatible
	github.com/docker/spdystream v0.2.0 // indirect
	github.com/elazarl/goproxy v0.0.0-20210110162100-a92cc753f88e // indirect
	github.com/fanatic/go-infoblox v0.0.0-20190709161059-e25f3820238c
	github.com/fatih/color v1.10.0
	github.com/go-ini/ini v1.62.0 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible
	github.com/golang/glog v0.0.0-20210429001901-424d2337a529 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gophercloud/gophercloud v0.17.0
	github.com/gophercloud/utils v0.0.0-20210323225332-7b186010c04f
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/gorilla/websocket v1.4.2
	github.com/grafov/bcast v0.0.0-20190217190352-1447f067e08d
	github.com/hanwen/go-fuse/v2 v2.1.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/howeyc/gopass v0.0.0-20190910152052-7cb4b85ec19c // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/jinzhu/configor v1.2.1
	github.com/jpillora/backoff v1.0.0
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/klauspost/cpuid/v2 v2.0.6 // indirect
	github.com/mattn/go-runewidth v0.0.12 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v6 v6.0.57 // indirect
	github.com/minio/sha256-simd v1.0.0 // indirect
	github.com/moby/term v0.0.0-20201216013528-df9cb8a40635 // indirect
	github.com/olekukonko/tablewriter v0.0.5
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/sftp v1.13.0
	github.com/rivo/uniseg v0.2.0 // indirect
	github.com/sasha-s/go-deadlock v0.2.1-0.20190427202633-1595213edefa
	github.com/sb10/l15h v0.0.0-20170510122137-64c488bf8e22
	github.com/sb10/waitgroup v0.0.0-20200305124406-7ed665007efa
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil v3.21.4+incompatible
	github.com/smartystreets/goconvey v1.6.4
	github.com/spf13/cobra v1.1.3
	github.com/tklauser/go-sysconf v0.3.5 // indirect
	github.com/ugorji/go/codec v1.2.5
	github.com/wtsi-ssg/wr v0.2.4
	go.etcd.io/bbolt v1.3.5
	golang.org/x/crypto v0.0.0-20210503195802-e9a32991a82e
	golang.org/x/oauth2 v0.0.0-20210427180440-81ed05c6b58c // indirect
	golang.org/x/term v0.0.0-20210503060354-a79de5458b56 // indirect
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.62.0 // indirect
	k8s.io/api v0.21.0
	k8s.io/apimachinery v0.21.0
	k8s.io/client-go v11.0.0+incompatible
	nanomsg.org/go-mangos v1.4.0
)

replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace github.com/sevlyar/go-daemon => github.com/sevlyar/go-daemon v0.1.1-0.20160925164401-01bb5caedcc4

replace sync => github.com/sasha-s/go-deadlock v0.2.1-0.20190427202633-1595213edefa // doesn't do anything?

replace github.com/sasha-s/go-deadlock => github.com/sasha-s/go-deadlock v0.2.1-0.20190427202633-1595213edefa

// we need a specific version of old k8s stuff
replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

// these versions needed to work with desired version of k8s
replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0
