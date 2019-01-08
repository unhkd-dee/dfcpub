// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

type (
	XactErase struct {
		// implements cmn.Xact a cmn.Runner interfaces
		cmn.XactDemandBase
		cmn.Named
		// runtime
		mpathChangeCh chan struct{}
		erasers       map[string]*eraser
		// init
		Bucket   string
		Mirror   cmn.MirrorConf
		Slab     *memsys.Slab2
		T        cluster.Target
		Bislocal bool
	}
	eraser struct { // one per mountpath
		parent    *XactErase
		mpathInfo *fs.MountpathInfo
		stopCh    chan struct{}
	}
)

/*
 * implements fs.PathRunner interface
 */
var _ fs.PathRunner = &XactErase{}

func (r *XactErase) SetID(id int64) { cmn.Assert(false) }

func (r *XactErase) ReqAddMountpath(mpath string)     { r.mpathChangeCh <- struct{}{} } // TODO: same for other "erasers"
func (r *XactErase) ReqRemoveMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactErase) ReqEnableMountpath(mpath string)  { r.mpathChangeCh <- struct{}{} }
func (r *XactErase) ReqDisableMountpath(mpath string) { r.mpathChangeCh <- struct{}{} }

//
// public methods
//

func (r *XactErase) Run() error {
	// init
	availablePaths, _ := fs.Mountpaths.Get()
	r.erasers = make(map[string]*eraser, len(availablePaths))
init:
	// start mpath erasers
	for mpath, mpathInfo := range availablePaths {
		var (
			mpathLC string
			eraser  = &eraser{parent: r, mpathInfo: mpathInfo}
		)
		if r.Bislocal {
			mpathLC = fs.Mountpaths.MakePathLocal(mpath, fs.ObjectType)
		} else {
			mpathLC = fs.Mountpaths.MakePathCloud(mpath, fs.ObjectType)
		}
		r.erasers[mpathLC] = eraser
		go eraser.jog()
	}
	// control loop
	for {
		select {
		case <-r.ChanAbort():
			r.stop()
			return fmt.Errorf("%s aborted, exiting", r)
		case <-r.mpathChangeCh:
			for _, eraser := range r.erasers {
				eraser.stop()
			}
			availablePaths, _ = fs.Mountpaths.Get()
			l := len(availablePaths)
			r.erasers = make(map[string]*eraser, l) // new erasers map
			if l == 0 {
				r.stop()
				return fmt.Errorf("%s no mountpaths, exiting", r)
			}
			goto init // reinitialize and keep running
		}
	}
}

func (r *XactErase) Stop(error) { r.Abort() } // call base method

//
// private methods
//

func (r *XactErase) stop() {
	if r.Finished() {
		glog.Warningf("%s is (already) not running", r)
		return
	}
	r.EndTime(time.Now())
	r.XactDemandBase.Stop()
	for _, eraser := range r.erasers {
		eraser.stop()
	}
}

//
// mpath eraser
//
func (j *eraser) stop() { j.stopCh <- struct{}{}; close(j.stopCh) }

func (j *eraser) jog() {
	j.stopCh = make(chan struct{}, 1)
	if err := filepath.Walk(lctx.bckTypeDir, j.walk); err != nil {
		s := err.Error()
		if strings.Contains(s, "xaction") {
			glog.Infof("%s: stopping traversal: %s", lctx.bckTypeDir, s)
		} else {
			glog.Errorf("%s: failed to traverse, err: %v", lctx.bckTypeDir, err)
		}
		return
	}
}
