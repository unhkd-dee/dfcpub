// Package registry provides core functionality for the AIStore extended actions xreg.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package xreg

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/query"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
)

type (
	// Providing default pre/post hooks
	BaseBckEntry struct{}

	// Serves to return the result of renewing
	DummyEntry struct {
		xact cluster.Xact
	}

	BucketEntry interface {
		BaseEntry
		// pre-renew: returns true iff the current active one exists and is either
		// - ok to keep running as is, or
		// - has been renew(ed) and is still ok
		PreRenewHook(previousEntry BucketEntry) (keep bool, err error)
		// post-renew hook
		PostRenewHook(previousEntry BucketEntry)
	}

	// BckFactory is an interface to provider a new instance of BucketEntry interface.
	BckFactory interface {
		// New should create empty stub for bucket xaction that could be started
		// with `Start()` method.
		New(args *XactArgs) BucketEntry
		Kind() string
	}

	XactArgs struct {
		Ctx    context.Context
		T      cluster.Target
		UUID   string
		Phase  string
		Custom interface{} // Additional arguments that are specific for a given xaction.
	}

	DirPromoteArgs struct {
		Dir    string
		Params *cmn.ActValPromote
	}

	TransferBckArgs struct {
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
		DM      *bundle.DataMover
		DP      cluster.LomReaderProvider
		Meta    *cmn.Bck2BckMsg
	}

	ListRangeArgs struct {
		Ctx      context.Context
		UUID     string
		RangeMsg *cmn.RangeMsg
		ListMsg  *cmn.ListMsg
	}

	BckRenameArgs struct {
		RebID   xaction.RebID
		BckFrom *cluster.Bck
		BckTo   *cluster.Bck
	}
)

//////////////////
// BaseBckEntry //
//////////////////

func (*BaseBckEntry) PreRenewHook(previousEntry BucketEntry) (keep bool, err error) {
	e := previousEntry.Get()
	_, keep = e.(xaction.XactDemand)
	return
}

func (*BaseBckEntry) PostRenewHook(_ BucketEntry) {}

////////////////
// DummyEntry //
////////////////

// interface guard
var (
	_ BaseEntry = (*DummyEntry)(nil)
)

func (*DummyEntry) Start(_ cmn.Bck) error { debug.Assert(false); return nil }
func (*DummyEntry) Kind() string          { debug.Assert(false); return "" }
func (d *DummyEntry) Get() cluster.Xact   { return d.xact }

//////////////
// registry //
//////////////

func RegFactory(entry BckFactory) { defaultReg.regFactory(entry) }

func (r *registry) regFactory(entry BckFactory) {
	debug.Assert(xaction.XactsDtor[entry.Kind()].Type == xaction.XactTypeBck)

	// It is expected that registrations happen at the init time. Therefore, it
	// is safe to assume that no `RenewXYZ` will happen before all xactions
	// are registered. Thus, no locking is needed.
	r.bckXacts[entry.Kind()] = entry
}

// RenewBucketXact is general function to renew bucket xaction without any
// additional or specific parameters.
func RenewBucketXact(kind string, bck *cluster.Bck, args *XactArgs) (res RenewRes) {
	return defaultReg.renewBucketXact(kind, bck, args)
}

func (r *registry) renewBucketXact(kind string, bck *cluster.Bck, args *XactArgs) (res RenewRes) {
	if args == nil {
		args = &XactArgs{}
	}
	e := r.bckXacts[kind].New(args)
	res = r.renewBckXact(e, bck)
	if res.Err != nil {
		return
	}
	if !res.IsNew {
		xact := res.Entry.Get()
		// NOTE: make sure existing on-demand is active to prevent it from (idle) expiration
		//       (see demand.go hkcb())
		if xactDemand, ok := xact.(xaction.XactDemand); ok {
			xactDemand.IncPending()
			xactDemand.DecPending()
		}
	}
	return
}

func RenewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) RenewRes {
	return defaultReg.renewECEncode(t, bck, uuid, phase)
}

func (r *registry) renewECEncode(t cluster.Target, bck *cluster.Bck, uuid, phase string) RenewRes {
	return r.renewBucketXact(cmn.ActECEncode, bck, &XactArgs{T: t, UUID: uuid, Phase: phase})
}

func RenewMakeNCopies(t cluster.Target, tag string) { defaultReg.renewMakeNCopies(t, tag) }

func (r *registry) renewMakeNCopies(t cluster.Target, tag string) {
	var (
		cfg      = cmn.GCO.Get()
		bmd      = t.Bowner().Get()
		provider = cmn.ProviderAIS
	)
	bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		if bck.Props.Mirror.Enabled {
			res := r.renewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
			if res.Err == nil {
				xact := res.Entry.Get()
				go xact.Run()
			}
		}
		return false
	})
	// TODO: remote ais
	for name, ns := range cfg.Backend.Providers {
		bmd.Range(&name, &ns, func(bck *cluster.Bck) bool {
			if bck.Props.Mirror.Enabled {
				res := r.renewBckMakeNCopies(t, bck, tag, int(bck.Props.Mirror.Copies))
				if res.Err == nil {
					xact := res.Entry.Get()
					go xact.Run()
				}
			}
			return false
		})
	}
}

func RenewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid string, copies int) (res RenewRes) {
	return defaultReg.renewBckMakeNCopies(t, bck, uuid, copies)
}

func (r *registry) renewBckMakeNCopies(t cluster.Target, bck *cluster.Bck, uuid string, copies int) (res RenewRes) {
	e := r.bckXacts[cmn.ActMakeNCopies].New(&XactArgs{T: t, UUID: uuid, Custom: copies})
	res = r.renewBckXact(e, bck)
	if res.Err != nil {
		return
	}
	if !res.IsNew {
		res.Err = fmt.Errorf("%s xaction already running", e.Kind())
	}
	return
}

func RenewDirPromote(t cluster.Target, bck *cluster.Bck, dir string, params *cmn.ActValPromote) RenewRes {
	return defaultReg.renewDirPromote(t, bck, dir, params)
}

func (r *registry) renewDirPromote(t cluster.Target, bck *cluster.Bck, dir string, params *cmn.ActValPromote) RenewRes {
	return r.renewBucketXact(cmn.ActPromote, bck, &XactArgs{
		T: t,
		Custom: &DirPromoteArgs{
			Dir:    dir,
			Params: params,
		},
	})
}

func RenewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) error {
	res := defaultReg.renewBckLoadLomCache(t, uuid, bck)
	return res.Err
}

func (r *registry) renewBckLoadLomCache(t cluster.Target, uuid string, bck *cluster.Bck) RenewRes {
	return r.renewBucketXact(cmn.ActLoadLomCache, bck, &XactArgs{T: t, UUID: uuid})
}

func RenewPutMirror(t cluster.Target, lom *cluster.LOM) RenewRes {
	return defaultReg.renewPutMirror(t, lom)
}

func (r *registry) renewPutMirror(t cluster.Target, lom *cluster.LOM) RenewRes {
	return r.renewBucketXact(cmn.ActPutCopies, lom.Bck(), &XactArgs{T: t, Custom: lom})
}

func RenewPutArchive(uuid string, t cluster.Target, bckFrom *cluster.Bck) RenewRes {
	return defaultReg.renewPutArchive(uuid, t, bckFrom)
}

func (r *registry) renewPutArchive(uuid string, t cluster.Target, bckFrom *cluster.Bck) RenewRes {
	return r.renewBucketXact(cmn.ActArchive, bckFrom, &XactArgs{
		T:    t,
		UUID: uuid,
	})
}

func RenewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) RenewRes {
	return defaultReg.renewTransferBck(t, bckFrom, bckTo, uuid, kind, phase, dm, dp, meta)
}

func (r *registry) renewTransferBck(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid, kind,
	phase string, dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) RenewRes {
	return r.renewBucketXact(kind, bckTo, &XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
		Custom: &TransferBckArgs{
			BckFrom: bckFrom,
			BckTo:   bckTo,
			DM:      dm,
			DP:      dp,
			Meta:    meta,
		},
	})
}

func RenewEvictDelete(t cluster.Target, kind string, bck *cluster.Bck, args *ListRangeArgs) RenewRes {
	return defaultReg.renewEvictDelete(t, kind, bck, args)
}

func (r *registry) renewEvictDelete(t cluster.Target, kind string, bck *cluster.Bck, args *ListRangeArgs) RenewRes {
	return r.renewBucketXact(kind, bck, &XactArgs{T: t, UUID: args.UUID, Custom: args})
}

func RenewPrefetch(t cluster.Target, bck *cluster.Bck, args *ListRangeArgs) RenewRes {
	return defaultReg.renewPrefetch(t, bck, args)
}

func (r *registry) renewPrefetch(t cluster.Target, bck *cluster.Bck, args *ListRangeArgs) RenewRes {
	return r.renewBucketXact(cmn.ActPrefetch, bck, &XactArgs{T: t, UUID: args.UUID, Custom: args})
}

func RenewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck, uuid string, rmdVersion int64, phase string) RenewRes {
	return defaultReg.renewBckRename(t, bckFrom, bckTo, uuid, rmdVersion, phase)
}

func (r *registry) renewBckRename(t cluster.Target, bckFrom, bckTo *cluster.Bck,
	uuid string, rmdVersion int64, phase string) RenewRes {
	return r.renewBucketXact(cmn.ActMoveBck, bckTo, &XactArgs{
		T:     t,
		UUID:  uuid,
		Phase: phase,
		Custom: &BckRenameArgs{
			RebID:   xaction.RebID(rmdVersion),
			BckFrom: bckFrom,
			BckTo:   bckTo,
		},
	})
}

func RenewObjList(t cluster.Target, bck *cluster.Bck, uuid string, msg *cmn.SelectMsg) RenewRes {
	return defaultReg.renewObjList(t, bck, uuid, msg)
}

func (r *registry) renewObjList(t cluster.Target, bck *cluster.Bck, uuid string, msg *cmn.SelectMsg) RenewRes {
	xact := r.getXact(uuid)
	if xact == nil || xact.Finished() {
		e := r.bckXacts[cmn.ActList].New(&XactArgs{
			Ctx:    context.Background(),
			T:      t,
			UUID:   uuid,
			Custom: msg,
		})
		return r.renewBckXact(e, bck, uuid)
	}
	return RenewRes{&DummyEntry{xact}, nil, false}
}

func RenewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	return defaultReg.renewBckSummary(ctx, t, bck, msg)
}

func (r *registry) renewBckSummary(ctx context.Context, t cluster.Target, bck *cluster.Bck, msg *cmn.BucketSummaryMsg) error {
	r.entries.mtx.Lock()
	err := r.entries.del(msg.UUID)
	r.entries.mtx.Unlock()
	if err != nil {
		return err
	}
	e := &bckSummaryTaskEntry{ctx: ctx, t: t, uuid: msg.UUID, msg: msg}
	if err := e.Start(bck.Bck); err != nil {
		return err
	}
	r.add(e)
	return nil
}

func RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	return defaultReg.RenewQuery(ctx, t, q, msg)
}

func (r *registry) RenewQuery(ctx context.Context, t cluster.Target, q *query.ObjectsQuery, msg *cmn.SelectMsg) RenewRes {
	debug.Assert(msg.UUID != "")
	if xact := query.Registry.Get(msg.UUID); xact != nil {
		if !xact.Aborted() {
			return RenewRes{&DummyEntry{xact}, nil, false}
		}
		query.Registry.Delete(msg.UUID)
	}
	r.entries.mtx.Lock()
	err := r.entries.del(msg.UUID)
	r.entries.mtx.Unlock()
	if err != nil {
		return RenewRes{&DummyEntry{nil}, err, false}
	}
	e := &queEntry{ctx: ctx, t: t, query: q, msg: msg}
	return r.renewBckXact(e, q.BckSource.Bck)
}
