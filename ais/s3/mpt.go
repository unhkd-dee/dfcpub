// Package s3 provides Amazon S3 compatibility layer
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package s3

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// NOTE: xattr stores only the (*) marked attributes
type (
	MptPart struct {
		MD5  string // MD5 of the part (*)
		FQN  string // FQN of the corresponding workfile
		Size int64  // part size in bytes (*)
		Num  int64  // part number (*)
	}
	mpt struct {
		bckName string
		objName string
		parts   []*MptPart // by part number
		ctime   time.Time  // InitUpload time
	}
	uploads map[string]*mpt // by upload ID
)

var (
	up uploads
	mu sync.RWMutex
)

func Init() { up = make(uploads) }

// Start miltipart upload
func InitUpload(id, bckName, objName string) {
	mu.Lock()
	up[id] = &mpt{
		bckName: bckName,
		objName: objName,
		parts:   make([]*MptPart, 0, iniCapParts),
		ctime:   time.Now(),
	}
	mu.Unlock()
}

// Add part to an active upload.
// Some clients may omit size and md5. Only partNum is must-have.
// md5 and fqn is filled by a target after successful saving the data to a workfile.
func AddPart(id string, npart *MptPart) (err error) {
	mu.Lock()
	upload, ok := up[id]
	if !ok {
		err = fmt.Errorf("upload %q not found (%s, %d)", id, npart.FQN, npart.Num)
	} else {
		upload.parts = append(upload.parts, npart)
	}
	mu.Unlock()
	return
}

// TODO: compare non-zero sizes (note: s3cmd sends 0) and part.ETag as well, if specified
func CheckParts(id string, parts []*PartInfo) ([]*MptPart, error) {
	mu.RLock()
	defer mu.RUnlock()
	upload, ok := up[id]
	if !ok {
		return nil, fmt.Errorf("upload %q not found", id)
	}
	// first, check that all parts are present
	var prev = int64(-1)
	for _, part := range parts {
		debug.Assert(part.PartNumber > prev) // must ascend
		if upload.getPart(part.PartNumber) == nil {
			return nil, fmt.Errorf("upload %q: part %d not found", id, part.PartNumber)
		}
		prev = part.PartNumber
	}
	// copy (to work on it with no locks)
	nparts := make([]*MptPart, 0, len(parts))
	for _, part := range parts {
		nparts = append(nparts, upload.getPart(part.PartNumber))
	}
	return nparts, nil
}

func ParsePartNum(s string) (partNum int64, err error) {
	partNum, err = strconv.ParseInt(s, 10, 16)
	if err != nil {
		err = fmt.Errorf("invalid part number %q (must be in 1-%d range): %v", s, MaxPartsPerUpload, err)
	}
	return
}

// Return a sum of upload part sizes.
// Used on upload completion to calculate the final size of the object.
func ObjSize(id string) (size int64, err error) {
	mu.RLock()
	upload, ok := up[id]
	if !ok {
		err = fmt.Errorf("upload %q not found", id)
	} else {
		for _, part := range upload.parts {
			size += part.Size
		}
	}
	mu.RUnlock()
	return
}

// remove all temp files and delete from the map
// if completed (i.e., not aborted): store xattr
func FinishUpload(id, fqn string, aborted bool) {
	mu.Lock()
	upload, ok := up[id]
	if !ok {
		mu.Unlock()
		debug.AssertMsg(aborted, fqn+": "+id)
		return
	}
	if !aborted {
		err := storeMptXattr(fqn, upload)
		debug.AssertNoErr(err)
	}
	for _, part := range upload.parts {
		_ = os.RemoveAll(part.FQN)
	}
	delete(up, id)
	mu.Unlock()
}

// Returns the info about active upload with ID
func GetUpload(id string) (upload *mpt, err error) {
	mu.RLock()
	upload, err = _getup(id)
	mu.RUnlock()
	return
}

func _getup(id string) (*mpt, error) {
	upload, ok := up[id]
	if !ok {
		return nil, fmt.Errorf("upload %q not found", id)
	}
	return upload, nil
}

// Returns true if there is active upload with ID
func UploadExists(id string) bool {
	mu.RLock()
	_, ok := up[id]
	mu.RUnlock()
	return ok
}

func ListUploads(bckName, idMarker string, maxUploads int) (result *ListMptUploadsResult) {
	mu.RLock()
	results := make([]UploadInfoResult, 0, len(up))
	for id, mpt := range up {
		results = append(results, UploadInfoResult{Key: mpt.objName, UploadID: id, Initiated: mpt.ctime})
	}
	mu.RUnlock()

	sort.Slice(results, func(i int, j int) bool {
		return results[i].Initiated.Before(results[j].Initiated)
	})

	var from int
	if idMarker != "" {
		// truncate
		for i, res := range results {
			if res.UploadID == idMarker {
				from = i + 1
				break
			}
			copy(results, results[from:])
			results = results[:len(results)-from]
		}
	}
	if maxUploads > 0 && len(results) > maxUploads {
		results = results[:maxUploads]
	}
	result = &ListMptUploadsResult{Bucket: bckName, Uploads: results, IsTruncated: from > 0}
	return
}

func ListParts(id string, lom *cluster.LOM) ([]*PartInfo, error) {
	mu.RLock()
	mpt, err := _getup(id)
	if err != nil {
		// TODO: check
		mpt, err = LoadMptXattr(lom.FQN)
		if err != nil || mpt == nil {
			mu.RUnlock()
			return nil, err
		}
		mpt.bckName, mpt.objName = lom.Bck().Name, lom.ObjName
		mpt.ctime = lom.Atime()
	}
	parts := make([]*PartInfo, 0, len(mpt.parts))
	for _, part := range mpt.parts {
		parts = append(parts, &PartInfo{ETag: part.MD5, PartNumber: part.Num, Size: part.Size})
	}
	mu.RUnlock()
	return parts, nil
}
