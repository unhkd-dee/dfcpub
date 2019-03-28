// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	downloadDescAllPrefix = "downloader-test-integration"
	downloadDescAllRegex  = "^" + downloadDescAllPrefix
)

var (
	downloadDescCurPrefix = fmt.Sprintf("%s-%d-", downloadDescAllPrefix, os.Getpid())
)

func generateDownloadDesc() string {
	return downloadDescCurPrefix + time.Now().Format(time.RFC3339Nano)
}

func clearDownloadList(t *testing.T) {
	listDownload, err := api.DownloadGetList(tutils.DefaultBaseAPIParams(t), downloadDescAllRegex)
	tutils.CheckFatal(err, t)

	for k := range listDownload {
		tutils.Logf("Removing: %v...\n", k)
		err := api.DownloadCancel(tutils.DefaultBaseAPIParams(t), k)
		tutils.CheckFatal(err, t)
	}
}

func checkDownloadList(t *testing.T, expNumEntries ...int) {
	defer clearDownloadList(t)

	expNumEntriesVal := 1
	if len(expNumEntries) > 0 {
		expNumEntriesVal = expNumEntries[0]
	}

	listDownload, err := api.DownloadGetList(tutils.DefaultBaseAPIParams(t), downloadDescAllRegex)
	tutils.CheckFatal(err, t)
	actEntries := len(listDownload)

	if expNumEntriesVal != actEntries {
		t.Fatalf("Incorrect # of downloader entries: expected %d, actual %d", expNumEntriesVal, actEntries)
	}
}

func waitForDownload(t *testing.T, id string) {
	for {
		all := true
		if resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
			if resp.Finished != resp.Total {
				all = false
			}
		}

		if all {
			break
		}
		time.Sleep(time.Second)
	}

}

func TestDownloadSingle(t *testing.T) {
	var (
		proxyURL      = getPrimaryURL(t, proxyURLReadOnly)
		bucket        = TestLocalBucketName
		objname       = "object"
		objnameSecond = "object-second"

		// links below don't contain protocols to test that no error occurs
		// in case they are missing.
		link      = "storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz"
		linkSmall = "github.com/NVIDIA/aistore"
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, objname, link)
	tutils.CheckError(err, t)

	time.Sleep(time.Second)

	// Schedule second object
	idSecond, err := api.DownloadSingle(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, objnameSecond, link)
	tutils.CheckError(err, t)

	// Cancel second object
	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), idSecond)
	tutils.CheckError(err, t)

	resp, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckError(err, t)

	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckError(err, t)

	time.Sleep(time.Second)

	if resp, err = api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
		t.Errorf("expected error when getting status for link that is not being downloaded: %v", resp)
	}

	if err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id); err == nil {
		t.Error("expected error when cancelling for link that is not being downloaded and is not in queue")
	}

	id, err = api.DownloadSingle(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, objname, linkSmall)
	tutils.CheckError(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckError(err, t)
	if len(objs) != 1 || objs[0] != objname {
		t.Errorf("expected single object (%s), got: %s", objname, objs)
	}

	checkDownloadList(t)
}

func TestDownloadRange(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName

		// base does not have following slash to test that it is prepended and
		// it is concatenated with template properly.
		base     = "storage.googleapis.com/lpr-vision"
		template = "imagenet/imagenet_train-{000000..000007}.tgz"
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadRange(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, base, template)
	tutils.CheckFatal(err, t)

	time.Sleep(3 * time.Second)

	err = api.DownloadCancel(tutils.DefaultBaseAPIParams(t), id)
	tutils.CheckFatal(err, t)

	checkDownloadList(t, 0)
}

func TestDownloadMultiMap(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		m        = map[string]string{
			"ais": "https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"k8s": "https://raw.githubusercontent.com/kubernetes/kubernetes/master/README.md",
		}
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, m)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != len(m) {
		t.Errorf("expected objects (%s), got: %s", m, objs)
	}

	checkDownloadList(t)
}

func TestDownloadMultiList(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		l        = []string{
			"https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
			"https://raw.githubusercontent.com/kubernetes/kubernetes/master/LICENSE?query=values",
		}
		expectedObjs = []string{"LICENSE", "README.md"}
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(tutils.DefaultBaseAPIParams(t), generateDownloadDesc(), bucket, l)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if !reflect.DeepEqual(objs, expectedObjs) {
		t.Errorf("expected objs: %s, got: %s", expectedObjs, objs)
	}

	checkDownloadList(t)
}

func TestDownloadTimeout(t *testing.T) {
	var (
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		bucket   = TestLocalBucketName
		objname  = "object"
		link     = "https://storage.googleapis.com/lpr-vision/imagenet/imagenet_train-000001.tgz"
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	body := cmn.DlSingleBody{
		DlObj: cmn.DlObj{
			Objname: objname,
			Link:    link,
		},
	}
	body.Bucket = bucket
	body.Description = generateDownloadDesc()
	body.Timeout = "1ms" // super small timeout to see if the request will be canceled

	id, err := api.DownloadSingleWithParam(tutils.DefaultBaseAPIParams(t), body)
	tutils.CheckFatal(err, t)

	time.Sleep(time.Second)

	if _, err := api.DownloadStatus(tutils.DefaultBaseAPIParams(t), id); err == nil {
		// TODO: we should get response that some files has been canceled or not finished.
		// For now we cannot do that since we don't collect information about
		// task being canceled.
		// t.Errorf("expected error when getting status for link that is not being downloaded: %s", string(resp))
		tutils.Logf("%v\n", err)
	}

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.LocalBs, "", 0)
	tutils.CheckFatal(err, t)
	if len(objs) != 0 {
		t.Errorf("expected 0 objects, got: %s", objs)
	}

	checkDownloadList(t)
}

func TestDownloadCloud(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
		bucket     = clibucket

		fileCnt = 5
		prefix  = "imagenet/imagenet_train-"
		suffix  = ".tgz"
	)

	if !isCloudBucket(t, proxyURL, bucket) {
		t.Skip("test requires a cloud bucket")
	}

	clearDownloadList(t)

	tutils.CleanCloudBucket(t, proxyURL, bucket, prefix)
	defer tutils.CleanCloudBucket(t, proxyURL, bucket, prefix)

	expectedObjs := make([]string, 0, fileCnt)
	for i := 0; i < fileCnt; i++ {
		reader, err := tutils.NewRandReader(cmn.MiB, false /* withHash */)
		tutils.CheckFatal(err, t)

		objName := fmt.Sprintf("%s%0*d%s", prefix, 5, i, suffix)
		err = api.PutObject(api.PutObjectArgs{
			BaseParams:     baseParams,
			Bucket:         bucket,
			BucketProvider: cmn.CloudBs,
			Object:         objName,
			Reader:         reader,
		})
		tutils.CheckFatal(err, t)

		expectedObjs = append(expectedObjs, objName)
	}

	// Test download
	err := api.EvictList(baseParams, bucket, cmn.CloudBs, expectedObjs, true, 0)
	tutils.CheckFatal(err, t)

	id, err := api.DownloadCloud(baseParams, bucket, prefix, suffix)
	tutils.CheckFatal(err, t)

	waitForDownload(t, id)

	objs, err := tutils.ListObjects(proxyURL, bucket, cmn.CloudBs, prefix, 0)
	tutils.CheckFatal(err, t)
	if !reflect.DeepEqual(objs, expectedObjs) {
		t.Errorf("expected objs: %s, got: %s", expectedObjs, objs)
	}
	checkDownloadList(t)

	// Test cancellation
	err = api.EvictList(baseParams, bucket, cmn.CloudBs, expectedObjs, true, 0)
	tutils.CheckFatal(err, t)

	id, err = api.DownloadCloud(baseParams, bucket, prefix, suffix)
	tutils.CheckFatal(err, t)

	time.Sleep(200 * time.Millisecond)

	err = api.DownloadCancel(baseParams, id)
	tutils.CheckFatal(err, t)

	checkDownloadList(t, 0)
}

func TestDownloadStatus(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		bucket        = TestLocalBucketName
		params        = tutils.DefaultBaseAPIParams(t)
		shortFileName = "shortFile"
		m             = metadata{t: t}
	)

	m.saveClusterState()
	if m.originalTargetCount < 2 {
		t.Errorf("At least 2 targets are required.")
		return
	}

	longFileName := tutils.GenerateNotConflictingObjectName(shortFileName, "longFile", TestLocalBucketName, &m.smap)

	files := map[string]string{
		shortFileName: "https://raw.githubusercontent.com/NVIDIA/aistore/master/README.md",
		longFileName:  "http://releases.ubuntu.com/18.04/ubuntu-18.04.2-desktop-amd64.iso",
	}

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, m.proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, m.proxyURL, bucket)

	id, err := api.DownloadMulti(params, generateDownloadDesc(), bucket, files)
	tutils.CheckFatal(err, t)

	// Wait for the short file to be downloaded
	err = tutils.WaitForObjectToBeDowloaded(shortFileName, TestLocalBucketName, params, 2*time.Second)
	tutils.CheckFatal(err, t)

	resp, err := api.DownloadStatus(params, id)
	tutils.CheckFatal(err, t)

	if resp.Total != 2 {
		t.Errorf("expected %d objects, got %d", 2, resp.Total)
	}
	if resp.Finished != 1 {
		t.Errorf("expected the short file to be downloaded")
	}
	if len(resp.CurrentTasks) != 1 {
		t.Fatal("did not expect the long file to be already downloaded")
	}
	if resp.CurrentTasks[0].Name != longFileName {
		t.Errorf("invalid file name in status message, expected: %s, got: %s", longFileName, resp.CurrentTasks[0].Name)
	}

	checkDownloadList(t)
}

func TestDownloadStatusError(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	var (
		bucket   = TestLocalBucketName
		proxyURL = getPrimaryURL(t, proxyURLReadOnly)
		params   = tutils.DefaultBaseAPIParams(t)
		files    = map[string]string{
			"invalidURL":   "http://some.invalid.url",
			"notFoundFile": "http://releases.ubuntu.com/18.04/amd65.iso",
		}
	)

	clearDownloadList(t)

	// Create local bucket
	tutils.CreateFreshLocalBucket(t, proxyURL, bucket)
	defer tutils.DestroyLocalBucket(t, proxyURL, bucket)

	id, err := api.DownloadMulti(params, generateDownloadDesc(), bucket, files)
	tutils.CheckFatal(err, t)

	// Wait to make sure both files were processed by downloader
	time.Sleep(1 * time.Second)

	resp, err := api.DownloadStatus(params, id)
	tutils.CheckFatal(err, t)

	if resp.Total != len(files) {
		t.Errorf("expected %d objects, got %d", len(files), resp.Total)
	}
	if resp.Finished != 0 {
		t.Errorf("expected 0 files to be finished")
	}
	if len(resp.Errs) != len(files) {
		t.Fatalf("expected 2 downloading errors, but got: %d errors: %v", len(resp.Errs), resp.Errs)
	}

	invalidAddressCausedError := resp.Errs[0].Name == "invalidURL" || resp.Errs[1].Name == "invalidURL"
	notFoundFileCausedError := resp.Errs[0].Name == "notFoundFile" || resp.Errs[1].Name == "notFoundFile"

	if !(invalidAddressCausedError && notFoundFileCausedError) {
		t.Errorf("expected objects that cause errors to be (%s, %s), but got: (%s, %s)",
			"invalidURL", "notFoundFile", resp.Errs[0].Name, resp.Errs[1].Name)
	}

	checkDownloadList(t)
}
