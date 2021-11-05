// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk_test

import (
	"testing"

	"github.com/NVIDIA/aistore/hk"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestHousekeeper(t *testing.T) {
	hk.TestInit()
	go hk.DefaultHK.Run()
	hk.WaitRunning()
	RegisterFailHandler(Fail)
	RunSpecs(t, "Housekeeper Suite")
}
