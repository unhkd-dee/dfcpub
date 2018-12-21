//
// # go test -v -run=NotFreed
//

package memsys_test

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
)

func TestNotFreed(t *testing.T) {
	const (
		iterations        = 1
		sglCount          = 70000
		sglPredefinedSize = cmn.MiB + 100
		sglFilledSize     = cmn.KiB * 12
	)

	mem := &memsys.Mem2{Period: time.Second * 20, MinFree: cmn.GiB, Name: "not-freed", Debug: verbose}
	mem.Init(true)
	for i := 0; i < iterations; i++ {
		sgls := make([]*memsys.SGL, 0)
		for j := 0; j < sglCount; j++ {
			sgl := mem.NewSGL(sglPredefinedSize)

			b := make([]byte, sglFilledSize)
			sgl.Write(b)
			sgls = append(sgls, sgl)
		}

		for _, sgl := range sgls {
			sgl.Free()
		}

		sgls = nil
		mem.Free(memsys.FreeSpec{
			ToOS:    true,
			Totally: true,
			MinSize: 1024,
		})

		// Just making sure that we run GC...
		runtime.GC()
		debug.FreeOSMemory()
	}

	time.Sleep(time.Second * 10) // change if want to have look into memory for longer period of time
}
