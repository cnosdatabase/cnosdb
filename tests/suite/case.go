package suite

import (
	"errors"
	"github.com/cnosdb/cnosdb/tests"
	"github.com/cnosdb/cnosdb/tests/suite/iot"
	"sync"
	"sync/atomic"
	"testing"
)

type Step struct {
	Name   string
	Query  string
	Result Results
}

func (s *Step) Run(caseName string, server tests.Server, t *testing.T) {
	te := func(e error) {
		if e != nil {
			t.Errorf("Case: %s, Step: %s, Error: %v", caseName, s.Name, e)
		}
	}
	//fmt.Printf(`[case Run] error func generating"%s"`, "\n")
	resStr, err := server.Query(s.Query)
	//fmt.Printf(`[case Run]"%s", "%s" "%s""%s"`, resStr, "\n", err, "\n")
	te(err)
	var res Results
	te(res.Unmarshal(resStr))
	//fmt.Printf("[case Run] equal pending...\n")
	if !s.Result.Equal(res) {
		te(errors.New("Mismatch. "))
	}
}

func (s *Step) ResCode(server tests.Server) {
	resStr, err := server.Query(s.Query)
	if err != nil {
		panic(err)
	}
	if err = s.Result.Unmarshal(resStr); err != nil {
		panic(err)
	}
	s.Result.ToCode("a")
}

type Case struct {
	Name  string
	Steps []Step
}

func (c *Case) Run(server tests.Server, t *testing.T) {
	for _, s := range c.Steps {
		s.Run(c.Name, server, t)
	}
}

type Suite struct {
	Gen      iot.Generator
	Cases    []Case
	Parallel bool
	flag     ParallelFlag
}

func (s *Suite) Run(t *testing.T) {
	s.Gen.Init()
	s.Gen.Run()
	if s.Parallel {
		var wg sync.WaitGroup
		wg.Add(s.Gen.Parallel)
		for i := 0; i < s.Gen.Parallel; i++ {
			go func() {
				f := int32(0)
				for {
					f = s.flag.Add()
					if f >= int32(len(s.Cases)) {
						break
					}
					s.Cases[f].Run(s.Gen.Server, t)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	} else {
		for _, c := range s.Cases {
			c.Run(s.Gen.Server, t)
		}
	}
}

type ParallelFlag struct {
	flag int32
}

func (p *ParallelFlag) Add() int32 {
	return atomic.AddInt32(&p.flag, 1) - 1 // begin with zero
}
