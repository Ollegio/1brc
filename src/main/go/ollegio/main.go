package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"runtime/pprof"
	"runtime/trace"
)

func main() {
	ctx := context.Background()
	ctx.Err()
	logger := slog.Default()

	f, _ := os.Create("bytes.trace")
	defer f.Close()
	trace.Start(f)
	defer trace.Stop()

	f1, _ := os.Create("bytes.cpuprofile")
	defer f1.Close()
	pprof.StartCPUProfile(f1)
	defer pprof.StopCPUProfile()

	if len(os.Args) < 2 {
		panic("provide file name")
	}

	filename := os.Args[1]

	input, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := input.Close()
		if err != nil {
			logger.Error(err.Error())
		}
	}()

	var output io.Writer

	if len(os.Args) > 2 {
		f, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_WRONLY, 0644)
		output = f
		if err != nil {
			panic(err)
		}
		defer func() {
			err := f.Close()
			if err != nil {
				logger.Error(err.Error())
			}
		}()
	} else {
		output = os.Stdout
	}

	solveBytesPool(input, output)
}
