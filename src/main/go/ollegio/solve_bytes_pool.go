package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"slices"
	"sync"

	"golang.org/x/exp/maps"
)

type istats struct {
	min, max, sum int
	cnt           int
}

type hashstats struct {
	key           string
	min, max, sum int
	cnt           int
}

type myhashmap struct {
	buf  []hashstats
	seed maphash.Seed
}

func NewHashMap() *myhashmap {
	return &myhashmap{
		buf:  make([]hashstats, 1<<16),
		seed: maphash.MakeSeed(),
	}
}

func (h *myhashmap) update(key []byte, temp int) {
	hash := maphash.Bytes(h.seed, key)
	idx := hash & uint64(len(h.buf)-1)
	for {
		if h.buf[idx].key == "" {
			h.buf[idx].key = string(key)

			h.buf[idx].min = temp
			h.buf[idx].max = temp
			h.buf[idx].cnt = 1

			return
		}

		if string(key) != h.buf[idx].key {
			idx++
			idx &= uint64(len(h.buf) - 1)
			continue
		}

		h.buf[idx].cnt++
		h.buf[idx].min = min(h.buf[idx].min, temp)
		h.buf[idx].max = max(h.buf[idx].max, temp)
		h.buf[idx].sum = h.buf[idx].sum + temp
		return
	}
}

func solveBytesPool(input io.Reader, output io.Writer) {
	logger := slog.Default()
	logger.Handler()
	var err error

	numThreads := 12

	resultCh := make([]*myhashmap, numThreads)

	lineCh := make(chan []byte, 100)
	pool := sync.Pool{
		New: func() any {
			a := make([]byte, 4*1024*1024)
			return &a
		},
	}

	result := map[string]istats{}

	wg := sync.WaitGroup{}
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go func() {
			defer wg.Done()

			result := NewHashMap()

			for lines := range lineCh {
				cur := 0
				next := 0
				for next != len(lines) {
					next = bytes.IndexByte(lines[cur:], '\n')
					if next == -1 {
						next = len(lines)
					} else {
						next = cur + next
					}
					line := lines[cur:next]
					cur = next + 1

					sep := bytes.IndexByte(line, ';')
					first, second := line[:sep], line[sep+1:]

					var itemp int
					sign := 1
					if second[0] == '-' {
						sign = -1
					}
					for _, c := range second {
						if c == '.' || c == '-' {
							continue
						}
						itemp = itemp*10 + int(c-'0')
					}
					temp := itemp * sign

					result.update(first, temp)
				}

				pool.Put(&lines)
			}

			resultCh[i] = result
		}()
	}

	rem := 0
	buf := *pool.Get().(*[]byte)
	for {
		var rn int
		rn, err = input.Read(buf[rem:])
		if err != nil {
			break
		}

		idx := bytes.LastIndexByte(buf[:rem+rn], '\n')

		lineCh <- buf[:idx]
		newbuf := *pool.Get().(*[]byte)
		rem = copy(newbuf, buf[idx+1:])
		buf = newbuf
	}

	close(lineCh)
	wg.Wait()

	for _, m := range resultCh {
		for _, v := range m.buf {
			if v.key == "" {
				continue
			}
			s, ok := result[v.key]
			if !ok {
				s = istats{
					min: v.min,
					max: v.max,
				}
			}
			s.cnt += v.cnt
			s.min = min(s.min, v.min)
			s.max = max(s.max, v.max)
			s.sum = s.sum + v.sum
			result[v.key] = s
		}
	}

	keys := maps.Keys(result)
	slices.Sort(keys)

	w := bufio.NewWriter(output)
	for _, k := range keys {
		stats := result[k]
		line := fmt.Sprintf("%v;%.1f;%.1f;%.1f\n", k, float64(stats.min)/10, float64(stats.sum)/float64(stats.cnt)/10, float64(stats.max)/10)
		_, err = w.WriteString(line)
		if err != nil {
			panic(err)
		}
	}

	err = w.Flush()
	if err != nil {
		panic(err)
	}
}
