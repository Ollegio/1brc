package main

import (
	"bufio"
	"bytes"
	"fmt"
	"hash/fnv"
	"hash/maphash"
	"io"
	"log/slog"
	"slices"
	"sync"
	"unsafe"

	"golang.org/x/exp/maps"
)

type istats struct {
	min, max, sum int
	cnt           int
}

type hashstats struct {
	// hash  uint64
	key   string
	stats *istats
}

type myhashmap struct {
	buf  []*hashstats
	seed maphash.Seed
}

func NewHashMap() *myhashmap {
	return &myhashmap{
		buf:  make([]*hashstats, 1<<16),
		seed: maphash.MakeSeed(),
	}
}

func bytes2str(b []byte) string {
	return unsafe.String(&b[0], len(b))
}

func hash(data []byte) uint64 {
	var hash uint64 = 14695981039346656037
	ptr := unsafe.Pointer(unsafe.SliceData(data))
	i := 0
	for ; i < len(data)>>2; i++ {
		// hash ^= uint64(data[i]) + uint64(data[i+1])<<8 + uint64(data[i+2])<<16 + uint64(data[i+3])<<24
		hash ^= uint64(*(*uint32)(unsafe.Add(ptr, i<<2)))
		hash *= 1099511628211
	}
	rem := uint64(*(*uint32)(unsafe.Add(ptr, i<<2)))
	rem >>= 32 - (len(data)&3)*8
	hash ^= uint64(rem)
	hash *= 1099511628211

	return hash
}

func (h *myhashmap) getOrCreate(key []byte) (*istats, bool) {
	var existing *hashstats

	hash := fnv.New64a()
	hash.Write(key)
	idx := hash.Sum64() & uint64(len(h.buf)-1)
	for {
		existing = h.buf[idx]

		if existing == nil {
			existing = &hashstats{
				// hash:  hash,
				key:   string(key),
				stats: &istats{},
			}
			h.buf[idx] = existing

			return existing.stats, false
		}

		if bytes2str(key) != existing.key {
			idx++
			idx &= uint64(len(h.buf) - 1)
			continue
		}

		return existing.stats, true
	}
}

func solveBytesPool(input io.Reader, output io.Writer) {
	logger := slog.Default()
	logger.Handler()
	var err error

	numThreads := 12

	// resultCh := make(chan map[string]*istats, numThreads)
	resultCh := make(chan *myhashmap, numThreads)

	lineCh := make(chan []byte, 100)
	pool := sync.Pool{
		New: func() any {
			a := make([]byte, 4*1024*1024)
			return &a
		},
	}

	result := map[string]*istats{}

	wg := sync.WaitGroup{}
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go func() {
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

					s, ok := result.getOrCreate(first)
					if !ok {
						s.min = temp
						s.max = temp
					}
					s.cnt++
					s.min = min(s.min, temp)
					s.max = max(s.max, temp)
					s.sum = s.sum + temp
				}

				pool.Put(&lines)
			}

			resultCh <- result

			wg.Done()
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
	close(resultCh)

	for m := range resultCh {
		for _, v := range m.buf {
			if v == nil {
				continue
			}
			s, ok := result[v.key]
			if !ok {
				s = &istats{
					min: v.stats.min,
					max: v.stats.max,
				}
			}
			s.cnt += v.stats.cnt
			s.min = min(s.min, v.stats.min)
			s.max = max(s.max, v.stats.max)
			s.sum = s.sum + v.stats.sum
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
