package balancer

import (
	"errors"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"math/big"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
)

var ErrUnequalLength = errors.New("balancer: Unequal length")

type IConsecutiveBalancer interface {
	//Load balancing associated with front and rear outputs
	Balance() interface{}
}

type IHashBalancer interface {
	//Load balancing related to input
	Balance(interface{}) interface{}
}

// RoundRobin
type RoundRobin struct {
	// Use a 32 bits integer so RoundRobin values don't need to be aligned to
	// apply atomic increments.
	offset     uint32
	partitions []interface{} //TODO Waiting for golang Generic features, rewrite
}

// Balance satisfies the Balancer interface.
func (rr *RoundRobin) Balance() interface{} {
	length := uint32(len(rr.partitions))
	offset := atomic.AddUint32(&rr.offset, 1) - 1
	return rr.partitions[offset%length]
}

//WeightBalancer
type WeightBalancer struct {
	weightPair []WeightPair
	gcd        uint32
	curWeight  uint32
	curOffset  uint32
	lock       sync.Mutex
}

type WeightPair struct {
	partition interface{}
	weight    uint32
}

func (wb *WeightBalancer) Init(partitions []interface{}, weights []uint32) error {
	if len(partitions) != len(weights) {
		return ErrUnequalLength
	}
	wb.weightPair = make([]WeightPair, len(partitions))
	for i := 0; i < len(partitions); i++ {
		wb.weightPair[i].partition = partitions[i]
		wb.weightPair[i].weight = weights[i]
	}
	sort.Slice(wb.weightPair, func(i int, j int) bool {
		return wb.weightPair[i].weight > wb.weightPair[j].weight
	})
	// gcd
	bigInt := big.Int{}
	curNum := big.NewInt(int64(weights[0]))
	for _, i := range weights[1:] {
		curNum = bigInt.GCD(nil, nil, curNum, big.NewInt(int64(i)))
	}
	wb.gcd = uint32(curNum.Int64())
	wb.curWeight = wb.weightPair[0].weight
	wb.curOffset = 0
	return nil
}

func (wb *WeightBalancer) Balance() interface{} {
	wb.lock.Lock()
	defer wb.lock.Unlock()
	switch {
	case wb.curWeight > wb.gcd:
		wb.curWeight -= wb.gcd
		return wb.weightPair[wb.curOffset].partition
	default:
		wb.curOffset++
		wb.curOffset %= uint32(len(wb.weightPair))
		wb.curWeight = wb.weightPair[wb.curOffset].weight
		return wb.weightPair[wb.curOffset].partition
	}
}

//randomBalancer
type randomBalancer struct {
	partitions []interface{}
}

func (b *randomBalancer) Balance() interface{} {
	return b.partitions[rand.Int()%len(b.partitions)]
}

// LeastValueBalance  is a Balancer implementation that routes to the partition with least resources
// etc. Number of connections, size of memory and space occupied
type LeastResourcesBalance struct {
	counters []leastResourcesCounter
}

type leastResourcesCounter struct {
	partition interface{}
	cost      uint64
}

func (lb *LeastResourcesBalance) Init(partitions []interface{}) {
	lb.counters = make([]leastResourcesCounter, len(partitions))

	for i, p := range partitions {
		lb.counters[i].partition = p
		lb.counters[i].cost = 0
	}
}

func (lr *LeastResourcesBalance) Balance(curCost interface{}) interface{} {

	minCost := lr.counters[0].cost
	minIndex := 0

	for i, c := range lr.counters[1:] {
		if c.cost < minCost {
			minIndex = i + 1
			minCost = c.cost
		}
	}
	c := &lr.counters[minIndex]
	c.cost += curCost.(uint64)
	return c.partition
}

var (
	fnv1aPool = &sync.Pool{
		New: func() interface{} {
			return fnv.New32a()
		},
	}
)

// refer: https://github.com/segmentio/kafka-go/blob/master/balancer.go#L127
// The logic to calculate the partition is:
//
// 		hasher.Sum32() % len(partitions) => partition
//
type HashBalancer struct {
	partitions []interface{}
	Hasher     hash.Hash32

	// lock protects Hasher while calculating the hash code.  It is assumed that
	// the Hasher field is read-only once the Balancer is created, so as a
	// performance optimization, reads of the field are not protected.
	lock sync.Mutex
}

func (h *HashBalancer) Balance(key interface{}) interface{} {
	//! make sure key != nil
	hasher := h.Hasher
	if hasher != nil {
		h.lock.Lock()
		defer h.lock.Unlock()
	} else {
		hasher = fnv1aPool.Get().(hash.Hash32)
		defer fnv1aPool.Put(hasher)
	}

	hasher.Reset()
	if _, err := hasher.Write(key.([]byte)); err != nil {
		panic(err)
	}

	// uses same algorithm that Sarama's hashPartitioner uses
	// note the type conversions here.  if the uint32 hash code is not cast to
	// an int32, we do not get the same result as sarama.
	offset := int32(hasher.Sum32()) % int32(len(h.partitions))
	if offset < 0 {
		offset = -offset
	}

	return h.partitions[int(offset)]
}

type CRC32Balancer struct {
	partitions []interface{}
}

func (h *CRC32Balancer) Balance(key interface{}) interface{} {
	//! make sure key != nil
	idx := crc32.ChecksumIEEE(key.([]byte)) % uint32(len(h.partitions))
	return h.partitions[idx]
}

type Murmur2Balancer struct {
	partitions []interface{}
}

func (b *Murmur2Balancer) Balance(key interface{}) interface{} {
	//! make sure key != nil
	idx := (murmur2(key.([]byte)) & 0x7fffffff) % uint32(len(b.partitions))
	return b.partitions[idx]
}

// Go port of the Java library's murmur2 function.
// https://github.com/apache/kafka/blob/1.0/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L353
func murmur2(data []byte) uint32 {
	length := len(data)
	const (
		seed uint32 = 0x9747b28c
		// 'm' and 'r' are mixing constants generated offline.
		// They're not really 'magic', they just happen to work well.
		m = 0x5bd1e995
		r = 24
	)

	// Initialize the hash to a random value
	h := seed ^ uint32(length)
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := (uint32(data[i4+0]) & 0xff) + ((uint32(data[i4+1]) & 0xff) << 8) + ((uint32(data[i4+2]) & 0xff) << 16) + ((uint32(data[i4+3]) & 0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	// Handle the last few bytes of the input array
	extra := length % 4
	if extra >= 3 {
		h ^= (uint32(data[(length & ^3)+2]) & 0xff) << 16
	}
	if extra >= 2 {
		h ^= (uint32(data[(length & ^3)+1]) & 0xff) << 8
	}
	if extra >= 1 {
		h ^= uint32(data[length & ^3]) & 0xff
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return h
}
