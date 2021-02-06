package balancer

import (
	"math/rand"
	"strconv"
	"testing"
)

func Test_robinBalancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	rb := &RoundRobinBalancer{offset: 0, partitions: []interface{}(s)}
	for i := 0; i < 50; i++ {
		y := rb.Balance()
		t.Log(y)
	}
}

func Test_LeastResBalancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	lr := &LeastResourcesBalancer{}
	lr.Init(s)
	for i := 0; i < 50; i++ {
		cost := uint64(i) * uint64(rand.Int63n(10))
		y := lr.Balance(cost)
		t.Log(y)
	}
}

func Test_WeightBalancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	weightList := []uint32{100, 200, 300, 400, 500, 600}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	lr := &WeightBalancer{}
	lr.Init(s, weightList)
	for i := 0; i < 50; i++ {
		y := lr.Balance()
		t.Log(y)
	}
}

func Test_HashBalancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	lr := &HashBalancer{partitions: s}
	for i := 0; i < 50; i++ {
		for j := 0; j < 2; j++ {
			y := lr.Balance([]byte(strconv.Itoa(i)))
			t.Log(i, j, y)
		}
	}
}

func Test_CRC32Balancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	lr := &CRC32Balancer{partitions: s}
	for i := 0; i < 50; i++ {
		for j := 0; j < 2; j++ {
			y := lr.Balance([]byte(strconv.Itoa(i)))
			t.Log(i, j, y)
		}
	}
}

func Test_Murmur2Balancer(t *testing.T) {
	ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
	s := make([]interface{}, len(ipList))
	for i, v := range ipList {
		s[i] = v
	}
	lr := &Murmur2Balancer{partitions: s}
	for i := 0; i < 50; i++ {
		for j := 0; j < 2; j++ {
			y := lr.Balance([]byte(strconv.Itoa(i)))
			t.Log(i, j, y)
		}
	}
}
