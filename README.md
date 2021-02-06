# Balancer

Load balancing interface implementation library, which implements some common load balancing algorithms

* RoundRobinBalancer
* WeightBalancer
* LeastResourcesBalancer
* HashBalancer
* ....

## Example
```go
ipList := []string{"0.0.0.0", "0.0.0.1", "0.0.0.2", "0.0.0.3", "0.0.0.4", "0.0.0.5"}
s := make([]interface{}, len(ipList))
for i, v := range ipList {
    s[i] = v
}
rb := &RoundRobinBalancer{offset: 0, partitions: []interface{}(s)}
for i := 0; i < 50; i++ {
    y := rb.Balance()  // gen 
}
```

##

*refer: https://github.com/segmentio/kafka-go/blob/master/balancer.go*
