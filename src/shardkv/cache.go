package shardkv

import (
	"sync"
	"time"
)

type Cache struct {
	mu sync.RWMutex
	Data map[uint32]int
}

func newCache(cleanupInterval time.Duration,data map[uint32]int)*Cache{
	cache := &Cache{
		sync.RWMutex{},
		data,
	}

	return cache
}

func (c *Cache)set(key uint32,val int){
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data[key] = val
}

func (c *Cache)get(key uint32)int{
	c.mu.Lock()
	defer c.mu.Unlock()
	if v,ok := c.Data[key];ok{
		return v
	}else{
		return -1
	}
}

func (c *Cache) checkDup(cliId uint32,seqNum int) bool {
	if seq := c.get(cliId); seq >= seqNum{
		return true
	}
	return false
}

func (c *Cache)combine(cache map[uint32]int){
	c.mu.Lock()
	defer c.mu.Unlock()
	for key,value:= range cache{
		if v, ok := c.Data[key]; !ok || value > v {
			c.Data[key] = value
		}
	}
}

func (c *Cache)CopyData()map[uint32]int{
	c.mu.Lock()
	defer c.mu.Unlock()
	data := make(map[uint32]int)
	for k,v := range c.Data{
		data[k] = v
	}
	return data
}