package shardctrler

import (
	"sync"
	"time"
)

type Cache struct {
	mu sync.RWMutex
	Data map[uint32]bool
}

func newCache(cleanupInterval time.Duration,data map[uint32]bool)*Cache{
	cache := &Cache{
		sync.RWMutex{},
		data,
	}

	return cache
}

func (c *Cache)set(key uint32){
	c.mu.Lock()
	defer c.mu.Unlock()
	c.Data[key] = true
}

func (c *Cache)get(key uint32)bool{
	c.mu.Lock()
	defer c.mu.Unlock()
	if v,ok := c.Data[key];ok{
		return v
	}else{
		return false
	}
}