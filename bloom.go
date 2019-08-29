// Copyright 2019 Enzo Yang.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package bloomf

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/go-redis/redis"
	"github.com/twmb/murmur3"
)

// PartBitCount the offset of bitmap in redis must smaller than 2^32
// https://redis.io/commands/SETBIT
const (
	PartBitCount uint64 = 1 << 32
)

// InfoKeys
const (
	InfoKeyName  = "name"
	InfoKeyN     = "n"
	InfoKeyP     = "p"
	InfoKeyM     = "m"
	InfoKeyK     = "k"
	InfoKeyParts = "parts"
)

// Errors
var (
	ErrDuplicated = errors.New("duplicated")
	ErrNotExists  = errors.New("not exists")
)

type partInfo struct {
	Name string
	Max  uint32
}

type location struct {
	Name   string
	Offset uint64
}

// Bloom bloomfilter class
type Bloom struct {
	client *redis.Client
	name   string
	n      uint64  // Estimated total element count
	p      float64 // Expected false negative rate

	m     uint64
	k     uint64
	parts []*partInfo
}

// New create and initialize bloomfilter
// client is instance of redis.Client
// name should be a unique key in redis instance
//     and it shouldn't exist before New function is called
// n is estimated amount of total element that will be marked in bloomfilter
// p is the expected false positive rate
//
// It returns Bloom instance if no error
// It returns error if duplicated or because of other errors such as bad network
func New(client *redis.Client, name string, n uint64, p float64) (bloom *Bloom, err error) {
	info, err := client.Pipeline().HGetAll(name).Result()
	if err == nil {
		if len(info) != 0 {
			return nil, ErrDuplicated
		}
	} else if err != redis.Nil {
		return nil, err
	}

	bloom = &Bloom{client: client, name: name, n: n, p: p}
	bloom.calculateParams()
	bloom.calculateParts()
	err = bloom.initStorage()
	if err != nil {
		return nil, err
	}

	err = bloom.saveParams()
	if err != nil {
		bloom.Clear()
		return nil, err
	}
	return bloom, nil
}

// Clear clear bloomfilter data in redis
func (b *Bloom) Clear() {
	pipe := b.client.Pipeline()
	pipe.Del(b.name)
	for _, p := range b.parts {
		pipe.Del(p.Name)
	}
	pipe.Exec()
}

// GetByName get bloomfilter by name
// client is instance of redis.Client
// name should be a unique key in redis instance
//     and it should exist before New function is called
//
// It returns Bloom instance if no error
// It returns error if not exists or because of other errors such as bad network
func GetByName(client *redis.Client, name string) (bloom *Bloom, err error) {
	info, err := client.Pipeline().HGetAll(name).Result()
	// fmt.Println(info)
	if err != nil {
		return nil, err
	} else if len(info) == 0 {
		return nil, ErrNotExists
	}

	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic")
			}
		}
	}()

	n, err := strconv.ParseUint(info[InfoKeyN], 10, 64)
	if err != nil {
		return nil, err
	}
	m, err := strconv.ParseUint(info[InfoKeyM], 10, 64)
	if err != nil {
		return nil, err
	}
	k, err := strconv.ParseUint(info[InfoKeyK], 10, 64)
	if err != nil {
		return nil, err
	}
	p, err := strconv.ParseFloat(info[InfoKeyP], 64)
	if err != nil {
		return nil, err
	}

	partString := info[InfoKeyParts]
	parts := make([]*partInfo, 0)
	err = json.Unmarshal([]byte(partString), &parts)
	if err != nil {
		return nil, err
	}

	bloom = &Bloom{
		name:  name,
		n:     n,
		p:     p,
		m:     m,
		k:     k,
		parts: parts,
	}
	return bloom, nil
}

// Add add element in bloomfilter
func (b *Bloom) Add(value []byte) error {
	locs := b.locations(value)
	pipe := b.client.Pipeline()
	for _, loc := range locs {
		pipe.SetBit(loc.Name, int64(loc.Offset), 1)
	}
	_, err := pipe.Exec()
	return err
}

// Exists check if element in bloomfilter
func (b *Bloom) Exists(value []byte) (bool, error) {
	locs := b.locations(value)
	pipe := b.client.Pipeline()
	res := make([]*redis.IntCmd, len(locs))
	for i, loc := range locs {
		cmd := pipe.GetBit(loc.Name, int64(loc.Offset))
		res[i] = cmd
	}
	_, err := pipe.Exec()
	if err != nil {
		return false, err
	}

	for _, cmd := range res {
		mark, err := cmd.Result()
		if err != nil {
			return false, err
		}
		if mark == 0 {
			return false, nil
		}
	}
	return true, nil
}

// calculateParams
func (b *Bloom) calculateParams() {
	n, p := b.n, b.p
	m := math.Ceil(float64(n) * math.Log(p) / math.Log(1.0/math.Pow(2.0, math.Ln2)))
	k := math.Ln2*m/float64(n) + 0.5

	b.m = uint64(m)
	b.k = uint64(k)
}

// calculateParts calculate how many bitmap should be created and its size
func (b *Bloom) calculateParts() {
	cnt := b.m/PartBitCount + 1
	b.parts = make([]*partInfo, cnt)
	for i := range b.parts {
		b.parts[i] = &partInfo{
			Name: fmt.Sprintf("%s:%d", b.name, i),
			Max:  math.MaxInt32,
		}
	}
	b.parts[cnt-1].Max = uint32(b.m % PartBitCount)
}

// initStorage create multiple bitmap in redis
// it may take a while if there are many
// https://redis.io/commands/SETBIT
func (b *Bloom) initStorage() (err error) {
	client, parts := b.client, b.parts

	defer func() {
		if r := recover(); r != nil {
			for _, p := range parts {
				client.Del(p.Name)
			}

			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic")
			}
		}
	}()

	for _, p := range parts {
		_, err = client.SetBit(p.Name, int64(p.Max), 0).Result()
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// saveParams save bloomfilter info in redis, so the bloomfilter can be restore next time
func (b *Bloom) saveParams() (err error) {
	info := make(map[string]interface{})
	info[InfoKeyName] = b.name
	info[InfoKeyN] = b.n
	info[InfoKeyP] = b.p
	info[InfoKeyM] = b.m
	info[InfoKeyK] = b.k

	parts, _ := json.Marshal(b.parts)
	info[InfoKeyParts] = parts

	_, err = b.client.HMSet(b.name, info).Result()
	return err
}

// rejectionSample normalize the hash value or throw it away
func (b *Bloom) rejectionSample(random uint64, m uint64) (uint64, bool) {
	if random > (math.MaxUint64-math.MaxUint64%m) || random == 0 {
		// if random fall in range above the hash result can not be evenly distributed
		// so skip this result
		return 0, false
	}
	return random % m, true
}

// hashes get k hash results of value
func (b *Bloom) hashes(value []byte) (hashes []uint64) {
	hashes = make([]uint64, b.k)
	var seed uint64

	var i uint64
	for i < b.k {
		seed = murmur3.SeedSum64(seed, value)
		hash, valid := b.rejectionSample(seed, b.m)
		if valid {
			hashes[i] = hash
			i++
		}
	}
	return hashes
}

// locations get k locations of value
// location means the offset in which redis bitmap
func (b *Bloom) locations(value []byte) []*location {
	hashes := b.hashes(value)
	locs := make([]*location, len(hashes))
	for i := range locs {
		p := hashes[i] / PartBitCount
		locs[i] = &location{
			Name:   b.parts[p].Name,
			Offset: hashes[i] % PartBitCount,
		}
	}
	return locs
}
