// Copyright 2019 Enzo Yang.  All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

package bloomf

import (
	"os"
	"testing"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

var client *redis.Client

func TestMain(m *testing.M) {
	srv, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer srv.Close()
	client = redis.NewClient(&redis.Options{
		Addr: srv.Addr(),
	})
	os.Exit(m.Run())
}

func TestCalculateParams(t *testing.T) {
	tests := []struct {
		n uint64
		p float64
		m uint64
		k uint64
	}{
		{n: 4000, p: 0.0000001, m: 134191, k: 23},
		{n: 100, p: 0.02, m: 815, k: 6},
		{n: 100000000, p: 0.0001, m: 1917011676, k: 13},
	}

	for _, test := range tests {
		bloom := &Bloom{name: "bl", n: test.n, p: test.p}
		bloom.calculateParams()
		assert.Equal(t, bloom.m, test.m, "m should be equal")
		assert.Equal(t, bloom.k, test.k, "k should be equal")
	}
}

func TestCalculateParts(t *testing.T) {
	tests := []struct {
		name        string
		m           uint64
		parts       int
		lastPart    string
		lastPartMax uint32
	}{
		{name: "bf1", m: 1024, parts: 1, lastPart: "bf1:0", lastPartMax: 1024},
		{name: "bf2", m: PartBitCount*5 + 1024, parts: 6, lastPart: "bf2:5", lastPartMax: 1024},
	}

	for _, test := range tests {
		bloom := &Bloom{name: test.name, m: test.m}
		bloom.calculateParts()
		assert.Equal(t, len(bloom.parts), test.parts, "part size should be valid")
		assert.Equal(t, bloom.parts[test.parts-1].Name, test.lastPart, "last part name should be valid")
		assert.Equal(t, bloom.parts[test.parts-1].Max, test.lastPartMax, "last part max should be valid")
	}
}

func TestCreateBloomFilter(t *testing.T) {
	tests := []struct {
		name string
		n    uint64
		p    float64
	}{
		{name: "bf1", n: 100000, p: 0.01},
	}

	defer func() {
		client.FlushDB()
	}()

	for _, test := range tests {
		bl1, err := New(client, test.name, test.n, test.p)
		assert.Nil(t, err, "should not be error")
		bl2, err := GetByName(client, test.name)
		assert.Nil(t, err, "should not be error")
		assert.Equal(t, bl1.m, bl2.m, "m should be equal")
		assert.Equal(t, bl1.k, bl2.k, "k should be equal")
		assert.Equal(t, len(bl1.parts), len(bl2.parts), "parts should be equal")
		assert.Equal(t, int(client.Exists(bl1.parts[0].Name).Val()), 1, "part should exist in redis")
	}
}

func TestHashes(t *testing.T) {
	tests := []struct {
		value string
	}{
		{value: "aaaaaa"},
		{value: "b"},
		{value: "abcdefg"},
	}

	defer func() {
		client.FlushDB()
	}()

	bl, err := New(client, "bf", 100000, 0.001)
	assert.Nil(t, err, "should not be error")
	for _, test := range tests {
		hashes := bl.hashes([]byte(test.value))
		t.Logf("hashes: %v", hashes)
		assert.Equal(t, len(hashes), int(bl.k), "should be k hashes")
	}
}

func TestLocations(t *testing.T) {
	tests := []struct {
		value string
	}{
		{value: "aaaaaa"},
		{value: "b"},
		{value: "abcdefg"},
	}

	defer func() {
		client.FlushDB()
	}()

	bl, err := New(client, "bf", 100000, 0.001)
	assert.Nil(t, err, "should not be error")
	for _, test := range tests {
		locations := bl.locations([]byte(test.value))
		t.Logf("locations: %v", locations)
		assert.Equal(t, len(locations), int(bl.k), "should be k locations")
	}
}

func TestAddAndCheck(t *testing.T) {
	tests := []struct {
		value string
	}{
		{value: "aaaa"},
		{value: "bbbb"},
		{value: "cccc"},
		{value: "dddd"},
	}

	defer func() {
		client.FlushDB()
	}()

	bl, err := New(client, "bf", 100000, 0.001)
	assert.Nil(t, err, "should not be error")
	for _, test := range tests {
		ex, err := bl.Exists([]byte(test.value))
		assert.Nil(t, err, "should not be error")
		assert.False(t, ex, "should not exist")
		err = bl.Add([]byte(test.value))
		assert.Nil(t, err, "should not be error")
		ex, err = bl.Exists([]byte(test.value))
		assert.Nil(t, err, "should not be error")
		assert.True(t, ex, "should exist")
	}
}
