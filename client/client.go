// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package client

import (
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb/client/v2"
	"github.com/zxfonline/chanutil"
)

const (
	Precision     = "s"
	MaxBatchSize  = int(1e3)        //保证不超过influxdb 配置 max-row-limit = 10000
	CacheDuration = 2 * time.Second //内存缓存定时存当的数据时间
	ChanSize      = 1 << 15         //缓存chan长度32768，评估 CacheDuration 时间范围内能够写入的最大数据条数
)

var (
	Addr      = "http://localhost:8086/"
	Username  = "root"
	Password  = "root"
	Database  = "mydb"
	startOnce sync.Once
	stopOnce  sync.Once
	pool      chan *client.Point
	start     bool
	exitChan  chanutil.DoneChan
	wg        *sync.WaitGroup
)

func Excute(task *client.Point) {
	if !start {
		return
	}
	pool <- task
}

func Start(addr, username, password, database string, wg *sync.WaitGroup) {
	startOnce.Do(func() {
		Addr = addr
		Username = username
		Password = password
		Database = database
		ping()
		start = true
		exitChan = chanutil.NewDoneChan()
		pool = make(chan *client.Point, ChanSize)
		wg.Add(1)
		go func() {
			defer func() {
				recover()
				wg.Done()
			}()
			points := make([]*client.Point, 0, 1<<11)
			for ex := false; !ex; {
				for q := false; !q; {
					select {
					case point := <-pool:
						points = append(points, point)
						if len(points) >= MaxBatchSize {
							q = true
						}
					case <-time.After(CacheDuration):
						q = true
					}
				}
				if len(points) > 0 {
					writePoints(points)
					points = points[:0] //清除缓存
				}
				select {
				case <-exitChan:
					if len(pool) == 0 {
						ex = true
					}
				default:
				}
			}
		}()
	})
}

func Close() {
	stopOnce.Do(func() {
		if !start {
			return
		}
		exitChan.SetDone()
		start = false
	})
}
func ping() {
	if c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     Addr,
		Username: Username,
		Password: Password,
	}); err != nil {
		log.Fatalf("ping err:%v", err)
	} else {
		defer c.Close()
		if t, _, err := c.Ping(0); err != nil {
			log.Fatalf("ping err:%v", err)
		} else {
			log.Printf("ping cost time:%v", t)
		}
	}
}
func writePoints(points []*client.Point) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("write err:%v", err)
		}
	}()
	if c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     Addr,
		Username: Username,
		Password: Password,
	}); err != nil {
		panic(err)
	} else {
		defer c.Close()
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  Database,
			Precision: Precision,
		})
		bp.AddPoints(points)
		if err := c.Write(bp); err != nil {
			panic(err)
		}
	}
}
