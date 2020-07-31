package grpcache

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"strings"
	"time"
)

type MethodCacheSpec struct {
	Enabled     bool
	TTL         time.Duration
	UniqueIdExt func(request interface{}) string
}

type RedisCacheConfig struct {
	RedisURL    string
	ServiceName string
	MethodSpecs map[string]MethodCacheSpec
}

type redisCache struct {
	config RedisCacheConfig
	rdb    redis.Conn
}

type RedisCache interface {
	GetInterceptor() grpc.UnaryClientInterceptor
	RunCleanerListener() error
}

func NewRedisCache(config RedisCacheConfig) RedisCache {
	rdb, err := redis.DialURL(config.RedisURL)
	if err != nil {
		panic(err)
	}

	return &redisCache{
		config: config,
		rdb:    rdb,
	}
}

func (rc *redisCache) GetInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		spec := rc.getMethodSpec(method)
		if spec == nil {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
		requestKey := spec.UniqueIdExt(req)
		cacheKey := fmt.Sprintf("grpcache:%s:%s:%s", rc.config.ServiceName, method, requestKey)

		cachedReply, err := rc.rdb.Do("GET", cacheKey)
		if cachedReply == nil {
			err := invoker(ctx, method, req, reply, cc, opts...)

			serializedReply, err := proto.Marshal(reply.(proto.Message))
			if err != nil {
				return err
			}

			ttl := int32(spec.TTL / time.Second)
			_, err = rc.rdb.Do("SETEX", cacheKey, ttl, serializedReply)
			return err
		} else {
			if err != nil {
				return err
			}
		}

		err = proto.Unmarshal(cachedReply.([]byte), reply.(proto.Message))
		if err != nil {
			return err
		}
		return nil
	}
}

func (rc *redisCache) RunCleanerListener() error {
	psc := redis.PubSubConn{Conn: rc.rdb}
	chanPattern := fmt.Sprintf("grpache:%s:*", rc.config.ServiceName)
	if err := psc.PSubscribe(chanPattern); err != nil {
		return err
	}

	go func() {
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				route := v.Channel // grpcache:post_storage:fsdfkjfe-324
				routeSegments := strings.Split(route, ":")
				uniqueId := routeSegments[2]
				pattern := fmt.Sprintf("grpcache:%s:*:%s", rc.config.ServiceName, uniqueId) // grpcache:post_storage:GetPost:fsdfkjfe-324

				keys, err := redis.Strings(rc.rdb.Do("KEYS", pattern))
				if err != nil {
					// TODO: log error
					fmt.Printf("error in chan sub (keys): %s, %v", chanPattern, err)
				}
				_, err = rc.rdb.Do("DEL", keys)
				if err != nil {
					// TODO: log error
					fmt.Printf("error in chan sub (del): %s, %v", chanPattern, err)
				}
			case error:
				// TODO: log error
				fmt.Printf("error in chan sub: %s, %v", chanPattern, v)
			}
		}
	}()
	return nil
}

func (rc *redisCache) getMethodSpec(method string) *MethodCacheSpec {
	spec, ok := rc.config.MethodSpecs[method]
	if !ok {
		return nil
	}
	return &spec
}
