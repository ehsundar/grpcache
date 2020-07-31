package grpcache

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"time"
)

type RedisCacheConfig struct {
	Host               string
	RequestUniqueIdExt func(req interface{}) string
	TTL                time.Duration
}

func NewRedisCacheInterceptor(config RedisCacheConfig) grpc.UnaryClientInterceptor {
	rdb, err := redis.DialURL(config.Host)
	if err != nil {
		panic(err)
	}

	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		requestKey := config.RequestUniqueIdExt(req)
		cacheKey := fmt.Sprintf("grpcache:%s:%s", method, requestKey)

		cachedReply, err := rdb.Do("GET", cacheKey)
		if cachedReply == nil {
			err := invoker(ctx, method, req, reply, cc, opts...)

			serializedReply, err := proto.Marshal(reply.(proto.Message))
			if err != nil {
				return err
			}

			ttl := int32(config.TTL / time.Second)
			_, err = rdb.Do("SETEX", cacheKey, ttl, serializedReply)
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
