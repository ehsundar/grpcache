package grpcache

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"time"
)

type RedisCacheConfig struct {
	Host               string
	RequestUniqueIdExt func(req interface{}) (string, error)
	TTL                time.Duration
}

func NewRedisCacheInterceptor(config RedisCacheConfig) grpc.UnaryClientInterceptor {
	rdb, err := redis.DialURL(config.Host)
	if err != nil {
		panic(err)
	}

	return func(
		ctx context.Context,
		method string, req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		key, err := config.RequestUniqueIdExt(req)
		if err != nil {
			return err
		}

		cachedReply, err := rdb.Do("GET", key)
		if err != nil {
			if err == redis.ErrNil {
				err := invoker(ctx, method, req, reply, cc, opts...)

				serializedReply, err := proto.Marshal(reply.(proto.Message))
				if err != nil {
					return err
				}

				ttl := int32(config.TTL / time.Second)
				_, err = rdb.Do("SETEX", key, ttl, serializedReply)
				return err
			}
			return err
		}

		err = proto.Unmarshal(cachedReply.([]byte), reply.(proto.Message))
		if err != nil {
			return err
		}
		return nil
	}
}
