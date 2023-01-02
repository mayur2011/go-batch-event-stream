package dbiface

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// a collection API interface
type CollectionAPI interface {
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, error)
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult
	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
}
