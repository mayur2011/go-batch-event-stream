package database

import (
	"context"

	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoClient struct {
	Client *mongo.Client
}

func NewMongoClient(ctx context.Context, uri string) (MongoClient, error) {

	//step-01: mongo uri and setup mongo client with mongo.NewClient
	mongoClient, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		log.Errorln("ERROR - Fail to get Mongo client", err)
		return MongoClient{}, err
	}

	//step-02: mongo client to connect with context timeout setting
	if err = mongoClient.Connect(ctx); err != nil {
		log.Errorln("ERROR - Fail to connect with Mongo client", err)
		return MongoClient{}, err
	}
	log.Info("MONGO CLIENT CONNECTED..!")

	//step-03: mongo client - perform ping
	err = mongoClient.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Errorln("ERROR - Fail to ping Mongo client", err)
		return MongoClient{}, err
	}

	return MongoClient{Client: mongoClient}, err
}

func (mc MongoClient) GetDatabase(dbname string) *mongo.Database {
	if mc.Client != nil {
		return mc.Client.Database(dbname)
	}
	return mc.Client.Database(dbname)
}

func (mc MongoClient) GetDatabaseNames(ctx context.Context, mongoClient *mongo.Client) []string {
	//step-04: mongo client to list database names
	db, err := mongoClient.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Println(err)
	}
	return db
}

func (mc MongoClient) DisconnectMongoClient(ctx context.Context) {
	if mc.Client != nil {
		mc.Client.Disconnect(ctx)
		log.Info("MONGO CLIENT DISCONNECTED..!")
	} else {
		log.Info("MONGO CLIENT EMPTY..!")
	}
}

// func ConnectDB(ctx context.Context, uri string, dbname string) *mongo.Database {
// 	connection := options.Client().ApplyURI(uri)
// 	client, err := mongo.Connect(ctx, connection)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return client.Database(dbname)
// }
