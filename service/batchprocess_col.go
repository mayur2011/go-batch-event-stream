package service

import (
	"context"
	"go-batch-event-stream/dbiface"
	"go-batch-event-stream/domain"

	log "github.com/sirupsen/logrus"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BatchProcessStore struct {
	C dbiface.CollectionAPI
}

func (store BatchProcessStore) GetBatchProcessInfo(name string) (*domain.BatchEventSteamType, error) {
	filterBatchDoc := bson.M{"jobName": name}

	batch := &domain.BatchEventSteamType{}

	projectBatch := bson.D{
		primitive.E{Key: "jobName", Value: 1},
		primitive.E{Key: "nextRunTime", Value: 1},
		primitive.E{Key: "sourceCollection", Value: 1},
		primitive.E{Key: "jobType", Value: 1},
	}

	setProjectBatch := options.FindOne().SetProjection(projectBatch)

	batchResult := store.C.FindOne(context.TODO(), filterBatchDoc, setProjectBatch)
	err := batchResult.Decode(batch)
	if err != nil {
		return &domain.BatchEventSteamType{}, err
	}

	if batch.CollectionName == "" {
		log.Fatalln("NOT_FOUND - No Mongo collection to query..!")
	}
	log.Info("STREAM-NAME: "+batch.Name, " SOURCING-FROM: "+batch.CollectionName, " LAST-RUNTIME: "+batch.NextRunTime, " BATCH-ID: "+batch.ID)
	return batch, nil
}

func (store BatchProcessStore) UpdateBatchProcess(id, lastModified, nextRunTime string) (*mongo.UpdateResult, error) {
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		log.Fatalln("ERROR - No Mongo Object ID from hex string")
	}
	updateID := bson.M{"_id": objID}
	updateBatch := bson.M{"$set": bson.M{"processedDate": lastModified, "nextRunTime": nextRunTime}}

	updateResult, err := store.C.UpdateOne(context.TODO(), updateID, updateBatch)
	if err != nil {
		log.Fatalln("ERROR - to update the batch with Next Run time", err)
		return &mongo.UpdateResult{}, err
	}
	log.Info("UPDATED RESULT: ", updateResult.ModifiedCount, " NEXT RUNTIME:"+nextRunTime)
	return updateResult, nil
}
