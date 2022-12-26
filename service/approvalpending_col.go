package service

import (
	"context"
	"encoding/json"
	"fmt"
	"go-batch-event-stream/dbiface"
	"go-batch-event-stream/domain"
	"go-batch-event-stream/queue"
	"go-batch-event-stream/util"

	amqp "github.com/rabbitmq/amqp091-go"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type OrderApprovalPendingCol struct {
	C dbiface.CollectionAPI
}

//Stream orders for commitment dates val.. is for CEDD validation only
func (store OrderApprovalPendingCol) StreamOrdersForApproval(startDate, endDate, mqName string, mqClientFunc func() queue.Client) {
	ctxToDo := context.TODO()
	var mqChan *amqp.Channel
	var mqueue amqp.Queue

	log.Info("FETCHING ORDERS WHERE START DT: " + startDate + " AND END DT: " + endDate)
	//Test only
	//orders := []string{"479179234", "842056317", "985139538"}
	//filter := bson.M{"identifier1": bson.M{"$in": orders}}

	filter := bson.M{"createdDate": bson.M{"$gte": startDate, "$lt": endDate}}

	projection := bson.D{
		primitive.E{Key: "identifier1", Value: 1},
		primitive.E{Key: "identifier2", Value: 1},
		primitive.E{Key: "createdDate", Value: 1},
		primitive.E{Key: "_id", Value: 0},
	}

	setProjection := options.Find().SetProjection(projection)
	cursor, err := store.C.Find(ctxToDo, filter, setProjection) //.Find(ctx, bson.M{})

	if err != nil {
		log.Fatal(err)
	}

	count := 0
	for cursor.Next(ctxToDo) {
		if count == 0 {
			//rmq setup
			mqClient := mqClientFunc()
			defer mqClient.CloseAMQPConnection()
			mqChan, err = mqClient.Conn.Channel()
			if err != nil {
				util.FailOnError("ERROR - Fail to open MQ-Channel", err)
			}
			defer mqChan.Close()
			//queue declare
			mqueue = queue.DeclareQueue(mqChan, mqName)
		}
		count++
		result := &domain.Timeliness{}
		err := cursor.Decode(result)
		if err != nil {
			util.FailOnError("ERROR - Fail to decode", err)
		}
		//fmt.Print(result.OrderId + "_" + result.Country + "--" + result.RecordDate + ":")

		message := domain.Message{
			SeqNum:  count,
			MsgType: "ORDER_APPROVAL_STATUS",
			OrderId: result.OrderId,
			Country: result.Country,
		}
		jsonByte, err := json.Marshal(message)
		if err != nil {
			util.FailOnError("ERROR - Fail to marshal the message", err)
		}
		//message publish
		err = queue.PublishMessage(context.TODO(), mqChan, mqueue, jsonByte)
		if err != nil {
			util.FailOnError("ERROR - Fail to publish the message", err)
		}
		log.Info(result.OrderId + "_" + result.Country + "--" + "RABBIT-MQ MESSAGE PUBLISHED..!")
	}
	fmt.Println()
	// once exhausted, close the cursor
	cursor.Close(ctxToDo)
}
