package queue

import (
	"context"
	"go-batch-event-stream/util"

	amqp "github.com/rabbitmq/amqp091-go"
)

func DeclareQueue(ch *amqp.Channel, name string) amqp.Queue {
	// declare queue
	q, err := ch.QueueDeclare(name, true, false, false, false, nil)
	if err != nil {
		util.FailOnError("ERROR - fail to declare a queue", err)
	}
	return q
}

func PublishMessage(ctx context.Context, ch *amqp.Channel, q amqp.Queue, msg []byte) error {
	return ch.PublishWithContext(ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msg,
		})
}
