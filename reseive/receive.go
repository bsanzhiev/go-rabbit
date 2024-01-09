package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Обратите внимание, что здесь мы также объявляем очередь.
	// Поскольку мы можем запустить потребителя раньше издателя, мы хотим убедиться,
	// что очередь существует, прежде чем мы попытаемся получить из нее сообщения.
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unusued
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // name
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		for d := range msgs {
			log.Printf("Reseived a message: %s\n", d.Body)
		}
	}()
	log.Printf(" [*] Waiting for a messages. To exit press CTRL+C")
	<-forever
}
