package main

import (
	"bytes"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// нужно имитировать секунду работы для каждой точки в теле сообщения

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
		"task_queue", // name
		true,         // durable, true - queue will survive a RabbitMQ node restart
		false,        // delete when unusued
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // gloabal
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // name
		"",     // consumer
		false,  // auto-ack
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
			dotCount := bytes.Count(d.Body, []byte("."))
			t := time.Duration(dotCount)
			time.Sleep(t * time.Second)
			log.Printf("Done")

			// Используя этот код, вы можете гарантировать, что даже если вы
			// завершите рабочий процесс с помощью CTRL+C
			// во время обработки сообщения, ничего не будет потеряно.
			// Вскоре после завершения работы работника
			// все неподтвержденные сообщения доставляются повторно.
			d.Ack(false)
		}
	}()
	log.Printf(" [*] Waiting for a messages. To exit press CTRL+C")
	<-forever
}
