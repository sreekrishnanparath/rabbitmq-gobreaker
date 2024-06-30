package main

import (
	"fmt"
	//"bytes"
	"encoding/json"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sony/gobreaker"
)

// Custom error type to handle HTTP status codes
type HttpStatusCodeError struct {
	StatusCode int
	Message    string
}

func (e *HttpStatusCodeError) Error() string {
	return e.Message
}

var cb *gobreaker.CircuitBreaker

func init() {
	settings := gobreaker.Settings{
		Name:        "HTTP GET",
		Timeout:     5 * time.Second,
		MaxRequests: 1,
		Interval:    0,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip the circuit breaker only if there are consecutive 429 errors
			return counts.ConsecutiveFailures > 1
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			log.Printf("Circuit breaker state changed from %s to %s\n", from.String(), to.String())
		},
	}

	cb = gobreaker.NewCircuitBreaker(settings)
}

func makeRequestToThirdPartyService(request []byte) (interface{}, error) {
	//body := bytes.NewBuffer(request)
	resp, err := http.Get("https://dummy.restapiexample.com/api/v1/employees")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	

	if resp.StatusCode == http.StatusTooManyRequests {
	    return nil, &HttpStatusCodeError{
	        StatusCode: http.StatusTooManyRequests,
	        Message:    "Received 429 Too Many Requests",
	    }
	}
	//fmt.Println("resp ", resp)
	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		fmt.Println("error:", err)
		return nil, err
	}

	return result, nil
}

func processMessage(msg amqp.Delivery) {
	var err error
	//var result interface{}

	retries := 3
	delay := time.Second * 1

	for i := 0; i < retries; i++ {
		_, err = cb.Execute(func() (interface{}, error) {
			res, err := makeRequestToThirdPartyService(msg.Body)
			return res, err
		})

		if err == nil {
			break
		}

		log.Printf("Attempt %d: Failed to process message: %s. Retrying...\n", i+1, err)
		time.Sleep(delay)
	}

	if err != nil {
		log.Printf("Failed to process message after %d attempts: %s\n", retries, err)
	} else {
		log.Printf("Successfully processed message")
		//msg.Ack(false)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatal(err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)	
			processMessage(d)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
