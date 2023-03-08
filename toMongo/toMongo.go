package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type empData struct {
	Name   string
	Age    uint8
	Salary uint32
}

func connToMongo() *mongo.Client {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	return client
}

func consumerConnect() *kafka.Consumer {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "group-id-1",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	return consumer
}

func readFromKafka(client *mongo.Client, consumer *kafka.Consumer) {

	consumer.SubscribeTopics([]string{"AzureToMongo"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			fmt.Println("Received what", msg.Value)
			fmt.Printf("Received from Kafka %s: %s\n", msg.TopicPartition, string(msg.Value))
			pushToMongo(client, msg.Value)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	consumer.Close()
}

func pushToMongo(client *mongo.Client, data []byte) {
	usersCollection := client.Database("db4").Collection("jobs")
	var emp empData

	err := json.Unmarshal(data, &emp)
	if err != nil {
		fmt.Println("Can;t unmarshal the byte array")
		return
	}

	fmt.Printf("Received data from Kafka Name: %s age: %d Salary: %d Writing to Mongo", emp.Name, emp.Age, emp.Salary)

	user := bson.D{{"Name", emp.Name}, {"Age", emp.Age}, {"Salary", emp.Salary}}
	result, err := usersCollection.InsertOne(context.TODO(), user)
	// check for errors in the insertion
	if err != nil {
		panic(err)
	}
	// display the id of the newly inserted object
	fmt.Println(result.InsertedID)

}

func main() {
	client := connToMongo()
	consumer := consumerConnect()
	fmt.Println("Consumer connect successful")

	//Read from Kafka and push to MongoDB
	readFromKafka(client, consumer)
}
