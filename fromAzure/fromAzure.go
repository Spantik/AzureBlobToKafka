package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func conntoToAzureAndAuth(url string) *azblob.Client {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	return client
}

func downloadFromAzure(client *azblob.Client, containerName, blobName string) {
	ctx := context.Background()

	// List the blobs in the container
	fmt.Println("Listing the blobs in the container:")

	pager := client.NewListBlobsFlatPager(containerName, &azblob.ListBlobsFlatOptions{
		Include: azblob.ListBlobsInclude{Snapshots: true, Versions: true},
	})

	for pager.More() {
		resp, err := pager.NextPage(context.TODO())
		handleError(err)

		for _, blob := range resp.Segment.BlobItems {
			fmt.Println(*blob.Name)
		}
	}

	// Download the blob
	get, err := client.DownloadStream(ctx, containerName, blobName, nil)
	handleError(err)

	downloadedData := bytes.Buffer{}
	retryReader := get.NewRetryReader(ctx, &azblob.RetryReaderOptions{})
	_, err = downloadedData.ReadFrom(retryReader)
	handleError(err)

	err = retryReader.Close()
	handleError(err)

	// Print the content of the blob we created
	fmt.Println("Blob contents:")
	fmt.Println(downloadedData.String())

	//To Kafka
	producer := connectToKafka()
	sendMsgToKafka(producer, downloadedData.Bytes())

}

func connectToKafka() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		panic(err)
	}

	return p
}

func sendMsgToKafka(p *kafka.Producer, msg []byte) {
	topic := "AzureToMongo"
	fmt.Println("Sending message to Kafka")
	p.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Value: msg}, nil)
}

func main() {
	containerName := "employee-data"
	blobName := "employee-blob"
	url := "https://sapk358730.blob.core.windows.net/"

	client := conntoToAzureAndAuth(url)

	//Download from Azure and send to Kafka
	downloadFromAzure(client, containerName, blobName)

	fmt.Printf("Press enter key to exit the application.\n")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Printf("Cleaning up.\n")
}
