package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

type empData struct {
	Name   string
	Age    uint8
	Salary uint32
}

func handleError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func connectToAzure(url string) *azblob.Client {

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	handleError(err)

	client, err := azblob.NewClient(url, credential, nil)
	handleError(err)

	return client
}

func createContainer(client *azblob.Client, ctx context.Context, containerName string) {

	fmt.Printf("Creating a container named %s\n", containerName)
	_, err := client.CreateContainer(ctx, containerName, nil)
	handleError(err)
}

func uploadData(client *azblob.Client, mgs []byte, ctx context.Context, containerName, blobName string, msg []byte) {

	// Upload to data to blob storage
	fmt.Printf("Uploading a blob named %s\n", blobName)
	_, err := client.UploadBuffer(ctx, containerName, blobName, msg, &azblob.UploadBufferOptions{})
	handleError(err)
}

func deleteContainer(client *azblob.Client, ctx context.Context, containerName, blobName string) {

	fmt.Printf("Press enter key to delete resources and exit the application.\n")
	bufio.NewReader(os.Stdin).ReadBytes('\n')
	fmt.Printf("Cleaning up.\n")

	// Delete the blob
	fmt.Printf("Deleting the blob " + blobName + "\n")

	_, err := client.DeleteBlob(ctx, containerName, blobName, nil)
	handleError(err)

	// Delete the container
	fmt.Printf("Deleting the container " + containerName + "\n")
	_, err = client.DeleteContainer(ctx, containerName, nil)
	handleError(err)
}

func main() {
	url := "https://sapk358730.blob.core.windows.net/"
	containerName := "employee-data"
	blobName := "employee-blob"

	fmt.Printf("Azure blob upload\n")

	client := connectToAzure(url)
	ctx := context.Background()

	// Create the container
	createContainer(client, ctx, containerName)

	emp := empData{Name: "Aries", Age: 45, Salary: 100000}
	msg, err := json.Marshal(&emp)

	if err != nil {
		log.Fatal("Json marshal error:", err)
		panic(err)
	}

	//Upload to Azure
	uploadData(client, msg, ctx, containerName, blobName, msg)

	//Delet container when job is done
	deleteContainer(client, ctx, containerName, blobName)
}
