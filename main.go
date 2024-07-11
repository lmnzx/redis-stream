package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	chunkSize     = 1024 * 1024 // 1MB chunks
	fileListKey   = "largeFile"
	metadataKey   = "fileMetadata"
	fileSize      = 1024 * 1024 * 1024 * 2 // 2GB file
	numGoroutines = 6
)

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	fileName := "large_file.bin"
	err := createLargeFile(fileName, fileSize)
	if err != nil {
		panic(err)
	}
	defer os.Remove(fileName)

	file, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	err = streamFileToRedis(client, file)
	if err != nil {
		panic(err)
	}

	err = verifyAssembledFileInRedis(client)
	if err != nil {
		panic(err)
	}

	fmt.Println("File successfully created, streamed, and assembled in Redis")

	outputFileName := "retrieved_large_file.bin"
	err = retrieveFileFromRedis(client, outputFileName)
	if err != nil {
		panic(err)
	}

	fmt.Printf("File successfully retrieved from Redis and saved as '%s'\n", outputFileName)
}

func createLargeFile(fileName string, size int64) error {
	file, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	buffer := make([]byte, chunkSize)
	bytesWritten := int64(0)

	for bytesWritten < size {
		_, err := rand.Read(buffer)
		if err != nil {
			return err
		}

		n, err := file.Write(buffer)
		if err != nil {
			return err
		}
		bytesWritten += int64(n)

		if bytesWritten%int64(100*chunkSize) == 0 {
			fmt.Printf("Created %d MB...\n", bytesWritten/(1024*1024))
		}
	}

	return nil
}

func streamFileToRedis(client *redis.Client, file *os.File) error {
	ctx := client.Context()
	reader := bufio.NewReader(file)
	chunkNum := 0
	totalSize := int64(0)

	client.Del(ctx, fileListKey)

	dataCh := make(chan []byte)
	doneCh := make(chan struct{})
	wg := sync.WaitGroup{}

	availableWorkers := make(chan int, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		availableWorkers <- i
		go worker(client, dataCh, availableWorkers, &wg, i)
	}

	for {
		chunk := make([]byte, chunkSize)
		n, err := reader.Read(chunk)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if n < chunkSize {
			chunk = chunk[:n]
		}

		dataCh <- chunk
		totalSize += int64(n)
		chunkNum++

		if chunkNum%100 == 0 {
			fmt.Printf("Streamed %d MB to Redis...\n", totalSize/(1024*1024))
		}
	}

	close(dataCh)
	wg.Wait()
	close(doneCh)

	err := client.HSet(ctx, metadataKey, map[string]interface{}{
		"totalChunks": chunkNum,
		"totalSize":   totalSize,
	}).Err()
	if err != nil {
		return err
	}

	return nil
}

func worker(client *redis.Client, dataCh <-chan []byte, availableWorkers chan int, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	ctx := context.Background()

	for chunk := range dataCh {
		select {
		case <-availableWorkers:
			fmt.Printf("Goroutine %d started processing a chunk\n", id)

			if mathrand.Float32() < 0.5 {
				busyDuration := time.Duration(mathrand.Intn(2)) * time.Second
				fmt.Printf("Goroutine %d is busy for %v\n", id, busyDuration)
				time.Sleep(busyDuration)
			}

			err := client.RPush(ctx, fileListKey, chunk).Err()
			if err != nil {
				fmt.Printf("Goroutine %d failed to push chunk to Redis: %v\n", id, err)
			} else {
				fmt.Printf("Goroutine %d pushed chunk to Redis\n", id)
			}

			fmt.Printf("Goroutine %d finished processing a chunk\n", id)
			availableWorkers <- id
		}
	}
}

func verifyAssembledFileInRedis(client *redis.Client) error {
	ctx := client.Context()

	metadata, err := client.HGetAll(ctx, metadataKey).Result()
	if err != nil {
		return err
	}

	totalChunks, _ := strconv.Atoi(metadata["totalChunks"])
	totalSize, _ := strconv.ParseInt(metadata["totalSize"], 10, 64)

	listLen, err := client.LLen(ctx, fileListKey).Result()
	if err != nil {
		return err
	}

	if int(listLen) != totalChunks {
		return fmt.Errorf("chunk count mismatch: expected %d, got %d", totalChunks, listLen)
	}

	verifiedSize := int64(0)
	batchSize := 100

	for i := int64(0); i < listLen; i += int64(batchSize) {
		end := i + int64(batchSize) - 1
		if end >= listLen {
			end = listLen - 1
		}

		chunks, err := client.LRange(ctx, fileListKey, i, end).Result()
		if err != nil {
			return fmt.Errorf("failed to read chunks %d to %d: %w", i, end, err)
		}

		for _, chunk := range chunks {
			verifiedSize += int64(len(chunk))
		}

		fmt.Printf("Verified %d MB in Redis...\n", verifiedSize/(1024*1024))
	}

	if verifiedSize != totalSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", totalSize, verifiedSize)
	}

	fmt.Printf("Verification complete. Total size: %d bytes (%d MB)\n", totalSize, totalSize/(1024*1024))
	return nil
}

func retrieveFileFromRedis(client *redis.Client, outputFileName string) error {
	ctx := client.Context()

	metadata, err := client.HGetAll(ctx, metadataKey).Result()
	if err != nil {
		return fmt.Errorf("failed to retrieve metadata: %w", err)
	}

	totalSize, _ := strconv.ParseInt(metadata["totalSize"], 10, 64)

	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	listLen, err := client.LLen(ctx, fileListKey).Result()
	if err != nil {
		return fmt.Errorf("failed to get list length: %w", err)
	}

	retrievedSize := int64(0)
	batchSize := 100

	for i := int64(0); i < listLen; i += int64(batchSize) {
		end := i + int64(batchSize) - 1
		if end >= listLen {
			end = listLen - 1
		}

		chunks, err := client.LRange(ctx, fileListKey, i, end).Result()
		if err != nil {
			return fmt.Errorf("failed to read chunks %d to %d: %w", i, end, err)
		}

		for _, chunk := range chunks {
			n, err := outputFile.Write([]byte(chunk))
			if err != nil {
				return fmt.Errorf("failed to write chunk to file: %w", err)
			}
			retrievedSize += int64(n)
		}

		fmt.Printf("Retrieved %d MB from Redis...\n", retrievedSize/(1024*1024))
	}

	if retrievedSize != totalSize {
		return fmt.Errorf("size mismatch: expected %d bytes, got %d bytes", totalSize, retrievedSize)
	}

	fmt.Printf("File retrieval complete. Total size: %d bytes (%d MB)\n", totalSize, totalSize/(1024*1024))
	return nil
}
