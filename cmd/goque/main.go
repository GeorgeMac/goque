package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/GeorgeMac/goque"
	"golang.org/x/net/context"
	"gopkg.in/redis.v3"
)

func main() {
	rcli := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	client := goque.NewClient(rcli)
	service, err := goque.New(client, 1)
	if err != nil {
		log.Fatal(err)
	}

	service.Register("transcode_get_media_info", goque.WorkerFunc(func(queue string, args []interface{}, ctxt context.Context) error {
		time.Sleep(10 * time.Second)
		log.Println(queue, args, ctxt)
		return nil
	}))

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	stop := service.Start(context.Background())

	<-c

	stop()
}
