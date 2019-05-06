package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/BurntSushi/toml"
	cluster "github.com/bsm/sarama-cluster"
)

type Config struct {
	Title       string
	Topic       topicinfo
	Broker      brokerinfo
	ConsumerGrp consmrGrpinfo
}

type topicinfo struct {
	EthAddrCmd string
}

type brokerinfo struct {
	Host string
}

type consmrGrpinfo struct {
	GrpName string
}

func main() {
	//read config file for topics
	var conf Config
	if _, err := toml.DecodeFile("conf/conf.toml", &conf); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(conf.Title)
	// init (custom) config, set mode to ConsumerModePartitions
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions

	// init consumer
	brokers := []string{conf.Broker.Host}
	topics := []string{conf.Topic.EthAddrCmd}
	consumer, err := cluster.NewConsumer(brokers, conf.ConsumerGrp.GrpName, topics, config)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume partitions
	for {
		select {
		case part, ok := <-consumer.Partitions():
			if !ok {
				return
			}

			// start a separate goroutine to consume messages
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Key, msg.Value)
					consumer.MarkOffset(msg, "") // mark message as processed
				}
			}(part)
		case <-signals:
			return
		}
	}
}
