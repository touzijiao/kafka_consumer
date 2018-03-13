package main

import (
	"fmt"
	"strings"
	//"sync"
	"time"

	"github.com/Shopify/sarama"
)

/*var (
	wg sync.WaitGroup
)*/

func main() {
	consumer, err := sarama.NewConsumer(strings.Split("192.168.1.166:9092", ","), nil) //创建一个消费者（本机kafka的端口）
	if err != nil {
		fmt.Println("Failed to start consumer: %s", err)
		return
	}
	partitionList, err := consumer.Partitions("10_xm_logAgent") //获取QQ_log在kafka内被到多少分区内（kafka配置文件分配了多少个分区就按多少分区消费）
	if err != nil {
		fmt.Println("Failed to get the list of partitions:", err)
		return
	}
	fmt.Println(partitionList)

	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("10_xm_logAgent", int32(partition), sarama.OffsetNewest) //消费分区（，分区ID, 从最新的位置读取数据）
		if err != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
			return

		}
		defer pc.AsyncClose() //关闭

		go func(sarama.PartitionConsumer) { //每一个分区起一个协程消费

			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				fmt.Println()
			}

		}(pc)
	}
	time.Sleep(time.Hour)

	consumer.Close()
}
