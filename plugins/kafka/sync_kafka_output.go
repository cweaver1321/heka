/***** BEGIN LICENSE BLOCK *****
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2014-2015
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   Mike Trinkala (trink@mozilla.com)
#   Rob Miller (rmiller@mozilla.com)
#
# ***** END LICENSE BLOCK *****/

/*
 For those who want better control of delivery failures with a 10X performance
 degradation.
 https://github.com/mozilla-services/heka/pull/1777
*/
package kafka

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
)

type SyncKafkaOutputConfig struct {
	// Client Config
	Id                         string
	Addrs                      []string
	MetadataRetries            int    `toml:"metadata_retries"`
	WaitForElection            uint32 `toml:"wait_for_election"`
	BackgroundRefreshFrequency uint32 `toml:"background_refresh_frequency"`

	// Broker Config
	MaxOpenRequests int    `toml:"max_open_reqests"`
	DialTimeout     uint32 `toml:"dial_timeout"`
	ReadTimeout     uint32 `toml:"read_timeout"`
	WriteTimeout    uint32 `toml:"write_timeout"`

	// Producer Config
	Partitioner string // Random, RoundRobin, Hash
	// Hash/Topic variables are restricted to the Type", "Logger", "Hostname" and "Payload" headers.
	// Field variables are unrestricted.
	HashVariable  string `toml:"hash_variable"`  // HashPartitioner key is extracted from a message variable
	TopicVariable string `toml:"topic_variable"` // Topic extracted from a message variable
	Topic         string // Static topic

	RequiredAcks               string `toml:"required_acks"` // NoResponse, WaitForLocal, WaitForAll
	Timeout                    uint32
	CompressionCodec           string `toml:"compression_codec"` // None, GZIP, Snappy
	MaxBufferTime              uint32 `toml:"max_buffer_time"`
	MaxBufferedBytes           uint32 `toml:"max_buffered_bytes"`
	BackPressureThresholdBytes uint32 `toml:"back_pressure_threshold_bytes"`
	MaxMessageBytes            uint32 `toml:"max_message_bytes"`
}

type SyncKafkaOutput struct {
	processMessageCount    int64
	processMessageFailures int64
	processMessageDiscards int64
	kafkaDroppedMessages   int64
	kafkaEncodingErrors    int64

	hashVariable   *messageVariable
	topicVariable  *messageVariable
	config         *SyncKafkaOutputConfig
	saramaConfig   *sarama.Config
	client         sarama.Client
	producer       sarama.SyncProducer
	pipelineConfig *pipeline.PipelineConfig
}

func (k *SyncKafkaOutput) ConfigStruct() interface{} {
	hn := k.pipelineConfig.Hostname()
	return &SyncKafkaOutputConfig{
		Id:                         hn,
		MetadataRetries:            3,
		WaitForElection:            250,
		BackgroundRefreshFrequency: 10 * 60 * 1000,
		MaxOpenRequests:            4,
		Timeout:                    1000,
		DialTimeout:                60 * 1000,
		ReadTimeout:                60 * 1000,
		WriteTimeout:               60 * 1000,
		Partitioner:                "Random",
		RequiredAcks:               "WaitForLocal",
		CompressionCodec:           "None",
		MaxBufferTime:              1,
		MaxBufferedBytes:           1,
	}
}

func (k *SyncKafkaOutput) SetPipelineConfig(pConfig *pipeline.PipelineConfig) {
	k.pipelineConfig = pConfig
}

func (k *SyncKafkaOutput) Init(config interface{}) (err error) {
	k.config = config.(*SyncKafkaOutputConfig)
	if len(k.config.Addrs) == 0 {
		return errors.New("addrs must have at least one entry")
	}

	if k.config.MaxMessageBytes == 0 {
		k.config.MaxMessageBytes = message.MAX_RECORD_SIZE
	}

	k.saramaConfig = sarama.NewConfig()
	k.saramaConfig.ClientID = k.config.Id
	k.saramaConfig.Metadata.Retry.Max = k.config.MetadataRetries
	k.saramaConfig.Metadata.Retry.Backoff = time.Duration(k.config.WaitForElection) * time.Millisecond
	k.saramaConfig.Metadata.RefreshFrequency = time.Duration(k.config.BackgroundRefreshFrequency) * time.Millisecond

	k.saramaConfig.Net.MaxOpenRequests = k.config.MaxOpenRequests
	k.saramaConfig.Net.DialTimeout = time.Duration(k.config.DialTimeout) * time.Millisecond
	k.saramaConfig.Net.ReadTimeout = time.Duration(k.config.ReadTimeout) * time.Millisecond
	k.saramaConfig.Net.WriteTimeout = time.Duration(k.config.WriteTimeout) * time.Millisecond

	k.saramaConfig.Producer.MaxMessageBytes = int(k.config.MaxMessageBytes)
	switch k.config.Partitioner {
	case "Random":
		k.saramaConfig.Producer.Partitioner = sarama.NewRandomPartitioner
		if len(k.config.HashVariable) > 0 {
			return fmt.Errorf("hash_variable should not be set for the %s partitioner", k.config.Partitioner)
		}
	case "RoundRobin":
		k.saramaConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		if len(k.config.HashVariable) > 0 {
			return fmt.Errorf("hash_variable should not be set for the %s partitioner", k.config.Partitioner)
		}
	case "Hash":
		k.saramaConfig.Producer.Partitioner = sarama.NewHashPartitioner
		if k.hashVariable = verifyMessageVariable(k.config.HashVariable); k.hashVariable == nil {
			return fmt.Errorf("invalid hash_variable: %s", k.config.HashVariable)
		}
	default:
		return fmt.Errorf("invalid partitioner: %s", k.config.Partitioner)
	}

	if len(k.config.Topic) == 0 {
		if k.topicVariable = verifyMessageVariable(k.config.TopicVariable); k.topicVariable == nil {
			return fmt.Errorf("invalid topic_variable: %s", k.config.TopicVariable)
		}
	} else if len(k.config.TopicVariable) > 0 {
		return errors.New("topic and topic_variable cannot both be set")
	}

	switch k.config.RequiredAcks {
	case "NoResponse":
		k.saramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "WaitForLocal":
		k.saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	case "WaitForAll":
		k.saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	default:
		return fmt.Errorf("invalid required_acks: %s", k.config.RequiredAcks)
	}

	k.saramaConfig.Producer.Timeout = time.Duration(k.config.Timeout) * time.Millisecond

	switch k.config.CompressionCodec {
	case "None":
		k.saramaConfig.Producer.Compression = sarama.CompressionNone
	case "GZIP":
		k.saramaConfig.Producer.Compression = sarama.CompressionGZIP
	case "Snappy":
		k.saramaConfig.Producer.Compression = sarama.CompressionSnappy
	default:
		return fmt.Errorf("invalid compression_codec: %s", k.config.CompressionCodec)
	}

	k.saramaConfig.Producer.Flush.Bytes = int(k.config.MaxBufferedBytes)
	k.saramaConfig.Producer.Flush.Frequency = time.Duration(k.config.MaxBufferTime) * time.Millisecond

	k.client, err = sarama.NewClient(k.config.Addrs, k.saramaConfig)
	if err != nil {
		return err
	}
	k.producer, err = sarama.NewSyncProducer(k.config.Addrs, k.saramaConfig)
	return err
}

func (k *SyncKafkaOutput) Run(or pipeline.OutputRunner, h pipeline.PluginHelper) (err error) {
	defer func() {
		k.producer.Close()
		k.client.Close()
	}()

	if or.Encoder() == nil {
		return errors.New("Encoder required.")
	}

	inChan := or.InChan()

	var (
		pack  *pipeline.PipelinePack
		topic = k.config.Topic
		key   sarama.Encoder
	)

	for pack = range inChan {
		atomic.AddInt64(&k.processMessageCount, 1)

		if k.topicVariable != nil {
			topic = getMessageVariable(pack.Message, k.topicVariable)
		}
		if k.hashVariable != nil {
			key = sarama.StringEncoder(getMessageVariable(pack.Message, k.hashVariable))
		}

		msgBytes, err := or.Encode(pack)
		if err != nil {
			atomic.AddInt64(&k.processMessageFailures, 1)
			or.LogError(err)
			// Don't retry encoding errors.
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
			continue
		}
		if msgBytes == nil {
			atomic.AddInt64(&k.processMessageDiscards, 1)
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
			continue
		}
		pMessage := &sarama.ProducerMessage{
			Topic: topic,
			Key:   key,
			Value: sarama.ByteEncoder(msgBytes),
		}

		partition, offset, err := k.producer.SendMessage(pMessage)
		_ = partition
		_ = offset

		if err != nil {
			switch err.(type) {
			case sarama.PacketEncodingError:
				atomic.AddInt64(&k.kafkaEncodingErrors, 1)
				or.LogError(fmt.Errorf("kafka encoding error: %s", err.Error()))
			default:
				e := pipeline.NewRetryMessageError(err.Error())
				pack.Recycle(e)
				atomic.AddInt64(&k.kafkaDroppedMessages, 1)
				or.LogError(fmt.Errorf("kafka raised error: %s while sending: %s", err.Error(), pMessage.Value))
			}
		} else {
			or.UpdateCursor(pack.QueueCursor)
			pack.Recycle(nil)
		}
	}

	return
}

func (k *SyncKafkaOutput) ReportMsg(msg *message.Message) error {
	message.NewInt64Field(msg, "ProcessMessageCount",
		atomic.LoadInt64(&k.processMessageCount), "count")
	message.NewInt64Field(msg, "ProcessMessageFailures",
		atomic.LoadInt64(&k.processMessageFailures), "count")
	message.NewInt64Field(msg, "ProcessMessageDiscards",
		atomic.LoadInt64(&k.processMessageDiscards), "count")
	message.NewInt64Field(msg, "KafkaDroppedMessages",
		atomic.LoadInt64(&k.kafkaDroppedMessages), "count")
	message.NewInt64Field(msg, "KafkaEncodingErrors",
		atomic.LoadInt64(&k.kafkaEncodingErrors), "count")
	return nil
}

func (k *SyncKafkaOutput) CleanupForRestart() {
	return
}

func init() {
	pipeline.RegisterPlugin("SyncKafkaOutput", func() interface{} {
		return new(SyncKafkaOutput)
	})
}
