package logrus_kinesis

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

// FirehoseWriter is an implementation of `HookWriter` for sending logs to
// AWS Kinesis Firehose
type FirehoseWriter struct {
	firehose *firehose.Firehose
	hook     *KinesisHook
}

// NewFirehoseHook returns an instance of `FirehoseHook`, which satisfies `HookWriter`
// interface
func NewFirehoseHook(config *Config, awsSession *session.Session, kinesisHook *KinesisHook) (HookWriter, error) {

	if config == nil {

		return nil, errors.New("config parameter cannot be nil")
	}
	if awsSession == nil {

		return nil, errors.New("awsSession parameter cannot be nil")
	}
	if kinesisHook == nil {

		return nil, errors.New("kinesisHook parameter cannot be nil")
	}

	_firehose := firehose.New(awsSession, config.AWSConfig())

	result := &FirehoseWriter{
		hook:     kinesisHook,
		firehose: _firehose,
	}

	return result, nil
}

func (fw *FirehoseWriter) Write(entry *logrus.Entry) error {

	if entry == nil {

		return errors.New("entry parameter cannot be nil")
	}

	data := fw.hook.getData(entry)
	data = append(data, 0x0A)

	pri := &firehose.PutRecordInput{}
	pri.SetDeliveryStreamName(fw.hook.getStreamName(entry))
	fhr := &firehose.Record{
		Data: data,
	}

	pri.SetRecord(fhr)

	_, err := fw.firehose.PutRecord(pri)

	return err
}
