package logrus_kinesis

import (
	"errors"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

// StreamWriter is an implementation of `HookWriter` for sending logs to
// AWS Kinesis Streams
type StreamWriter struct {
	kinesis   *kinesis.Kinesis
	hook      *KinesisHook
	formatter logrus.Formatter
}

// NewStreamHook returns an instance of `StreamWriter` which satisfies `HookWriter`
// interface
func NewStreamHook(config *Config, awsSession *session.Session, kinesisHook *KinesisHook) (HookWriter, error) {

	if config == nil {

		return nil, errors.New("config parameter cannot be nil")
	}
	if awsSession == nil {

		return nil, errors.New("awsSession parameter cannot be nil")
	}
	if kinesisHook == nil {

		return nil, errors.New("kinesisHook parameter cannot be nil")
	}

	_kinesis := kinesis.New(awsSession)

	result := &StreamWriter{
		kinesis: _kinesis,
		hook:    kinesisHook,
	}

	return result, nil
}

func (sw *StreamWriter) Write(entry *logrus.Entry) error {

	if entry == nil {

		return errors.New("entry parameter cannot be nil")
	}

	var data []byte
	// Hmm, I was unsure how to handle formatting as this library already seems to do that,
	// but it didn't suit my needs so I added the option to include a `logrus.Formatter`.
	// Check if that formatter has been set here.
	if sw.formatter != nil {

		_data, err := sw.formatter.Format(entry)
		data = _data

		if err != nil {

			return err
		}

	} else {

		data = sw.hook.getData(entry)
		// `evalphobia` github user didn't include a new line so I won't include,
		// in the interest of leaving the original format the same.
		// data = append(data, 0x0A) // Add a new line, very important
	}

	in := &kinesis.PutRecordInput{
		StreamName:   stringPtr(sw.hook.getStreamName(entry)),
		PartitionKey: stringPtr(sw.hook.getPartitionKey(entry)),
		Data:         data,
	}

	_, err := sw.kinesis.PutRecord(in)

	return err
}
