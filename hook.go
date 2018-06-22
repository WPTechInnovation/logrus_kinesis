package logrus_kinesis

import (
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/sirupsen/logrus"
)

var defaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

// HookWriter defines common functionality for writing data from hook to destination
type HookWriter interface {
	Write(entry *logrus.Entry) error
}

// KinesisHook is logrus hook for AWS kinesis.
type KinesisHook struct {
	client              HookWriter
	defaultStreamName   string
	defaultPartitionKey string
	async               bool
	levels              []logrus.Level
	ignoreFields        map[string]struct{}
	filters             map[string]func(interface{}) interface{}
}

// New returns initialized logrus hook for fluentd with persistent fluentd logger.
func New(name string, conf Config) (*KinesisHook, error) {

	return new(name, conf.AWSConfig(), &conf)
}

// NewWithAWSConfig returns initialized logrus hook for fluentd with persistent fluentd logger.
func NewWithAWSConfig(name string, awsConf *aws.Config) (*KinesisHook, error) {

	// StreamMode is default in order to maintain backward compatibility // TODO needs testing
	conf := &Config{}
	conf.KinesisMode = StreamMode

	return new(name, awsConf, conf)
}

// Generalised constructor - both the New and NewWithConfig call this - this function is
// code deduplication effort
func new(name string, awsConf *aws.Config, conf *Config) (*KinesisHook, error) {

	result := &KinesisHook{}

	awsSession, err := session.NewSession(awsConf)
	if err != nil {
		return nil, err
	}

	var hook HookWriter

	switch conf.KinesisMode {

	case FirehoseMode:
		hookFH, errFH := NewFirehoseHook(conf, awsSession, result)
		hook = hookFH
		err = errFH
	case StreamMode:
		hookSH, errSH := NewStreamHook(conf, awsSession, result)
		hook = hookSH
		err = errSH
	default:
		return nil, fmt.Errorf("Unsupported KinesisMode: %d", conf.KinesisMode)
	}

	if err != nil {

		return nil, err
	}

	result.client = hook
	result.defaultStreamName = name
	result.levels = defaultLevels
	result.ignoreFields = make(map[string]struct{})
	result.filters = make(map[string]func(interface{}) interface{})

	return result, nil
}

// Levels returns logging level to fire this hook.
func (h *KinesisHook) Levels() []logrus.Level {
	return h.levels
}

// SetLevels sets logging level to fire this hook.
func (h *KinesisHook) SetLevels(levels []logrus.Level) {
	h.levels = levels
}

// SetPartitionKey sets default partitionKey for kinesis.
func (h *KinesisHook) SetPartitionKey(key string) {
	h.defaultPartitionKey = key
}

// Async sets async flag and send log asynchroniously.
// If use this option, Fire() does not return error.
func (h *KinesisHook) Async() {
	h.async = true
}

// AddIgnore adds field name to ignore.
func (h *KinesisHook) AddIgnore(name string) {
	h.ignoreFields[name] = struct{}{}
}

// AddFilter adds a custom filter function.
func (h *KinesisHook) AddFilter(name string, fn func(interface{}) interface{}) {
	h.filters[name] = fn
}

// Fire is invoked by logrus and sends log to kinesis.
func (h *KinesisHook) Fire(entry *logrus.Entry) error {
	if !h.async {
		return h.fire(entry)
	}

	// send log asynchroniously and return no error.
	go h.fire(entry)
	return nil
}

// Fire is invoked by logrus and sends log to kinesis.
func (h *KinesisHook) fire(entry *logrus.Entry) error {
	return h.client.Write(entry)
}

func (h *KinesisHook) getStreamName(entry *logrus.Entry) string {
	if name, ok := entry.Data["stream_name"].(string); ok {
		return name
	}
	return h.defaultStreamName
}

func (h *KinesisHook) getPartitionKey(entry *logrus.Entry) string {
	if key, ok := entry.Data["partition_key"].(string); ok {
		return key
	}
	if h.defaultPartitionKey != "" {
		return h.defaultPartitionKey
	}
	return entry.Message
}

func (h *KinesisHook) getData(entry *logrus.Entry) []byte {
	if _, ok := entry.Data["message"]; !ok {
		entry.Data["message"] = entry.Message
	}

	data := make(logrus.Fields)
	for k, v := range entry.Data {
		if _, ok := h.ignoreFields[k]; ok {
			continue
		}
		if fn, ok := h.filters[k]; ok {
			v = fn(v) // apply custom filter
		} else {
			v = formatData(v) // use default formatter
		}
		data[k] = v
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	return bytes
}

// formatData returns value as a suitable format.
func formatData(value interface{}) (formatted interface{}) {
	switch value := value.(type) {
	case json.Marshaler:
		return value
	case error:
		return value.Error()
	case fmt.Stringer:
		return value.String()
	default:
		return value
	}
}

func stringPtr(str string) *string {
	return &str
}
