package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/exchangedataset/streamcommons/formatter"
)

// FilterParameter is the parameter needed to filter lines
type FilterParameter struct {
	exchange string
	// This is used for formatting target
	channels  []string
	minute    int64
	start     int64
	end       int64
	format    string
	preFilter map[string]bool
	buffer    *bytes.Buffer
	writer    *bufio.Writer
	form      formatter.Formatter
}

func (c *FilterParameter) finishResultBuffer() ([]byte, error) {
	serr := c.writer.Flush()
	if serr != nil {
		return nil, serr
	}
	return c.buffer.Bytes(), nil
}

func (c *FilterParameter) initResultBuffer() {
	// Buffer to store final result
	buf := make([]byte, 0, 50*1024*1024)
	c.buffer = bytes.NewBuffer(buf)
	c.writer = bufio.NewWriter(c.buffer)
	return
}

func (c *FilterParameter) initFormatter() (err error) {
	if c.format != "raw" {
		c.form, err = formatter.GetFormatter(c.exchange, c.channels, c.format)
		if err != nil {
			return
		}
	}
	return
}

func (c *FilterParameter) writeTabSeparated(bytes ...[]byte) error {
	for i, b := range bytes {
		if _, serr := c.writer.Write(b); serr != nil {
			return fmt.Errorf("%d: %v", i, serr)
		}
		if i != len(bytes)-1 {
			if _, serr := c.writer.WriteRune('\t'); serr != nil {
				return fmt.Errorf("tab: %v", serr)
			}
		}
	}
	if _, serr := c.writer.WriteRune('\n'); serr != nil {
		return fmt.Errorf("line terminator: %v", serr)
	}
	return nil
}

func toPreFilter(exchange string, channels []string) map[string]bool {
	preFilter := make(map[string]bool)
	switch exchange {
	case "bitmex":
		// Construct unique list of raw channels
		for _, ch := range channels {
			// Take prefix from full channel name
			// Eg. orderBookL2_XBTUSD -> orderBookL2
			ri := strings.IndexRune(ch, '_')
			if ri == -1 {
				ri = len(ch)
			}
			prefix := ch[:ri]
			preFilter[prefix] = true
		}
	default:
		for _, ch := range channels {
			preFilter[ch] = true
		}
	}
	return preFilter
}

func makeParameter(event *events.APIGatewayProxyRequest) (param *FilterParameter, err error) {
	param = new(FilterParameter)

	var ok bool
	param.exchange, ok = event.PathParameters["exchange"]
	if !ok {
		err = errors.New("path parameter 'exchange' must be specified")
		return
	}
	param.channels, ok = event.MultiValueQueryStringParameters["channels"]
	if !ok {
		err = errors.New("query parameter 'channels' must be specified")
		return
	}
	param.preFilter = toPreFilter(param.exchange, param.channels)

	minuteStr, ok := event.PathParameters["minute"]
	if !ok {
		err = errors.New("path parameter 'minute' must be specified")
		return
	}
	param.minute, err = strconv.ParseInt(minuteStr, 10, 64)
	if err != nil {
		err = errors.New("path parameter 'minute' must be of integer")
		return
	}
	startStr, ok := event.QueryStringParameters["start"]
	if ok {
		param.start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			err = errors.New("query parameter 'start' must be integer")
			return
		}
	} else {
		param.start = 0
	}
	endStr, ok := event.QueryStringParameters["end"]
	if ok {
		param.end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			err = errors.New("query parameter 'end' must be integer")
			return
		}
	} else {
		param.end = 9223372036854775807
	}
	format, ok := event.QueryStringParameters["format"]
	if ok {
		param.format = format
	} else {
		param.format = "raw"
	}
	err = param.initFormatter()
	return
}
