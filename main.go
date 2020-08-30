package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
)

// Test is `true` if and only if this instance is running on the context of test.
var Test = os.Getenv("TEST") == "1"

// handleRequest handles request from amazon api
func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	if !Test {
		sc.AWSEnableProduction()
	}

	db, serr := sc.ConnectDatabase()
	if serr != nil {
		err = serr
		return
	}
	defer func() {
		serr := db.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("database close: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("database close: %v", serr)
			}
		}
	}()

	// initialize apikey
	apikey, serr := sc.NewAPIKey(event)
	if serr != nil {
		return sc.MakeResponse(401, fmt.Sprintf("API-key authorization: %v", serr)), nil
	}
	fmt.Println("NewAPIKey end")
	if !apikey.Demo {
		// if this is not a demo apikey, then check for availability
		serr = apikey.CheckAvalability(db)
		if serr != nil {
			return sc.MakeResponse(401, fmt.Sprintf("API key is invalid: %v", serr)), nil
		}
		fmt.Println("API Checked")
	}

	// get paramaters
	param, serr := makeParameter(&event)
	if serr != nil {
		return sc.MakeResponse(400, serr.Error()), nil
	}
	if apikey.Demo {
		// if apikey is demo key, then limit the start and end date
		if param.minute < int64(sc.DemoAPIKeyAllowedStart/time.Minute) ||
			param.minute >= int64(sc.DemoAPIKeyAllowedEnd/time.Minute) {
			// out of range
			return sc.MakeResponse(400, "parameter minute out of range: Demo API-key can only request certain date"), nil
		}
	}

	fmt.Println("Parameter made")
	var form formatter.Formatter
	if param.format != "raw" {
		form, serr = formatter.GetFormatter(param.exchange, event.MultiValueQueryStringParameters["channels"], param.format)
		if serr != nil {
			return sc.MakeResponse(400, serr.Error()), nil
		}
	}
	fmt.Println("Formatter prepared")

	// generate result string for specified paramters
	buf := make([]byte, 0, 50*1024*1024)
	buffer := bytes.NewBuffer(buf)
	// this does not need to be closed
	writer := bufio.NewWriter(buffer)

	serr = filter(db, param, form, writer)
	if serr != nil {
		err = fmt.Errorf("Getting result failed: %v", serr)
		return
	}
	fmt.Println("Filtered")

	ferr := writer.Flush()
	if ferr != nil {
		if err != nil {
			err = fmt.Errorf("flush writer failed, originally: %v", err)
		} else {
			err = errors.New("flush writer failed")
		}
		return
	}

	written := buffer.Bytes()
	writtenSize := int64(len(written))

	var incremented int64
	if apikey.Demo {
		incremented = sc.CalcQuotaUsed(writtenSize)
	} else {
		// if apikey is not test key, update transfer amount
		incremented, serr = apikey.IncrementUsed(db, writtenSize)
		if serr != nil {
			err = fmt.Errorf("transfer update failed: %v", serr)
			return
		}
		fmt.Println("Increment done")
	}

	var statusCode int
	// return value
	if writtenSize != 0 {
		statusCode = 200
	} else {
		statusCode = 404
	}

	return sc.MakeLargeResponse(statusCode, written, incremented)
}

func makeParameter(event *events.APIGatewayProxyRequest) (*FilterParameter, error) {
	param := new(FilterParameter)

	var err error
	var ok bool
	param.exchange, ok = event.PathParameters["exchange"]
	if !ok {
		return param, errors.New("path parameter 'exchange' must be specified")
	}
	channels, ok := event.MultiValueQueryStringParameters["channels"]
	if !ok {
		return param, errors.New("query parameter 'channels' must be specified")
	}
	// make set of channels to be included for easy filtering with less computation
	param.filterChannels = make(map[string]bool)
	for _, channel := range channels {
		param.filterChannels[channel] = true
	}
	minuteStr, ok := event.PathParameters["minute"]
	if !ok {
		return param, errors.New("path parameter 'minute' must be specified")
	}
	param.minute, err = strconv.ParseInt(minuteStr, 10, 64)
	if err != nil {
		return param, errors.New("path parameter 'minute' must be of integer")
	}
	startStr, ok := event.QueryStringParameters["start"]
	if ok {
		param.start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			return param, errors.New("query parameter 'start' must be integer")
		}
	} else {
		param.start = 0
	}
	endStr, ok := event.QueryStringParameters["end"]
	if ok {
		param.end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			return param, errors.New("query parameter 'end' must be integer")
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
	return param, nil
}

func main() {
	lambda.Start(handleRequest)
}
