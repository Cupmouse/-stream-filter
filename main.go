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

// Production is `true` if and only if this instance is running on the context of production environment.
var Production = os.Getenv("PRODUCTION") == "1"

// handleRequest handles request from amazon api
func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	if Production {
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
	// Initialize apikey
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
	// Make parameter struct from AWS Gateway Proxy Event
	param, serr := makeParameter(&event)
	if serr != nil {
		return sc.MakeResponse(400, serr.Error()), nil
	}
	if apikey.Demo && Production {
		// If the apikey is an demo key, then limit the start and end date
		if param.minute < int64(sc.DemoAPIKeyAllowedStart/time.Minute) ||
			param.minute >= int64(sc.DemoAPIKeyAllowedEnd/time.Minute) {
			// Out of range
			return sc.MakeResponse(400, "parameter minute out of range: Demo API-key can only request certain date"), nil
		}
	}
	fmt.Println("Parameter loaded")
	var form formatter.Formatter
	if param.format != "raw" {
		form, serr = formatter.GetFormatter(param.exchange, event.MultiValueQueryStringParameters["channels"], param.format)
		if serr != nil {
			return sc.MakeResponse(400, serr.Error()), nil
		}
		for channel := range param.channelFilter {
			if !form.IsSupported(channel) {
				return sc.MakeResponse(400, fmt.Sprintf("formatting for channel '%v' is not supported", channel)), nil
			}
		}
	}
	fmt.Println("Formatter prepared")
	// Buffer to store final result
	buf := make([]byte, 0, 50*1024*1024)
	buffer := bytes.NewBuffer(buf)
	writer := bufio.NewWriter(buffer)

	found, serr := filter(param, form, writer)
	if serr != nil {
		err = serr
		return
	}
	fmt.Println("Filtered")
	serr = writer.Flush()
	if serr != nil {
		if err != nil {
			err = fmt.Errorf("flush writer: %v, originally: %v", serr, err)
		} else {
			err = fmt.Errorf("flush writer: %v", serr)
		}
		return
	}
	written := buffer.Bytes()
	writtenSize := int64(len(written))
	var incremented int64
	if apikey.Demo {
		incremented = sc.CalcQuotaUsed(writtenSize)
	} else {
		// If apikey is not test key, update transfer amount
		incremented, serr = apikey.IncrementUsed(db, writtenSize)
		if serr != nil {
			err = fmt.Errorf("transfer update: %v", serr)
			return
		}
		fmt.Println("Increment done")
	}
	var statusCode int
	if found {
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
	param.channelFilter = make(map[string]bool)
	for _, channel := range channels {
		param.channelFilter[channel] = true
	}
	postFilter, ok := event.MultiValueQueryStringParameters["postFilter"]
	if ok {
		param.postFilter = make(map[string]bool)
		for _, channel := range postFilter {
			param.postFilter[channel] = true
		}
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
