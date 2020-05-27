package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
)

// handleRequest handles request from amazon api
func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	db, cerr := sc.ConnectDatabase()
	if cerr != nil {
		err = fmt.Errorf("failed to connect to database: %s", cerr.Error())
		return
	}
	defer func() {
		cerr := db.Close()
		if cerr != nil {
			if err != nil {
				err = fmt.Errorf("failed to close database connection: %s, original error was: %s", cerr.Error(), err.Error())
			} else {
				err = fmt.Errorf("failed to close database connection: %s", cerr.Error())
			}
		}
	}()

	// initialize apikey
	apikey, cerr := sc.NewAPIKey(event)
	if cerr != nil {
		return sc.MakeResponse(401, fmt.Sprintf("API-key authorization failed: %s", cerr.Error())), nil
	}
	fmt.Println("NewAPIKey end")
	if !apikey.Demo {
		// if this is not a demo apikey, then check for availability
		cerr = apikey.CheckAvalability(db)
		if cerr != nil {
			return sc.MakeResponse(401, fmt.Sprintf("API key is invalid: %s", cerr.Error())), nil
		}
		fmt.Println("API Checked")
	}

	// get paramaters
	param, cerr := makeParameter(&event)
	if cerr != nil {
		return sc.MakeResponse(400, cerr.Error()), nil
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
		form, cerr = formatter.GetFormatter(param.exchange, event.MultiValueQueryStringParameters["channels"], param.format)
		if cerr != nil {
			return sc.MakeResponse(400, cerr.Error()), nil
		}
	}
	fmt.Println("Formatter prepared")

	// generate result string for specified paramters
	buf := make([]byte, 0, 50*1024*1024)
	buffer := bytes.NewBuffer(buf)
	// this does not need to be closed
	writer := bufio.NewWriter(buffer)

	cerr = filter(db, param, form, writer)
	if cerr != nil {
		err = fmt.Errorf("Getting result failed: %s", cerr.Error())
		return
	}
	fmt.Println("Filtered")

	ferr := writer.Flush()
	if ferr != nil {
		if err != nil {
			err = fmt.Errorf("failed to flush writer, original error was: %s", err.Error())
		} else {
			err = errors.New("failed to flush writer")
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
		incremented, cerr = apikey.IncrementUsed(db, writtenSize)
		if cerr != nil {
			err = fmt.Errorf("Transfer update failed: %s", cerr.Error())
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
