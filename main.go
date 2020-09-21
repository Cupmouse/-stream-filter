package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	sc "github.com/exchangedataset/streamcommons"
)

// Production is `true` if and only if this instance is running on the context of production environment.
var Production = os.Getenv("PRODUCTION") == "1"

// handleRequest handles request from amazon api
func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	if Production {
		sc.AWSEnableProduction()
	}

	// Make parameter struct from AWS Gateway Proxy Event
	param, serr := makeParameter(&event)
	if serr != nil {
		return sc.MakeResponse(400, serr.Error()), nil
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
	if apikey.Demo && Production {
		// If the apikey is an demo key, then limit the start and end date
		if param.minute < int64(sc.DemoAPIKeyAllowedStart/time.Minute) ||
			param.minute >= int64(sc.DemoAPIKeyAllowedEnd/time.Minute) {
			// Out of range
			return sc.MakeResponse(400, "parameter minute out of range: Demo API-key can only request certain date"), nil
		}
	}
	fmt.Println("Parameter loaded")
	// Initialize result buffer
	param.initResultBuffer()
	found, serr := filter(param)
	if serr != nil {
		err = serr
		return
	}
	fmt.Println("Filtered")
	written, serr := param.finishResultBuffer()
	if serr != nil {
		err = serr
		return
	}
	if param.orderbookWritten+param.othersWritten != len(written) {
		// Counted wrongly
		return nil, errors.New("others + orderbook != written")
	}
	cost := sc.CalcCost(param.othersWritten, param.orderbookWritten)
	if !apikey.Demo {
		// If apikey is not test key, update transfer amount
		serr = apikey.IncrementUsed(db, cost)
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
	return sc.MakeLargeResponse(statusCode, written, cost)
}

func main() {
	lambda.Start(handleRequest)
}
