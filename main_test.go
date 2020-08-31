package main

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func makeLambdaEvent(exchange string, channels []string, minute string, format string) events.APIGatewayProxyRequest {
	return events.APIGatewayProxyRequest{
		PathParameters: map[string]string{
			"exchange": exchange,
			"minute":   minute,
		},
		QueryStringParameters: map[string]string{
			"format": format,
		},
		MultiValueQueryStringParameters: map[string][]string{
			"channels": channels,
		},
		Headers: map[string]string{"Authorization": "Bearer demo"},
	}
}

func TestBinanceTrade(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@trade"}, "26647344", "json"))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatal(res.Body)
	}
	if len(res.Body) == 0 {
		t.Fatal("empty body")
	}
}

func TestBinanceDepth(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@depth@100ms"}, "26647344", "json"))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatal(res.Body)
	}
	if len(res.Body) == 0 {
		t.Fatal("empty body")
	}
}

func TestBinanceTicker(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@ticker"}, "26647344", "json"))
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != 200 {
		t.Fatal(res.Body)
	}
	if len(res.Body) == 0 {
		t.Fatal("empty body")
	}
}
