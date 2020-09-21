package main

import (
	"fmt"
	"os"
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

func testCommon(t *testing.T, res *events.APIGatewayProxyResponse, err error) {
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Body) == 0 {
		t.Fatal("empty body")
	}
	if res.StatusCode != 200 {
		t.Fatal(res.Body)
	}
	fmt.Print(res.Body)
}

func TestBitmexTrade(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"trade_XBTUSD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitmexOrderBookL2(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"orderBookL2_XBTUSD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitmexInstrument(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"instrument_XBTUSD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitmexFunding(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"funding_USD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitmexSettlement(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"settlement_XBTUSD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitmexLiquidation(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"liquidation_XBTUSD"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitflyerBoard(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitflyer", []string{"lightning_board_BTC_JPY"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitflyerExecutions(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitflyer", []string{"lightning_executions_BTC_JPY"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestBitflyerTicker(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitflyer", []string{"lightning_ticker_BTC_JPY"}, "26647380", "json"))
	testCommon(t, res, err)
}
func TestBitfinexBook(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitfinex", []string{"book_tBTCUSD"}, "26647381", "json"))
	testCommon(t, res, err)
}

func TestBitfinexTrade(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitfinex", []string{"trades_tBTCUSD"}, "26647381", "json"))
	testCommon(t, res, err)
}

func TestBinanceTrade(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@trade"}, "26647344", "json"))
	testCommon(t, res, err)
}

func TestBinanceDepth(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@depth@100ms"}, "26647344", "json"))
	testCommon(t, res, err)
}

func TestBinanceTicker(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@ticker"}, "26647344", "json"))
	testCommon(t, res, err)
}

func TestLiquidPriceLaddersBuy(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("liquid", []string{"price_ladders_cash_btcjpy_buy"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestLiquidPriceLaddersSell(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("liquid", []string{"price_ladders_cash_btcjpy_sell"}, "26647380", "json"))
	testCommon(t, res, err)
}

func TestLiquidExecution(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("liquid", []string{"executions_cash_btcjpy"}, "26647380", "json"))
	testCommon(t, res, err)
}

func init() {
	os.Setenv("DATABASE_URL", "localhost")
	os.Setenv("DATABASE_USER", "root")
	os.Setenv("DATABASE_PORT", "3306")
	os.Setenv("DATABASE_PASSWORD", "")
	os.Setenv("DATABASE_DISABLE_CERT", "1")
}
