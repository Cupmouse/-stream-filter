package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"

	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
)

// FilterParameter is the parameter needed to filter lines
type FilterParameter struct {
	exchange      string
	channelFilter map[string]bool
	postFilter    map[string]bool
	minute        int64
	start         int64
	end           int64
	format        string
}

func filter(param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) (found bool, err error) {
	ctx := context.Background()
	// Fetch object
	body, serr := sc.GetS3Object(ctx, fmt.Sprintf("%s_%d.gz", param.exchange, param.minute))
	if serr != nil {
		return false, fmt.Errorf("filter: %v", serr)
	}
	if body == nil {
		// Object did not found
		return false, nil
	}
	found = true
	defer func() {
		serr := body.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("filter: body close: %v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	serr = filterReader(body, param, form, writer)
	if serr != nil {
		err = fmt.Errorf("filter: %v", serr)
		return
	}
	return
}

func filterReader(reader io.ReadCloser, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) (err error) {
	defer func() {
		// close reader
		serr := reader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("reader: %v, originally %v", serr, err)
			} else {
				err = fmt.Errorf("reader: %v", serr)
			}
		}
	}()
	// Fetched object is in Gzip format
	greader, serr := gzip.NewReader(reader)
	if serr != nil {
		err = fmt.Errorf("open gzip: %v", serr)
		return
	}
	// This ensures both reader will be closed
	defer func() {
		// Close gzip reader
		// This won't close the underlying reader
		serr := greader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("greader: %v, originally %v", serr, err)
				return
			}
			err = fmt.Errorf("greader: %v", serr)
			return
		}
	}()
	// wrap gzip stream into string reader, this does not need closing
	breader := bufio.NewReader(greader)

	return filterGZip(breader, param, form, writer)
}

func writeTabSeparated(writer *bufio.Writer, bytes [][]byte) error {
	for i, b := range bytes {
		if _, serr := writer.Write(b); serr != nil {
			return fmt.Errorf("%d: %v", i, serr)
		}
		if i != len(bytes) {
			if _, serr := writer.WriteRune('\t'); serr != nil {
				return fmt.Errorf("tab: %v", serr)
			}
		}
	}
	if _, serr := writer.WriteRune('\n'); serr != nil {
		return fmt.Errorf("line terminator: %v", serr)
	}
	return nil
}

// filterGZip reads gzip from s3 with key and filters out channels not in filterChannels
func filterGZip(reader *bufio.Reader, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) error {
	for {
		// Read all
		all, serr := reader.ReadBytes('\n')
		if serr != nil {
			if serr == io.EOF {
				return nil
			}
			return fmt.Errorf("read: %v", serr)
		}
		// Remove line terminator at the end
		line := all[:len(all)-1]
		splitted := bytes.SplitN(line, []byte{'\t'}, 4)
		typBytes := splitted[0]
		typ := string(typBytes)
		timestampBytes := splitted[1]
		timestamp, serr := strconv.ParseInt(string(timestampBytes), 10, 64)
		if serr != nil {
			return fmt.Errorf("timestamp: %v", serr)
		}
		if timestamp < param.start {
			// Not reaching the start yet, ignore this line
			continue
		} else if param.end <= timestamp {
			// If timestamp of this line is out of range specified in parameter then end
			return nil
		}
		switch typ {
		case "msg":
			channel := string(splitted[2])
			// Should this channel be filtered in?
			if _, ok := param.channelFilter[channel]; !ok {
				// Not in filter, ignore this line
				continue
			}
			msg := splitted[3]
			// Formatter is specified, apply it
			if form != nil {
				formatted, serr := form.FormatMessage(channel, msg)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					if param.postFilter != nil {
						// Apply post filter
						if _, ok := param.postFilter[f.Channel]; !ok {
							continue
						}
					}
					writeTabSeparated(writer, [][]byte{typBytes, timestampBytes, []byte(f.Channel), f.Message})
				}
			} else {
				if _, serr = writer.Write(all); serr != nil {
					return fmt.Errorf("line: %v", serr)
				}
			}
		case "send":
			channel := string(splitted[2])
			if _, ok := param.channelFilter[channel]; !ok {
				continue
			}
			if _, serr = writer.Write(all); serr != nil {
				return fmt.Errorf("line: %v", serr)
			}
		case "state":
			// Ignore state line
		case "start":
			url := string(splitted[2])
			// Formatter is specified, apply it
			if form != nil {
				formatted, serr := form.FormatStart(url)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					if _, ok := param.channelFilter[f.Channel]; !ok {
						continue
					}
					writeTabSeparated(writer, [][]byte{[]byte("msg"), timestampBytes, []byte(f.Channel), f.Message})
				}
			} else {
				if _, serr = writer.Write(all); serr != nil {
					return fmt.Errorf("line: %v", serr)
				}
			}
		case "end":
			continue
		case "err":
			// Filter has no effect and err
			if _, serr = writer.Write(all); serr != nil {
				return fmt.Errorf("line: %v", serr)
			}
		default:
			return fmt.Errorf("unsupported line type: %s", typ)
		}
	}
}
