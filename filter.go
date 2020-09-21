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
)

func filter(param *FilterParameter) (found bool, err error) {
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
	serr = filterReader(body, param)
	if serr != nil {
		err = fmt.Errorf("filter: %v", serr)
		return
	}
	return
}

func filterReader(reader io.ReadCloser, param *FilterParameter) (err error) {
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

	return filterGZip(breader, param)
}

// filterGZip reads gzip from s3 with key and filters out channels not in filterChannels
func filterGZip(reader *bufio.Reader, param *FilterParameter) error {
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
		// This variable will be updated in below switch section
		cg := sc.ChannelGroupOthers
		written := 0
		switch typ {
		case "msg":
			channel := string(splitted[2])
			if _, ok := param.preFilter[channel]; !ok {
				// Filter out
				continue
			}
			msg := splitted[3]
			// Formatter is specified, apply it
			if param.form != nil {
				formatted, serr := param.form.FormatMessage(channel, msg)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					w, serr := param.writeTabSeparated(typBytes, timestampBytes, []byte(f.Channel), f.Message)
					if serr != nil {
						return fmt.Errorf("")
					}
					written += w
				}
			} else {
				if _, serr = param.writer.Write(all); serr != nil {
					return fmt.Errorf("line: %v", serr)
				}
				written += len(all)
			}
			if cg, serr = sc.GetChannelGroup(param.exchange, channel); serr != nil {
				return fmt.Errorf("msg: %v", serr)
			}
		case "send":
			channel := string(splitted[2])
			if _, ok := param.preFilter[channel]; !ok {
				// Filter out
				continue
			}
			if _, serr = param.writer.Write(all); serr != nil {
				return fmt.Errorf("line: %v", serr)
			}
			written += len(all)
			// Count differently by its channel group
			if cg, serr = sc.GetChannelGroup(param.exchange, channel); serr != nil {
				return fmt.Errorf("send: %v", serr)
			}
		case "state":
			// Ignore state line
		case "start":
			url := string(splitted[2])
			// Formatter is specified, apply it
			if param.form != nil {
				formatted, serr := param.form.FormatStart(url)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					w, serr := param.writeTabSeparated([]byte("msg"), timestampBytes, []byte(f.Channel), f.Message)
					if serr != nil {
						return fmt.Errorf("start: %v", serr)
					}
					written += w
				}
			}
			if _, serr = param.writer.Write(all); serr != nil {
				return fmt.Errorf("start: %v", serr)
			}
			written += len(all)
		case "end":
			if _, serr = param.writer.Write(all); serr != nil {
				return fmt.Errorf("end: %v", serr)
			}
			written += len(all)
		case "err":
			// Filter has no effect and err
			if _, serr = param.writer.Write(all); serr != nil {
				return fmt.Errorf("err: %v", serr)
			}
			written += len(all)
		default:
			return fmt.Errorf("unsupported line type: %s", typ)
		}
		// Count differently by its channel group
		if cg == sc.ChannelGroupOrderbook {
			param.orderbookWritten += written
		} else {
			param.othersWritten += written
		}
	}
}
