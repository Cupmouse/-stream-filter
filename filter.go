package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"unsafe"

	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
)

// FilterParameter is the parameter needed to filter lines
type FilterParameter struct {
	exchange       string
	filterChannels map[string]bool
	minute         int64
	start          int64
	end            int64
	format         string
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

// filterGZip reads gzip from s3 with key and filters out channels not in filterChannels
func filterGZip(reader *bufio.Reader, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) error {
	for {
		// Read type
		typeBytes, serr := reader.ReadBytes('\t')
		if serr != nil {
			if serr == io.EOF {
				return nil
			}
			return fmt.Errorf("new line: %v", serr)
		}
		typeStr := *(*string)(unsafe.Pointer(&typeBytes))

		// Ignore status line
		if typeStr == "state\t" {
			// Skip line
			_, serr := reader.ReadBytes('\n')
			if serr != nil {
				return fmt.Errorf("skipping state line: %v", serr)
			}
			continue
		}
		var timestampBytes []byte
		if typeStr == "end\t" {
			// End line does not have message
			timestampBytes, serr = reader.ReadBytes('\n')
			if serr != nil {
				return fmt.Errorf("timestamp for end: %v", serr)
			}
		} else {
			// Read timestamp
			timestampBytes, serr = reader.ReadBytes('\t')
			if serr != nil {
				return fmt.Errorf("timestamp: %v", serr)
			}
		}

		// Remove the last byte from string because it is tab/lineterm
		timestampTrimmed := timestampBytes[:len(timestampBytes)-1]
		timestampStr := *(*string)(unsafe.Pointer(&timestampTrimmed))
		timestamp, serr := strconv.ParseInt(timestampStr, 10, 64)
		if serr != nil {
			return fmt.Errorf("timestamp string to int64: %v", serr)
		}

		if timestamp < param.start {
			// Not reaching the start yet, ignore this line
			_, serr := reader.ReadBytes('\n')
			if serr != nil {
				return fmt.Errorf("skipping line: %v", serr)
			}
			continue
		} else if param.end <= timestamp {
			// If timestamp of this line is out of range specified in parameter then end
			return nil
		}

		// Filtering part
		if typeStr == "msg\t" || typeStr == "send\t" {
			// Get channel
			channelBytes, serr := reader.ReadBytes('\t')
			if serr != nil {
				return fmt.Errorf("reading channel: %v", serr)
			}
			// Slicing of slice is really cheap operation
			// It won't copy the value in a slice, but creates new slice sharing
			// The same space in memory with different start pos and end pos
			channelTrimmedBytes := channelBytes[:len(channelBytes)-1]
			channelTrimmed := *(*string)(unsafe.Pointer(&channelTrimmedBytes))

			// Should this channel be filtered in?
			_, ok := param.filterChannels[channelTrimmed]
			if !ok {
				// Not in filter, ignore this line
				// ReadSlice reads until delimiter or until buffer be full
				_, serr := reader.ReadBytes('\n')
				if serr != nil {
					return fmt.Errorf("skipping line: %v", serr)
				}
				continue
			}
			// Filter applied, send message to client
			bytes, serr := reader.ReadBytes('\n')
			if serr != nil {
				return fmt.Errorf("reading line: %v", serr)
			}
			// Formatter is specified, apply it
			if form != nil {
				if typeStr == "send\t" {
					// Do not format send message
					continue
				}
				formatted, serr := form.FormatMessage(channelTrimmed, bytes)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					_, serr := writer.Write(typeBytes)
					if serr != nil {
						return fmt.Errorf("typeBytes: %v", serr)
					}
					_, serr = writer.Write(timestampBytes)
					if serr != nil {
						return fmt.Errorf("timestampBytes: %v", serr)
					}
					_, serr = writer.Write(channelBytes)
					if serr != nil {
						return fmt.Errorf("channelBytes: %v", serr)
					}
					_, serr = writer.Write(f)
					if serr != nil {
						return fmt.Errorf("f: %v", serr)
					}
					_, serr = writer.WriteRune('\n')
					if serr != nil {
						return fmt.Errorf("line terminator: %v", serr)
					}
				}
			} else {
				_, serr := writer.Write(typeBytes)
				if serr != nil {
					return fmt.Errorf("typeBytes: %v", serr)
				}
				_, serr = writer.Write(timestampBytes)
				if serr != nil {
					return fmt.Errorf("timestampBytes: %v", serr)
				}
				_, serr = writer.Write(channelBytes)
				if serr != nil {
					return fmt.Errorf("channelBytes: %v", serr)
				}
				// This includes line terminator
				_, serr = writer.Write(bytes)
				if serr != nil {
					return fmt.Errorf("bytes: %v", serr)
				}
			}
		} else if typeStr == "end\t" {
			// Stop filtering
			// But we should not stop as we might have more datasets to filter
			return nil
		} else if typeStr == "start\t" {
			urlStr, serr := reader.ReadString('\n')
			if serr != nil {
				return fmt.Errorf("reading line: %v", serr)
			}
			urlStrTrimmed := string(urlStr[:len(urlStr)-1])
			_, serr = writer.Write(typeBytes)
			if serr != nil {
				return fmt.Errorf("typeBytes: %v", serr)
			}
			_, serr = writer.Write(timestampBytes)
			if serr != nil {
				return fmt.Errorf("timestampBytes: %v", serr)
			}
			_, serr = writer.WriteString(urlStr)
			if serr != nil {
				return fmt.Errorf("urlStr: %v", serr)
			}
			// Formatter is specified, apply it
			if form != nil {
				formatted, serr := form.FormatStart(urlStrTrimmed)
				if serr != nil {
					return fmt.Errorf("formatting: %v", serr)
				}
				for _, f := range formatted {
					if _, ok := param.filterChannels[f.Channel]; !ok {
						continue
					}
					_, serr := writer.WriteString("msg\t")
					if serr != nil {
						return fmt.Errorf("msg: %v", serr)
					}
					_, serr = writer.Write(timestampBytes)
					if serr != nil {
						return fmt.Errorf("timestampBytes: %v", serr)
					}
					_, serr = writer.WriteString(f.Channel)
					if serr != nil {
						return fmt.Errorf("channel: %v", serr)
					}
					_, serr = writer.WriteRune('\t')
					if serr != nil {
						return fmt.Errorf("tab: %v", serr)
					}
					_, serr = writer.Write(f.Message)
					if serr != nil {
						return fmt.Errorf("message: %v", serr)
					}
					_, serr = writer.WriteRune('\n')
					if serr != nil {
						return fmt.Errorf("line terminator: %v", serr)
					}
				}
			}
		} else {
			// Filter has no effect and err
			_, serr := writer.Write(typeBytes)
			if serr != nil {
				return fmt.Errorf("typeBytes: %v", serr)
			}
			_, serr = writer.Write(timestampBytes)
			if serr != nil {
				return fmt.Errorf("timestampBytes: %v", serr)
			}
			bytes, serr := reader.ReadBytes('\n')
			if serr != nil {
				return fmt.Errorf("reading line: %v", serr)
			}
			_, serr = writer.Write(bytes)
			if serr != nil {
				return fmt.Errorf("bytes: %v", serr)
			}
		}
	}
}
