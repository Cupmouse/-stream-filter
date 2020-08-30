package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
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

func filter(db *sql.DB, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) (err error) {
	// find the right gzip files
	rows, serr := db.Query("CALL dataset_info.find_dataset(?, ?)", param.exchange, param.minute)
	if serr != nil {
		err = fmt.Errorf("find_dataset: %v", serr)
		return
	}
	defer func() {
		serr = rows.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("closing rows: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("closing rows: %v", serr)
			}
		}
	}()
	// fetch key by key and read
	var key string
	stop := false
	for rows.Next() && !stop {
		serr := rows.Scan(&key)
		if serr != nil {
			err = fmt.Errorf("scan: %v", serr)
			return
		}
		fmt.Println("reading: ", key)
		stop, err = filterDataset(key, param, form, writer)
		if err != nil {
			return
		}
	}

	return
}

// filterDataset reads gzip from s3 with key and filters out channels not in filterChannels
func filterDataset(key string, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) (stop bool, err error) {

	stop = true
	reader, serr := sc.S3GetAll(key)
	if serr != nil {
		err = serr
		return
	}
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
		err = fmt.Errorf("failed to open gzip stream: %v", serr)
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

	return filterGZipReader(breader, param, form, writer)
}

func filterGZipReader(reader *bufio.Reader, param *FilterParameter, form formatter.Formatter, writer *bufio.Writer) (stop bool, err error) {
	stop = true
	for {
		// read type
		typeBytes, serr := reader.ReadBytes('\t')
		if serr != nil {
			if serr == io.EOF {
				break
			} else {
				// some error
				err = fmt.Errorf("error occurred on new line: %v", serr)
				return
			}
		}
		typeStr := *(*string)(unsafe.Pointer(&typeBytes))

		// ignore status line
		if typeStr == "state\t" {
			// skip line
			_, serr := reader.ReadBytes('\n')
			if serr != nil {
				err = fmt.Errorf("error on skipping a state line: %v", serr)
				return
			}
			continue
		}
		var timestampBytes []byte
		if typeStr == "end\t" {
			// end line does not have message
			timestampBytes, serr = reader.ReadBytes('\n')
			if serr != nil {
				err = fmt.Errorf("error occurred on reading timestamp for end: %v", serr)
				return
			}
		} else {
			// read timestamp
			timestampBytes, serr = reader.ReadBytes('\t')
			if serr != nil {
				err = fmt.Errorf("error occurred on reading timestamp: %v", serr)
				return
			}
		}

		// remove the last byte from string because it is tab/lineterm
		timestampTrimmed := timestampBytes[:len(timestampBytes)-1]
		timestampStr := *(*string)(unsafe.Pointer(&timestampTrimmed))
		timestamp, serr := strconv.ParseInt(timestampStr, 10, 64)
		if serr != nil {
			err = fmt.Errorf("could not convert timestamp string to int64: %v", serr)
			return
		}

		if timestamp < param.start {
			// not reaching the start yet, ignore this line
			_, serr := reader.ReadBytes('\n')
			if serr != nil {
				err = fmt.Errorf("error occurred skipping a line: %v", serr)
				return
			}
			continue
		} else if param.end <= timestamp {
			// if timestamp of this line is out of range specified in parameter then end
			return
		}

		// filtering part
		if typeStr == "msg\t" || typeStr == "send\t" {
			// get channel
			channelBytes, serr := reader.ReadBytes('\t')
			if serr != nil {
				err = fmt.Errorf("error on reading channel: %v", serr)
				return
			}
			// slicing of slice is really cheap operation
			// it won't copy the value in a slice, but creates new slice sharing
			// the same space in memory with different start pos and end pos
			channelTrimmedBytes := channelBytes[:len(channelBytes)-1]
			channelTrimmed := *(*string)(unsafe.Pointer(&channelTrimmedBytes))

			// should this channel be filtered in?
			_, ok := param.filterChannels[channelTrimmed]
			if !ok {
				// not in filter, ignore this line
				// ReadSlice reads until delimiter or until buffer be full
				_, serr := reader.ReadBytes('\n')
				if serr != nil {
					err = fmt.Errorf("error on skipping a line: %v", serr)
					return
				}
				continue
			}
			// filter applied, send message to client
			bytes, serr := reader.ReadBytes('\n')
			if serr != nil {
				err = fmt.Errorf("error on reading a line: %v", serr)
				return
			}
			// formatter is specified, apply it
			if form != nil {
				if typeStr == "send\t" {
					continue // do not format send message
				}
				formatted, serr := form.FormatMessage(channelTrimmed, bytes)
				if serr != nil {
					err = fmt.Errorf("error on formatting: %v", serr)
					return
				}
				for _, f := range formatted {
					_, serr := writer.Write(typeBytes)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.Write(timestampBytes)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.Write(channelBytes)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.Write(f)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.WriteRune('\n')
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
				}
			} else {
				_, serr := writer.Write(typeBytes)
				if serr != nil {
					err = fmt.Errorf("write failed: %v", serr)
					return
				}
				_, serr = writer.Write(timestampBytes)
				if serr != nil {
					err = fmt.Errorf("write failed: %v", serr)
					return
				}
				_, serr = writer.Write(channelBytes)
				if serr != nil {
					err = fmt.Errorf("write failed: %v", serr)
					return
				}
				_, serr = writer.Write(bytes) // this includes line terminator
				if serr != nil {
					err = fmt.Errorf("write failed: %v", serr)
					return
				}
			}
		} else if typeStr == "end\t" {
			// stop filtering
			// but we should not stop as we might have more datasets to filter
			stop = false
			return
		} else if typeStr == "start\t" {
			urlStr, serr := reader.ReadString('\n')
			if serr != nil {
				err = fmt.Errorf("error on reading a line: %v", serr)
				return
			}
			urlStrTrimmed := string(urlStr[:len(urlStr)-1])
			_, serr := writer.Write(typeBytes)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
			_, serr = writer.Write(timestampBytes)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
			_, serr = writer.WriteString(urlStr)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
			// formatter is specified, apply it
			if form != nil {
				formatted, serr := form.FormatStart(urlStrTrimmed)
				if serr != nil {
					err = fmt.Errorf("error on formatting: %v", serr)
					return
				}
				for _, f := range formatted {
					_, serr := writer.WriteString("msg\t")
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.Write(timestampBytes)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.Write(f)
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
					_, serr = writer.WriteRune('\n')
					if serr != nil {
						err = fmt.Errorf("write failed: %v", serr)
						return
					}
				}
			}
		} else {
			// filter has no effect and err
			_, serr := writer.Write(typeBytes)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
			_, serr = writer.Write(timestampBytes)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
			bytes, serr := reader.ReadBytes('\n')
			if serr != nil {
				err = fmt.Errorf("error on reading a line: %v", serr)
				return
			}
			_, serr = writer.Write(bytes)
			if serr != nil {
				err = fmt.Errorf("write failed: %v", serr)
				return
			}
		}
	}
	stop = false
	return
}
