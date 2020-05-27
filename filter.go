package main

import (
	"bufio"
	"compress/gzip"
	"database/sql"
	"errors"
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
	rows, cerr := db.Query("CALL dataset_info.find_dataset(?, ?)", param.exchange, param.minute)
	if cerr != nil {
		err = fmt.Errorf("database querying failed: %s", cerr.Error())
		return
	}
	defer func() {
		cerr = rows.Close()
		if cerr != nil {
			if err != nil {
				err = fmt.Errorf("closing rows failed: %s, original error was: %s", cerr.Error(), err.Error())
			} else {
				err = fmt.Errorf("closing rows failed: %s", cerr.Error())
			}
		}
	}()
	// fetch key by key and read
	var key string
	stop := false
	for rows.Next() && !stop {
		cerr := rows.Scan(&key)
		if cerr != nil {
			err = fmt.Errorf("scan failed: %s", cerr.Error())
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
	reader, cerr := sc.GetS3Object(key)
	if cerr != nil {
		err = fmt.Errorf("failed to fetch s3 object: %s", cerr.Error())
		return
	}
	defer func() {
		// close reader
		cerr := reader.Close()
		if cerr != nil {
			if err != nil {
				err = fmt.Errorf("failed to close reader: %s, original error was %s", cerr.Error(), err.Error())
			} else {
				err = fmt.Errorf("failed to close reader: %s", cerr.Error())
			}
		}
	}()
	// wrap s3 object reader into gzip reader to read as gzip
	greader, cerr := gzip.NewReader(reader)
	if cerr != nil {
		err = errors.New("failed to open gzip stream: " + cerr.Error())
		return
	}
	// this ensures both reader will be closed
	defer func() {
		// close gzip reader
		// this won't close underlying reader
		cerr := greader.Close()
		if cerr != nil {
			if err != nil {
				err = fmt.Errorf("failed to close greader: %s, original error was %s", cerr.Error(), err.Error())
				return
			}
			err = fmt.Errorf("failed to close greader: %s", cerr.Error())
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
		typeBytes, cerr := reader.ReadBytes('\t')
		if cerr != nil {
			if cerr == io.EOF {
				break
			} else {
				// some error
				err = fmt.Errorf("error occurred on new line: %s", cerr.Error())
				return
			}
		}
		typeStr := *(*string)(unsafe.Pointer(&typeBytes))

		// ignore status line
		if typeStr == "state\t" {
			// skip line
			_, cerr := reader.ReadBytes('\n')
			if cerr != nil {
				err = fmt.Errorf("error on skipping a state line: %s", cerr.Error())
				return
			}
			continue
		}
		var timestampBytes []byte
		if typeStr == "end\t" {
			// end line does not have message
			timestampBytes, cerr = reader.ReadBytes('\n')
			if cerr != nil {
				err = fmt.Errorf("error occurred on reading timestamp for end: %s", cerr.Error())
				return
			}
		} else {
			// read timestamp
			timestampBytes, cerr = reader.ReadBytes('\t')
			if cerr != nil {
				err = fmt.Errorf("error occurred on reading timestamp: %s", cerr.Error())
				return
			}
		}

		// remove the last byte from string because it is tab/lineterm
		timestampTrimmed := timestampBytes[:len(timestampBytes)-1]
		timestampStr := *(*string)(unsafe.Pointer(&timestampTrimmed))
		timestamp, cerr := strconv.ParseInt(timestampStr, 10, 64)
		if cerr != nil {
			err = fmt.Errorf("could not convert timestamp string to int64: %s", cerr.Error())
			return
		}

		if timestamp < param.start {
			// not reaching the start yet, ignore this line
			_, cerr := reader.ReadBytes('\n')
			if cerr != nil {
				err = fmt.Errorf("error occurred skipping a line: %s", cerr.Error())
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
			channelBytes, cerr := reader.ReadBytes('\t')
			if cerr != nil {
				err = fmt.Errorf("error on reading channel: %s", cerr.Error())
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
				_, cerr := reader.ReadBytes('\n')
				if cerr != nil {
					err = fmt.Errorf("error on skipping a line: %s", cerr.Error())
					return
				}
				continue
			}
			// filter applied, send message to client
			bytes, cerr := reader.ReadBytes('\n')
			if cerr != nil {
				err = fmt.Errorf("error on reading a line: %s", cerr.Error())
				return
			}
			// filter is specified, apply it
			if form != nil {
				if typeStr == "send\t" {
					continue // do not format send message
				}
				formatted, cerr := form.Format(channelTrimmed, bytes)
				if cerr != nil {
					err = fmt.Errorf("error on formatting: %s", cerr.Error())
					return
				}
				for _, f := range formatted {
					_, cerr := writer.Write(typeBytes)
					if cerr != nil {
						err = fmt.Errorf("write failed: %s", cerr.Error())
						return
					}
					_, cerr = writer.Write(timestampBytes)
					if cerr != nil {
						err = fmt.Errorf("write failed: %s", cerr.Error())
						return
					}
					_, cerr = writer.Write(channelBytes)
					if cerr != nil {
						err = fmt.Errorf("write failed: %s", cerr.Error())
						return
					}
					_, cerr = writer.Write(f)
					if cerr != nil {
						err = fmt.Errorf("write failed: %s", cerr.Error())
						return
					}
					_, cerr = writer.WriteRune('\n')
					if cerr != nil {
						err = fmt.Errorf("write failed: %s", cerr.Error())
						return
					}
				}
			} else {
				_, cerr := writer.Write(typeBytes)
				if cerr != nil {
					err = fmt.Errorf("write failed: %s", cerr.Error())
					return
				}
				_, cerr = writer.Write(timestampBytes)
				if cerr != nil {
					err = fmt.Errorf("write failed: %s", cerr.Error())
					return
				}
				_, cerr = writer.Write(channelBytes)
				if cerr != nil {
					err = fmt.Errorf("write failed: %s", cerr.Error())
					return
				}
				_, cerr = writer.Write(bytes) // this includes line terminator
				if cerr != nil {
					err = fmt.Errorf("write failed: %s", cerr.Error())
					return
				}
			}
		} else if typeStr == "end\t" {
			// stop filtering
			// but we should not stop as we might have more datasets to filter
			stop = false
			return
		} else {
			// filter has no effect on start and err
			_, cerr := writer.Write(typeBytes)
			if cerr != nil {
				err = fmt.Errorf("write failed: %s", cerr.Error())
				return
			}
			_, cerr = writer.Write(timestampBytes)
			if cerr != nil {
				err = fmt.Errorf("write failed: %s", cerr.Error())
				return
			}
			bytes, cerr := reader.ReadBytes('\n')
			if cerr != nil {
				err = fmt.Errorf("error on reading a line: %s", cerr.Error())
				return
			}
			_, cerr = writer.Write(bytes)
			if cerr != nil {
				err = fmt.Errorf("write failed: %s", cerr.Error())
				return
			}
		}
	}
	stop = false
	return
}
