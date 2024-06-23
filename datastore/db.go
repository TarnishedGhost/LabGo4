package datastore

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	outputFileName   = "current-data"
	bufferSize       = 8192
	segmentThreshold = 3
)

type hashMapIndex map[string]int64

type IndexOperation struct {
	isWriteOperation bool
	key              string
	index            int64
}

type InsertOperation struct {
	record entry
	result chan error
}

type KeyLocation struct {
	segment  *Segment
	position int64
}

type FetchOperation struct {
	key        string
	result     chan string
	errorResult chan error
}

type SegmentController struct {
	segments         []*Segment
	lastSegmentIndex int
}

type Database struct {
	outputFile      *os.File
	outputFilePath  string
	outputFileOffset int64
	directory       string
	segmentSize     int64
	indexOperations chan IndexOperation
	keyLocations    chan *KeyLocation
	insertOperations chan InsertOperation
	insertDone      chan error
	fetchOperations chan FetchOperation
	segmentController *SegmentController
	fileMutex       sync.Mutex
	indexMutex      sync.Mutex
}

type Segment struct {
	index    hashMapIndex
	filePath string
}

var (
	ErrorRecordNotFound = fmt.Errorf("record does not exist")
)

func NewDatabase(directory string, segmentSize int64) (*Database, error) {
	db := &Database{
		segmentController: &SegmentController{
			segments: make([]*Segment, 0),
		},
		directory:       directory,
		segmentSize:     segmentSize,
		indexOperations: make(chan IndexOperation),
		keyLocations:    make(chan *KeyLocation),
		insertOperations: make(chan InsertOperation),
		insertDone:      make(chan error),
		fetchOperations: make(chan FetchOperation, 10), // Limit the number of concurrent read operations
	}

	if err := db.createSegment(); err != nil {
		return nil, err
	}

	if err := db.recoverAll(); err != nil && err != io.EOF {
		return nil, err
	}

	db.startIndexRoutine()
	db.startInsertRoutine()
	db.startFetchWorkers(5)

	return db, nil
}

func (db *Database) Close() error {
	return db.outputFile.Close()
}

func (db *Database) startIndexRoutine() {
	go func() {
		for operation := range db.indexOperations {
			db.indexMutex.Lock()
			if operation.isWriteOperation {
				db.setKey(operation.key, operation.index)
			} else {
				segment, position, err := db.getSegmentAndPosition(operation.key)
				if err != nil {
					db.keyLocations <- nil
				} else {
					db.keyLocations <- &KeyLocation{segment, position}
				}
			}
			db.indexMutex.Unlock()
		}
	}()
}

func (db *Database) startInsertRoutine() {
	go func() {
		for {
			operation := <-db.insertOperations
			db.fileMutex.Lock()
			length := operation.record.GetLength()
			stats, err := db.outputFile.Stat()
			if err != nil {
				operation.result <- err
				db.fileMutex.Unlock()
				continue
			}
			if stats.Size()+length > db.segmentSize {
				err := db.createSegment()
				if err != nil {
					operation.result <- err
					db.fileMutex.Unlock()
					continue
				}
			}
			n, err := db.outputFile.Write(operation.record.Encode())
			if err == nil {
				db.indexOperations <- IndexOperation{
					isWriteOperation: true,
					key:              operation.record.key,
					index:            int64(n),
				}
			}
			operation.result <- nil
			db.fileMutex.Unlock()
		}
	}()
}

func (db *Database) startFetchWorkers(workerCount int) {
	for i := 0; i < workerCount; i++ {
		go func() {
			for operation := range db.fetchOperations {
				keyLocation := db.getPosition(operation.key)
				if keyLocation == nil {
					operation.errorResult <- ErrorRecordNotFound
				} else {
					value, err := keyLocation.segment.getFromSegment(keyLocation.position)
					if err != nil {
						operation.errorResult <- err
					} else {
						operation.result <- value
					}
				}
			}
		}()
	}
}

func (db *Database) createSegment() error {
	filePath := db.generateNewFileName()
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return err
	}

	newSegment := &Segment{
		filePath: filePath,
		index:    make(hashMapIndex),
	}

	db.outputFile = file
	db.outputFileOffset = 0
	db.outputFilePath = filePath
	db.segmentController.segments = append(db.segmentController.segments, newSegment)
	if len(db.segmentController.segments) >= segmentThreshold {
		db.performOldSegmentsCompaction()
	}

	return nil
}

func (db *Database) generateNewFileName() string {
	result := filepath.Join(db.directory, fmt.Sprintf("%s%d", outputFileName, db.segmentController.lastSegmentIndex))
	db.segmentController.lastSegmentIndex++
	return result
}

func (db *Database) performOldSegmentsCompaction() {
	go func() {
		filePath := db.generateNewFileName()
		newSegment := &Segment{
			filePath: filePath,
			index:    make(hashMapIndex),
		}
		var offset int64
		file, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
		if err != nil {
			return
		}
		lastSegmentIndex := len(db.segmentController.segments) - 2
		for i := 0; i <= lastSegmentIndex; i++ {
			segment := db.segmentController.segments[i]
			for key, index := range segment.index {
				if i < lastSegmentIndex {
					isInNewerSegments := findKeyInSegments(db.segmentController.segments[i+1:lastSegmentIndex+1], key)
					if isInNewerSegments {
						continue
					}
				}
				value, _ := segment.getFromSegment(index)
				record := entry{
					key:   key,
					value: value,
				}
				n, err := file.Write(record.Encode())
				if err == nil {
					newSegment.index[key] = offset
					offset += int64(n)
				}
			}
		}
		db.segmentController.segments = []*Segment{newSegment, db.getLastSegment()}
	}()
}

func findKeyInSegments(segments []*Segment, key string) bool {
	for _, segment := range segments {
		if _, ok := segment.index[key]; ok {
			return true
		}
	}
	return false
}

func (db *Database) recoverAll() error {
	for _, segment := range db.segmentController.segments {
		if err := db.recoverSegment(segment); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) recoverSegment(segment *Segment) error {
	file, err := os.Open(segment.filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := db.recover(file); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (db *Database) recover(file *os.File) error {
	var err error
	var buffer [bufferSize]byte

	reader := bufio.NewReaderSize(file, bufferSize)
	for err == nil {
		var (
			header, data []byte
			n            int
		)
		header, err = reader.Peek(bufferSize)
		if err == io.EOF {
			if len(header) == 0 {
				return err
			}
		} else if err != nil {
			return err
		}
		size := binary.LittleEndian.Uint32(header)

		if size < bufferSize {
			data = buffer[:size]
		} else {
			data = make([]byte, size)
		}
		n, err = reader.Read(data)

		if err == nil {
			if n != int(size) {
				return fmt.Errorf("corrupted file")
			}

			var record entry
			record.Decode(data)
			db.setKey(record.key, int64(n))
		}
	}
	return err
}

func (db *Database) setKey(key string, n int64) {
	db.getLastSegment().index[key] = db.outputFileOffset
	db.outputFileOffset += n
}

func (db *Database) getSegmentAndPosition(key string) (*Segment, int64, error) {
	for i := range db.segmentController.segments {
		segment := db.segmentController.segments[len(db.segmentController.segments)-i-1]
		position, ok := segment.index[key]
		if ok {
			return segment, position, nil
		}
	}

	return nil, 0, ErrorRecordNotFound
}

func (db *Database) getPosition(key string) *KeyLocation {
	operation := IndexOperation{
		isWriteOperation: false,
		key:              key,
	}
	db.indexOperations <- operation
	return <-db.keyLocations
}

func (db *Database) Get(key string) (string, error) {
	result := make(chan string)
	errorResult := make(chan error)
	db.fetchOperations <- FetchOperation{
		key:        key,
		result:     result,
		errorResult: errorResult,
	}

	select {
	case value := <-result:
		return value, nil
	case err := <-errorResult:
		return "", err
	}
}

func (db *Database) Put(key, value string) error {
	result := make(chan error)
	db.insertOperations <- InsertOperation{
		record: entry{
			key:   key,
			value: value,
		},
		result: result,
	}
	err := <-result
	close(result)
	return err
}

func (db *Database) getLastSegment() *Segment {
	return db.segmentController.segments[len(db.segmentController.segments)-1]
}

func (segment *Segment) getFromSegment(position int64) (string, error) {
	file, err := os.Open(segment.filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	reader := bufio.NewReader(file)
	value, err := readValue(reader)
	if err != nil {
		return "", err
	}
	return value, nil
}
