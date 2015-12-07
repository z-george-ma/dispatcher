// translog
package main

import (
	"bytes"
	"os"
	"encoding/binary"
	"encoding/gob"
)

type TransactionLog struct {
	file *os.File
	deadletter *os.File
	metadata []messageMetaData
	head uint
	lock chan bool
}

type messageMetaData struct {
	messageUUID string
	transLogPos uint
}

func NewTransactionLog(file string) (*TransactionLog, error) {
	tlog := TransactionLog{}
	
	f, err := os.Create(file)
	if err != nil {
		return nil, err
	}
	
	tlog.file = f
	tlog.lock = make(chan bool, 1)
	tlog.lock <- true
	return &tlog, nil
}

func (tlog *TransactionLog) Write(data *MessageRecord) error {
	var buf bytes.Buffer
	encode := gob.NewEncoder(&buf)
	
	if err := encode.Encode(data); err != nil {
		return err
	}
	
	b := buf.Bytes()
	buf5 := make([]byte, 5)
	buf5[0] = 1 // Type: 1 - Message
	binary.LittleEndian.PutUint32(buf5[1:], uint32(len(b)))
	b = append(b, buf5...)

	<-tlog.lock
	tlog.metadata = append(tlog.metadata, messageMetaData{data.UUID, tlog.head})
	_, err := tlog.file.Write(b)	
	tlog.head++
	tlog.lock <- true
	return err
}

func indexOfMessageMetaData(slice []messageMetaData, messageUUID string) (int, messageMetaData) {
	for i, elm := range slice {
		if elm.messageUUID == messageUUID {
			return i, elm
		}
	}
	
	return -1, messageMetaData{}
}

func (tlog *TransactionLog) WriteAck(UUID string) error {
	var err error
	<-tlog.lock
	
	i, elm := indexOfMessageMetaData(tlog.metadata, UUID)
	
	if i >= 0 {
		tlog.metadata = append(tlog.metadata[:i], tlog.metadata[i+1:]...)
		b := make([]byte, 13) // messageOffset(4), unAckedMessageOffset(4), type(1), length(4)
		
		binary.LittleEndian.PutUint32(b[0:4], uint32(tlog.head - elm.transLogPos)) // messageOffset(4)
		
		if len(tlog.metadata) > 0 {
			binary.LittleEndian.PutUint32(b[4:8], uint32(tlog.head - tlog.metadata[0].transLogPos)) // unAckedMessageOffset(4)
		}
		
		b[8] = 2 // Type: 2 - Ack
		binary.LittleEndian.PutUint32(b[9:], uint32(8))
		_, err = tlog.file.Write(b)
		tlog.head++
	}
	
	tlog.lock <- true
	return err
}

func (tlog *TransactionLog) WriteRetry(data *MessageRecord) error {
	<-tlog.lock
	i, elm := indexOfMessageMetaData(tlog.metadata, data.UUID)
	
	if i < 0 {
		tlog.lock <- true
		return tlog.Write(data)
	}
	
	var buf bytes.Buffer
	encode := gob.NewEncoder(&buf)
	
	if err := encode.Encode(data); err != nil {
		return err
	}
	
	b := buf.Bytes()

	tlog.metadata = append(tlog.metadata[:i], tlog.metadata[i+1:]...)
	tlog.metadata = append(tlog.metadata, messageMetaData{data.UUID, tlog.head})
	
	b1 := make([]byte, 13)
	binary.LittleEndian.PutUint32(b1[0:4], uint32(tlog.head - elm.transLogPos)) // messageOffset(4)
	binary.LittleEndian.PutUint32(b1[4:8], uint32(tlog.head - tlog.metadata[0].transLogPos)) // unAckedMessageOffset(4)
	
	b1[8] = 3 // Type: 3 - Retry
	binary.LittleEndian.PutUint32(b1[9:], uint32(len(b) + 8))
	b = append(b, b1...)

	_, err := tlog.file.Write(b)
	tlog.head++
	tlog.lock <- true
	return err
}

func (tlog *TransactionLog) WriteDeadLetter(data *MessageRecord) error {
	<-tlog.lock
	i, elm := indexOfMessageMetaData(tlog.metadata, data.UUID)
	
	if i < 0 {
		tlog.lock <- true
		return tlog.Write(data)
	}
	
	var buf bytes.Buffer
	encode := gob.NewEncoder(&buf)
	
	if err := encode.Encode(data); err != nil {
		return err
	}
	
	b := buf.Bytes()

	tlog.metadata = append(tlog.metadata[:i], tlog.metadata[i+1:]...)
	tlog.metadata = append(tlog.metadata, messageMetaData{data.UUID, tlog.head})
	
	b1 := make([]byte, 13)
	binary.LittleEndian.PutUint32(b1[0:4], uint32(tlog.head - elm.transLogPos)) // messageOffset(4)
	binary.LittleEndian.PutUint32(b1[4:8], uint32(tlog.head - tlog.metadata[0].transLogPos)) // unAckedMessageOffset(4)
	
	b1[8] = 3 // Type: 3 - Retry
	binary.LittleEndian.PutUint32(b1[9:], uint32(len(b) + 8))
	b = append(b, b1...)

	_, err := tlog.file.Write(b)
	tlog.head++
	tlog.lock <- true
	return err
}
