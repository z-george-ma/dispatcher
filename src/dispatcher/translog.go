// translog
package main

import (
	"bytes"
	"os"
	"log"
	"encoding/binary"
	"encoding/gob"
)

type TransactionLog struct {
	file *os.File
	deadletter *os.File
	metadata []messageMetaData
	head uint
	maxID uint64
	lock chan bool
}

type messageMetaData struct {
	messageID uint64
	transLogPos uint
}

func NewTransactionLog(config Config, startId uint64) (*TransactionLog, error) {
	var f *os.File
	var df *os.File
	var err error
	
	if config.EventSourcing {
		if f, err = os.OpenFile(config.Log, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0600); err != nil {
			return nil, err
		}
	} else {
		if f, err = os.Create(config.Log); err != nil {
			return nil, err
		}
	}

	if df, err = os.OpenFile(config.DeadLetter, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0600); err != nil {
		return nil, err
	}

	tlog := TransactionLog{
		file: f,
		deadletter: df,
		metadata: make([]messageMetaData, 0, 4096),
		maxID: startId,
		lock: make(chan bool, 1),
	}

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
	b1 := make([]byte, 13)

	b1[8] = 1 // Type: 1 - Message
	binary.LittleEndian.PutUint32(b1[9:], uint32(len(b)))

	<-tlog.lock
	tlog.maxID++
	data.ID = tlog.maxID
	
	binary.LittleEndian.PutUint64(b1, data.ID) // messageId(8)
	b = append(b, b1...)


	tlog.metadata = append(tlog.metadata, messageMetaData{data.ID, tlog.head})
	_, err := tlog.file.Write(b)	
	tlog.head++
	tlog.lock <- true
	return err
}

func indexOfMessageMetaData(slice []messageMetaData, messageID uint64) (int, messageMetaData) {
	for i, elm := range slice {
		if elm.messageID == messageID {
			return i, elm
		}
	}
	
	return -1, messageMetaData{}
}

func (tlog *TransactionLog) WriteAck(ID uint64) error {
	var err error
	<-tlog.lock
	
	i, elm := indexOfMessageMetaData(tlog.metadata, ID)
	
	if i < 0 {
		tlog.lock <- true
		return nil
	}

	tlog.metadata = append(tlog.metadata[:i], tlog.metadata[i+1:]...)
	b := make([]byte, 21) // messageOffset(4), unAckedMessageOffset(4), messageId(8), type(1), length(4)


	binary.LittleEndian.PutUint32(b, uint32(tlog.head - elm.transLogPos)) // messageOffset(4)
	
	if len(tlog.metadata) > 0 {
		binary.LittleEndian.PutUint32(b[4:], uint32(tlog.head - tlog.metadata[0].transLogPos)) // unAckedMessageOffset(4)
	}
	
	binary.LittleEndian.PutUint64(b[8:], ID) // messageId(8)
	
	b[16] = 2 // Type: 2 - Ack
	binary.LittleEndian.PutUint32(b[17:], uint32(8))
	_, err = tlog.file.Write(b)
	tlog.head++
	
	tlog.lock <- true
	return err
}

func (tlog *TransactionLog) WriteRetry(data *MessageRecord) error {
	<-tlog.lock
	i, elm := indexOfMessageMetaData(tlog.metadata, data.ID)
	
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
	tlog.metadata = append(tlog.metadata, messageMetaData{data.ID, tlog.head})
	
	b1 := make([]byte, 21)

	binary.LittleEndian.PutUint32(b1, uint32(tlog.head - elm.transLogPos)) // messageOffset(4)
	binary.LittleEndian.PutUint32(b1[4:], uint32(tlog.head - tlog.metadata[0].transLogPos)) // unAckedMessageOffset(4)
	binary.LittleEndian.PutUint64(b1[8:], data.ID) // messageId(8)
	
	b1[16] = 3 // Type: 3 - Retry
	binary.LittleEndian.PutUint32(b1[17:], uint32(len(b) + 8))
	b = append(b, b1...)

	_, err := tlog.file.Write(b)
	tlog.head++
	tlog.lock <- true
	return err
}


func (tlog *TransactionLog) Reconstruct(data *MessageRecord) {
	tlog.metadata = append(tlog.metadata, messageMetaData{})
	copy(tlog.metadata[1:], tlog.metadata)
	tlog.metadata[0] = messageMetaData{data.ID, tlog.head}
	tlog.head++
}

func (tlog *TransactionLog) WriteDeadLetter(data *MessageRecord) error {
	log.Println("Sending message to dead letter queue. Message ID:", data.ID)
	
	<-tlog.lock
	
	var buf bytes.Buffer
	encode := gob.NewEncoder(&buf)
	
	if err := encode.Encode(data); err == nil {
		b := buf.Bytes()

		b1 := make([]byte, 4)

		binary.LittleEndian.PutUint32(b1, uint32(len(b)))
		b = append(b, b1...)
	
		if _, err = tlog.deadletter.Write(b); err != nil {
			log.Println(err)
		}
	} else {
		log.Println(err)
	}

	tlog.lock <- true
	
	return tlog.WriteAck(data.ID)
}
