// recover
package main

import (
	"log"
	"os"
	"io"
	"encoding/gob"
	"bytes"
)

func bytesToInt(b []byte) int {
	return int(b[0]) + int(b[1])<<8 + int(b[2])<<16 + int(b[3])<<24
}


func bytesToUInt64(b []byte) uint64 {
	return uint64(b[0]) + uint64(b[1])<<8 + uint64(b[2])<<16 + uint64(b[3])<<24 + uint64(b[4])<<32 + uint64(b[5])<<40 + uint64(b[6])<<48 + uint64(b[7])<<56
}

func indexOf(slice []int, pos int) int {
	for i, elm := range slice {
		if elm == pos {
			return i
		}
	}
	
	return -1
}

func getMessageId(f *os.File) (uint64, error) {	
	if pos, err := f.Seek(0, 2); err != nil || pos == 0 {
		return 0, err
	}
	
	if _, err := f.Seek(-13, 1); err != nil {
		return 0, err
	}

	b := make([]byte, 13)
	if _, err := f.Read(b); err != nil && err != io.EOF {
		return 0, err
	}
	
	return bytesToUInt64(b), nil
}

func recover(f *os.File, process func(*MessageRecord) error) error {
	var pos int64
	var err error
	
	if pos, err = f.Seek(0, 2); err != nil {
		log.Println("Unable to read the record", err)
		return nil
	}
	
	maxReadMsgCount := -1
	msgReadCount := 0
	var ackOffsets []int

	log.Println("Start recovery...")
	for pos > 0 && (msgReadCount < maxReadMsgCount || maxReadMsgCount < 0) {
		msgReadCount++
		b := make([]byte, 13)
				
		if pos, err = f.Seek(-13, 1); err != nil {
			return err
		}		

		if _, err = f.Read(b); err != nil && err != io.EOF {
			return err
		}

		msgId := bytesToUInt64(b)
		msgLen := bytesToInt(b[9:])
		
		switch b[8] {
		case 1:
			if i := indexOf(ackOffsets, msgReadCount); i >= 0 {
				ackOffsets = append(ackOffsets[:i], ackOffsets[i+1:]...)
				
				if _, err = f.Seek(-13, 1); err != nil {
					return err
				}
				
				log.Printf("%d: IGNORE %d, ACKED\n", msgReadCount, msgId)
			} else {
				if _, err = f.Seek(int64(-13-msgLen), 1); err != nil {
					return err
				}
				
				b := make([]byte, msgLen)
				if _, err = f.Read(b); err != nil {
					log.Println("Failed to read message", err)
					return err
				}
				
				var data MessageRecord
				decode := gob.NewDecoder(bytes.NewReader(b))

				if err := decode.Decode(&data); err != nil {
					log.Println("Failed to decode message", err)
				} else {
					if err = process(&data); err != nil {
						return err
					}
				}

				log.Printf("%d: REPLAY %d\n", msgReadCount, msgId)
			}
						
		case 2:
			if _, err = f.Seek(int64(-13-msgLen), 1); err != nil {
				return err
			}
			
			b = make([]byte, msgLen)
			
			if _, err = f.Read(b); err != nil {
				return err
			}
			
			msgOffset := bytesToInt(b)
			unAckedMsgOffset := bytesToInt(b[4:])
			ackOffsets = append(ackOffsets, msgReadCount + msgOffset)
			if maxReadMsgCount < 0 {
				maxReadMsgCount = msgReadCount + unAckedMsgOffset
			}
						
			log.Printf("%d: ACK %d\n", msgReadCount, msgId)
		
		case 3:
			if i := indexOf(ackOffsets, msgReadCount); i >= 0 {
				ackOffsets = append(ackOffsets[:i], ackOffsets[i+1:]...)

				if _, err = f.Seek(int64(-13), 1); err != nil {
					return err
				}
				
				log.Printf("%d: IGNORE %d, ACKED RETRIED\n", msgReadCount, msgId)
			} else {				
				if _, err = f.Seek(int64(-13-msgLen), 1); err != nil {
					return err
				}
				
				b := make([]byte, msgLen)
				if _, err = f.Read(b); err != nil {
					log.Println("Failed to read message", err)
					return err
				}
				
				c := b[msgLen - 8:]

				msgOffset := bytesToInt(c)
				unAckedMsgOffset := bytesToInt(c[4:])
				
				if maxReadMsgCount < 0 {
					maxReadMsgCount = msgReadCount + unAckedMsgOffset
				}
								
				var data MessageRecord
				decode := gob.NewDecoder(bytes.NewReader(b))
				
				if err := decode.Decode(&data); err != nil {
					if unAckedMsgOffset < msgOffset {
						maxReadMsgCount = msgReadCount + msgOffset
					}
					log.Fatal("Failed to decode message", err)
				} else {
					if err = process(&data); err != nil {
						return err
					}
					
					ackOffsets = append(ackOffsets, msgReadCount + msgOffset)
				}
				
				log.Printf("%d: REPLAY %d, RETRIED\n", msgReadCount, msgId)
			}
		}

		if pos, err = f.Seek(int64(-msgLen), 1); err != nil {
			return err
		}
	}
	
	log.Println("Recovery completed.")
	return nil
}
