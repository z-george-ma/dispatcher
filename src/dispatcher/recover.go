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

func indexOf(slice []int, pos int) int {
	for i, elm := range slice {
		if elm == pos {
			return i
		}
	}
	
	return -1
}

func recover(file string, process func(*MessageRecord) error) error {
	f, err := os.OpenFile(file, os.O_RDONLY, 500)
	if os.IsNotExist(err) {
		return nil
	}
	
	var pos int64
	if pos, err = f.Seek(0, 2); err != nil {
		log.Println("Unable to read the record", err)
		return nil
	}
	
	maxReadMsgCount := -1
	msgReadCount := 0
	var ackOffsets []int

	log.Println("Start recovery...")
	for (pos > 0 && (msgReadCount < maxReadMsgCount || maxReadMsgCount < 0)) {
		msgReadCount++
		log.Println("Recovering message", msgReadCount)
		b := make([]byte, 5)
				
		if pos, err = f.Seek(-5, 1); err != nil {
			return err
		}		

		if _, err = f.Read(b); err != nil && err != io.EOF {
			return err
		}

		msgLen := bytesToInt(b[1:])
		switch b[0] {
		case 1:
			if i := indexOf(ackOffsets, msgReadCount); i >= 0 {
				ackOffsets = append(ackOffsets[:i], ackOffsets[i+1:]...)
				
				if _, err = f.Seek(int64(-5-msgLen), 1); err != nil {
					return err
				}
			} else {
				if _, err = f.Seek(int64(-5-msgLen), 1); err != nil {
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
				
				if _, err = f.Seek(int64(-msgLen), 1); err != nil {
					return err
				}
			}			
		case 2:
			if _, err = f.Seek(int64(-5-msgLen), 1); err != nil {
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
						
			if _, err = f.Seek(int64(-msgLen), 1); err != nil {
				return err
			}
		
		case 3:
			if i := indexOf(ackOffsets, msgReadCount); i >= 0 {
				ackOffsets = append(ackOffsets[:i], ackOffsets[i+1:]...)
				
				if _, err = f.Seek(int64(-5-msgLen), 1); err != nil {
					return err
				}
			} else {
				if _, err = f.Seek(int64(-5-msgLen), 1); err != nil {
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
				
				if _, err = f.Seek(int64(-msgLen), 1); err != nil {
					return err
				}	
			}
		}
		
		if pos, err = f.Seek(0, 1); err != nil {
			return err
		}
	}

	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Remove(file); err != nil {
		return err
	}
	
	log.Println("Recovery completed.")
	return nil
}
