package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

/********************************************  DHT SERVER *******************************************************/

type PeerInfo struct {
	activePeers  map[string]bool
	fileHostsDir map[string][]string
	mx           sync.Mutex
}

type dserver struct {
	ln          net.Listener
	PeerDetails PeerInfo
}

type Request struct {
	Type string
}

type Address struct {
	NodeIP   string
	NodePort string
}

type FileList struct {
	Files []string
}

type File struct {
	FileName string
}

func (ds *dserver) receiveRequests() error {
	loggerPrint("started receiving requests")
	for {
		conn, err := ds.ln.Accept()

		if err != nil {
			return err
		}
		loggerPrint("Accepted connection from " + conn.RemoteAddr().String())
		//Start a go routine that receives incoming requests
		go ds.serveRequest(conn)
	}
}

func (ds *dserver) serveRequest(conn net.Conn) {
	//Read until "\n" from client
	//Then unmarshall it to Request
	var req Request

	buf := bufio.NewReader(conn)

	msg, err := buf.ReadString('\n')
	msg = strings.TrimSuffix(msg, "\n")
	fmt.Println(msg)
	if err != nil {
		loggerPrint("Reading request failed from DHT client" + err.Error())
		//May be send a failure message to the client
		return
	}
	err = json.Unmarshal([]byte(msg), &req)

	if err != nil {
		loggerPrint("Unmarshalling request message error at server" + err.Error())
		return
	}

	loggerPrint("request is ")
	fmt.Println(req)

	switch req.Type {
	case "Deactivate":
		var newReq struct {
			Request
			Address
		}
		if err = json.Unmarshal([]byte(msg), &newReq); err != nil {
			loggerPrint("Unmarshalling failed")
			return
		}
		ds.PeerDetails.mx.Lock()
		delete(ds.PeerDetails.fileHostsDir, newReq.NodeIP+":"+newReq.NodePort)
		ds.PeerDetails.mx.Unlock()

	case "ServedFiles":
		//Make changes to dserver.fileMap
		var newReq struct {
			Request
			Address
			FileList
		}
		if err = json.Unmarshal([]byte(msg), &newReq); err != nil {
			loggerPrint("Unmarshalling failed to get served files")
			return
		}

		ds.PeerDetails.mx.Lock()
		clientAddr := newReq.NodeIP + ":" + newReq.NodePort
		_, present := ds.PeerDetails.fileHostsDir[clientAddr]
		if present {
			delete(ds.PeerDetails.fileHostsDir, clientAddr)
		} else {
			ds.PeerDetails.activePeers[clientAddr] = true
		}

		ds.PeerDetails.fileHostsDir[clientAddr] = newReq.Files

		ds.PeerDetails.mx.Unlock()

		debugger("set of files are  ", ds.PeerDetails.fileHostsDir)

	case "FileLocation":
		var requiredNode Address
		//Get the location of the node that the current file is in
		var newReq struct {
			Request
			File
		}
		if err = json.Unmarshal([]byte(msg), &newReq); err != nil {
			loggerPrint("Unmarshalling failed to get served files")
			return
		}
		for key, files := range ds.PeerDetails.fileHostsDir {
			for i := 0; i < len(files); i++ {
				if files[i] == newReq.FileName {
					if idx := strings.IndexByte(key, ':'); idx >= 0 {
						requiredNode.NodeIP = key[:idx]
						requiredNode.NodePort = key[idx+1:]
					} else {
						loggerPrint("Invalid node location stored here")
					}
				}
			}
		}

		//Marshall requiredNode and send it back
		byteArray, err := json.Marshal(requiredNode)

		byteArray = append(byteArray, '\n')

		if err != nil {
			loggerPrint("Error marshalling node location for given file")
			return
		}

		conn.Write(byteArray)
		return
	default:
		loggerPrint("Unknown Request")
	}
}

func main() {

	//Create a server
	server, err := net.Listen("tcp", "localhost:27001")
	if err != nil {
		loggerPrint("Unable to start DHT Server" + err.Error())
		return
	}

	hashTableServer := dserver{ln: server}
	hashTableServer.PeerDetails.fileHostsDir = make(map[string][]string)
	hashTableServer.PeerDetails.activePeers = make(map[string]bool)

	//Start receiving requests
	hashTableServer.receiveRequests()

}

/****************************************** Utils *********************************************************/

func loggerPrint(s string) {
	fmt.Println(s)
	log.Println(s)
}

func debugger(desc string, content interface{}) {
	fmt.Printf(desc+" %v ", content)
}
