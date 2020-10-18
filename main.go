package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/************************************************ CLIENT ********************************************************/

type Client struct {
	serverIP       string
	serverPort     string
	readBufferSize int64 //decides how much can the client read at once
	conn           net.Conn
	isConnected    bool
	buf            *bufio.Reader
}

func (c *Client) Connect() error {
	//fmt.Println("Connecting to destination" + c.serverIP)
	var err error
	c.conn, err = net.Dial("tcp", doGetSocketAddress(c.serverIP, c.serverPort))
	//Decide what to do at this stage of no connection
	//Catch it in main function and try for more attempts
	if err != nil {
		loggerPrint(err.Error())
		return err
	}
	loggerPrint("Connected to " + doGetSocketAddress(c.serverIP, c.serverPort))
	c.isConnected = true
	c.buf = bufio.NewReader(c.conn)
	return nil
}

func (c Client) Disconnect() {
	//Just close the connection
	//Closing connection to the destination
	if c.isConnected {
		c.conn.Close()
		fmt.Println("Closing the connection")
		c.isConnected = false
	}
}

/*Check: I think uploading is not required */
func (c Client) Upload(data []byte) {
	//fmt.Println("Sending Data to destination")
	//Send data to servers

}

func (c Client) Download() []byte {
	//Downloads stream of bytes
	//that are unmarshalled later
	//fmt.Println("Downloading Data from destination")
	return []byte{}
}

/*******************************************************************************************************************/

/************************************************ DHTClient ********************************************************/

type DHTClient struct {
	Client
}

//reqType is "FileList"
type DHTFilesMessage struct {
	NodeAddress
	ReqType string `json:"Type"`
	Files   []string
}

//reqType is "Location"
type FileLocationReq struct {
	ReqType string `json:"Type"`
	File    string `json:"FileName"`
}

//type NodeAddress struct {
type NodeAddress struct {
	NodeIP   string
	NodePort string
}

func (d DHTClient) doGetFileList() []string {
	//call download function from here
	d.Download()
	return []string{"a", "b"}
}

func (d DHTClient) updateListOfFiles(s *Server) error {
	var fileList []string

	s.listOfFiles.mx.Lock()
	for i := 0; i < len(s.listOfFiles.files); i++ {
		fileList = append(fileList, s.listOfFiles.files[i])
	}
	s.listOfFiles.mx.Unlock()

	debugger("List of files served are", fileList)

	dhtFilesMessage := DHTFilesMessage{ReqType: "ServedFiles", Files: fileList, NodeAddress: NodeAddress{NodeIP: s.ip, NodePort: s.port}}

	byteArray, err := json.Marshal(dhtFilesMessage)

	byteArray = append(byteArray, '\n')

	if err != nil {
		loggerPrint("Error marshalling files list for DHT")
		return err
	}

	d.conn.Write(byteArray)

	return nil
}

func (d DHTClient) updateDHT(s *Server) {
	//Messages DHT every 10 minutes regarding the list of files
	for {
		d.updateListOfFiles(s)
		time.Sleep(10 * time.Minute)
	}
}

//String is the ip address and error can be nil or anything else
func (d DHTClient) doGetFileLocation(fileName string) (NodeAddress, error) {

	//Marshalling here because there are no endpoints
	fileLocReq := FileLocationReq{ReqType: "FileLocation", File: fileName}

	//Create json marshall of this request

	byteArray, err := json.Marshal(fileLocReq)

	if err != nil {
		loggerPrint("Error marshalling file location request")
		return NodeAddress{}, err
	}

	byteArray = append(byteArray, '\n')

	//send this byte array to DHT node and receive the response
	d.conn.Write(byteArray)

	//receive the response
	//buf, err := ioutil.ReadAll(d.conn)

	//buf := bufio.NewReader(d.conn)

	msg, err := d.buf.ReadString('\n')
	msg = strings.TrimSuffix(msg, "\n")

	if err != nil {
		loggerPrint("Error in getting the file location from DHT Node")
		return NodeAddress{}, err
	}

	//response is again a struct that has to be unmarshalled
	var resp NodeAddress

	loggerPrint("Received node address is " + msg)

	err = json.Unmarshal([]byte(msg), &resp)

	if err != nil {
		loggerPrint("Error in unmarshaling DHT server response")
		return NodeAddress{}, err
	}

	return resp, nil
}

/*********************************************** GeneralClient*******************************************************/

type GeneralClient struct {
	directory string //directory where it holds the files
	Client
}

//May not be useful at all!!
func (g GeneralClient) doGetFileList() []string {

	return []string{"a", "b"}
}

type FileDownloadRequest struct {
	ReqType  string
	FileName string
}

type FileProperties struct {
	Size   int64
	Md5sum string
}

//Send a request to DHTServer to get file location
//Then download it
func (g GeneralClient) downloadFile(fileName string, d DHTClient) error {

	//Mechanism to download file

	loggerPrint("started downloading the file" + fileName)

	//Use DHT client to get the file location
	serverAddress, err := d.doGetFileLocation(fileName)

	loggerPrint("server address is" + serverAddress.NodeIP + ":" + serverAddress.NodePort)

	if err != nil {
		loggerPrint("Could not find the file in Hash Table " + err.Error())
		return err
	}

	//Then connect to the correct server using the address above
	g.serverIP = serverAddress.NodeIP
	g.serverPort = serverAddress.NodePort

	err = g.Connect()

	if err != nil {
		loggerPrint("Unable to connect to file server" + err.Error())
		return err
	}

	//Send request (marshall) to download a file with given
	req := FileDownloadRequest{ReqType: "Download", FileName: fileName}

	byteArray, err := json.Marshal(req)

	byteArray = append(byteArray, '\n')

	//Send this byteArray to the server
	g.conn.Write(byteArray)

	//Get the file size from the server
	var fileSizeResp FileProperties

	msg, err := g.buf.ReadString('\n')

	if err != nil {
		loggerPrint("Error in getting the file size from server " + err.Error())
		return err
	}

	msg = strings.TrimSuffix(msg, "\n")
	debugger("Obtained file properties", msg)

	err = json.Unmarshal([]byte(msg), &fileSizeResp)
	fileSize := fileSizeResp.Size
	md5sum := fileSizeResp.Md5sum

	if err != nil {
		loggerPrint("Error in unmarshaling file size response from server" + err.Error())
		return err
	}

	newFile, err := os.Create(g.directory + "/" + fileName)

	if err != nil {
		loggerPrint("Error while creating file in the client directory" + err.Error())
		return err
	}

	loggerPrint("Starting downloading files")

	debugger("buffer file size is ", g.readBufferSize)

	var receivedBytes int64

	for {
		if (fileSize - receivedBytes) < g.readBufferSize {
			io.CopyN(newFile, g.conn, (fileSize - receivedBytes))
			//fmt.Println("Finally reading num bytes : %v", (receivedBytes+g.readBufferSize)-fileSize)
			//g.conn.Read(make([]byte, (receivedBytes+g.readBufferSize)-fileSize))
			break
		}
		io.CopyN(newFile, g.conn, g.readBufferSize)
		receivedBytes += g.readBufferSize
	}

	debugger("receivedBytes so far", receivedBytes)

	loggerPrint("Completed downloading the file")

	//Recalculate md5sum

	//Check if md5sum is valid
	downloadedFile, err := os.Open(g.directory + "/" + fileName)
	defer downloadedFile.Close()

	newMd5Sum := md5Calculator(downloadedFile)

	//Should I download again in this case?
	if newMd5Sum != md5sum {
		loggerPrint("Downloaded file is corrupted")
	}

	return nil
}

/****************************************************************************************************************/

/************************************************ SERVER ********************************************************/

type FileList struct {
	files []string
	mx    sync.Mutex
}

// type ServerRequest struct {
// 	Type string `json:"reqType"`
// 	Data string `json:"data"` // Data contains file name that has to be downloaded
// }

type Server struct {
	ip                 string
	port               string
	listOfFiles        FileList
	directory          string
	ln                 net.Listener
	transferBufferSize int
}

func (s *Server) startServer() error {
	var err error
	s.ln, err = net.Listen("tcp", ":"+s.port)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) populateListOfFiles() error {

	//debugger("directory is ", s.directory)
	//Read list of files from server directory
	var files []string
	err := filepath.Walk(s.directory, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		idx := strings.IndexByte(path, '/')
		path = path[idx+1:]
		files = append(files, path)
		return nil
	})

	debugger("files hosted by this server are ", files)

	if err != nil {
		loggerPrint("Error getting files in directory")
		return nil
	}

	//Lock the FileList mutex
	s.listOfFiles.mx.Lock()
	s.listOfFiles.files = s.listOfFiles.files[:0]
	//Write to the FileList
	for i := 0; i < len(files); i++ {
		s.listOfFiles.files = append(s.listOfFiles.files, files[i])
	}
	//Unlock FileList
	s.listOfFiles.mx.Unlock()

	return nil
}

//This should run in a separate thread every 60 seconds
func (s *Server) messageDHT() error {

	//Send list of files to DHT node

	//Use mutex lock when reading all the files

	//Create marshalled

	return nil
}

func (s *Server) receiveRequests() error {
	loggerPrint("Started receiving requests")
	for {
		conn, err := s.ln.Accept()

		if err != nil {
			return err
		}
		loggerPrint("Accepted connection from " + conn.RemoteAddr().String())
		//Start a go routine that receives incoming requests
		go s.serveRequest(conn)
	}
}

func (s *Server) serveRequest(conn net.Conn) {
	//unmarshall the request message
	//and send the output based on that
	var req FileDownloadRequest

	//TO DO: Write marshalled file list to the client => NOT REQUIRED

	//Read the marshalled file name from client
	//buf, err := ioutil.ReadAll(conn)

	buf := bufio.NewReader(conn)

	msg, err := buf.ReadString('\n')
	msg = strings.TrimSuffix(msg, "\n")

	if err != nil {
		loggerPrint("Reading content from the request failed")
		//May be send a failure message to the client
		return
	}
	err = json.Unmarshal([]byte(msg), &req)
	if err != nil {
		loggerPrint("Unmarshalling request message error at server" + err.Error())
		return
	}
	//Send the response on the type of request
	//There is only one type of request : DownloadFile

	switch req.ReqType {
	case "Download":
		// Read the file from the folder
		// Send it to the connection
		var file *os.File

		debugger("File requested is ", s.directory+"/"+req.FileName)

		//Filename is stored in req.Data
		file, err = os.Open(s.directory + "/" + req.FileName)
		if err != nil {
			loggerPrint("Server unable to open the file")
			return
		}
		defer file.Close()

		//Get the filename and filesize
		fileInfo, err := file.Stat()
		if err != nil {
			loggerPrint("Error in reading file statistics")
			return
		}
		//send file size
		//fileSize := fillString(strconv.FormatInt(fileInfo.Size(), 10), 10)

		var fileProp FileProperties

		fileProp.Md5sum = md5Calculator(file)
		fileProp.Size = fileInfo.Size()

		//Marshal this and send it

		byteArray, err := json.Marshal(fileProp)

		byteArray = append(byteArray, '\n')

		//Send this byteArray to the server
		conn.Write(byteArray)

		sendBuffer := make([]byte, s.transferBufferSize)

		loggerPrint("\nSending file " + fileInfo.Name() + " to " + conn.RemoteAddr().String())

		//Start sending the file to the client
		for {
			_, err = file.Read(sendBuffer)
			if err == io.EOF {
				break
			}
			conn.Write(sendBuffer)
		}
		loggerPrint("Completed Sending file " + fileInfo.Name() + " to " + conn.RemoteAddr().String())
	}

}

func (s *Server) stopServer() error {
	err := s.ln.Close()
	if err != nil {
		loggerPrint("Unable to close the server")
		return err
	}

	//TO DO: Update DHT regarding this nodes unavailability

	return nil
}

/****************************************** Utils *********************************************************/

func loggerPrint(s string) {
	//fmt.Println(s)
	log.Println(s)
}

func doGetSocketAddress(ip string, port string) string {
	return ip + ":" + port
}

func fillString(retunString string, toLength int) string {
	for {
		lengtString := len(retunString)
		if lengtString < toLength {
			retunString = retunString + ":"
			continue
		}
		break
	}
	return retunString
}

func md5Calculator(file *os.File) string {
	h := md5.New()
	if _, err := io.Copy(h, file); err != nil {
		log.Fatal(err)
	}
	file.Seek(0, 0)
	return hex.EncodeToString(h.Sum(nil))
}

func debugger(desc string, content interface{}) {
	fmt.Printf(desc+" %v ", content)
}

/****************************************** Main ***********************************************************/

func main() {
	// fmt.Println("Hello World")
	// c := Client{
	// 	serverIP:   "127.0.0.1",
	// 	serverPort: "8080",
	// }

	// c.Connect()
	// c.Disconnect()
	// c.Upload([]byte{1, 2, 3})

	// d := DHTClient{Client: Client{serverIP: "localhost", serverPort: "27001"}}

	// d.Connect()
	// //d.doGetFileList()
	// loc, err := d.doGetFileLocation("testfile")

	// if err != nil {
	// 	fmt.Println("Error getting file location" + err.Error())
	// }

	// fmt.Println(loc)

	//Create general client and request files

	//Create dht client

	//Create server and run it in the background

	//No compilation issues :)

	//0 .Read server directory from command line and the file to download(useful for testing)
	serverPort := flag.String("port", "8080", "server's port")
	nodeDir := flag.String("folder", "NA", "server's directory")
	fileToDownload := flag.String("download", "NA", "file to download")

	flag.Parse()

	//1. Create DHT Client => lets make this first!!

	dhtClient := DHTClient{Client: Client{serverIP: "localhost", serverPort: "27001"}}

	dhtClient.Connect()

	//1. Start server
	server := Server{ip: "localhost", port: *serverPort, directory: *nodeDir, transferBufferSize: 1024}

	err := server.populateListOfFiles()

	if err != nil {
		loggerPrint("Error populating list of files in server directory " + err.Error())
		return
	}

	go dhtClient.updateDHT(&server)

	err = server.startServer()

	if err != nil {
		loggerPrint("Error while starting the server: " + err.Error())
		return
	}

	go server.receiveRequests()

	//2a. Create General client
	client := GeneralClient{directory: *nodeDir, Client: Client{readBufferSize: 1024}}

	if *fileToDownload != "NA" {

		//3. Download the file obtained from the terminal
		client.downloadFile(*fileToDownload, dhtClient)

	}

	time.Sleep(2 * time.Minute)

}
