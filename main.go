package main

import (
	"fmt"
	"flag"
	//"os"
	"time"
	"math/rand"
	"net/rpc"
	"net"
)

type Node struct {
	Name string
	Type string
	Port string
	Votes int
}

var nodes = [4]string{"localhost:8000", "localhost:8080", "localhost:8089", "localhost:8090"}

type API int

var MasterRes chan string
//var CandidateReq chan string
//var VoterRes chan bool 


func (a *API) TestCase(msg string, resp *string) error {
	//MasterRes <- msg
	//*resp = <-CandidateReq
	//fmt.Println("asjdkfbasv[pj ", msg)
	return nil
}


func main() {
	
	var nodeName= flag.String("name", "", "Name of Node")
        var nodePort = flag.String("port", "", "Which port number?")

        flag.Parse()
	
        node := Node{*nodeName, "Stateless", *nodePort, 0}

	fmt.Println("Node Name: ", node.Name, "Node State: ", node.Type, "Node Port: ", node.Port)

	//Here I seed and get a random timeout, this is for the initial election
	rand.Seed(time.Now().UTC().UnixNano())

	//The paper states that the election timout should be between 150-300ms, however 
	//in this case I'm just scaling by 10 to visualize	
	//-----electionTimeout := time.Millisecond * time.Duration(rand.Intn(3000 - 1500) + 1500)
	//-----fmt.Println("Election Timeout: ", electionTimeout)	

	//remember in Raft for stability broadcastTime << electionTimeout << MTBF

	//-----heartbeatTimeout := time.Millisecond * time.Duration(1000)
	//-----voterTimeout := time.Millisecond * time.Duration(1000)

	//channels to account for inter-node variables
	//MasterRes := make(chan string)
	//CandidateReq := make(chan string)
	//-----VoterRes := make(chan bool)
	
	//registering RPC's
        var api = new(API)
        err := rpc.Register(api)

        if err != nil {
                fmt.Println("Error Registering API", err)
        } 	

	//let's start listening
	hostVal := "localhost:" + node.Port

	listener, err := net.Listen("tcp", hostVal)
	if err != nil {
		fmt.Println()
	}
	
	//fmt.Println("LLOL", nodes)

	//go rpc.Accept(listener)
	
	//enter routine 
	for {
		go rpc.Accept(listener)
		
		//we iterate through all nodes including our own, remember we want to vote for our own
		for _, nodeCurr := range nodes {
			go func(nodeCurr string){
				nodeExt, err := rpc.Dial("tcp", nodeCurr)
				if err != nil {
					fmt.Println("Node: ", node.Name, "Port: ", node.Port, "couldn't Connect to ", nodeCurr)
				} else {
					fmt.Println("Node: ", node.Name, "Port: ", "Leader Msg: ", nodeExt, node.Port, "Connected to: ", nodeCurr)
				}
				/*	
				req := new(string)
				res := new(string)
				err = nodeExt.Call("A.TestCase", req, &res)
					if err != nil {
						fmt.Println("Can't call")
					}
				MasterRes <- *res
				*/
			}(nodeCurr)
			
		}
	
		break
		 	
	}	
	

}
