#!/bin/bash

echo "---------Distributed Raft Based RPC Server In Go----------"
echo ""

go run main.go -name=a -port=8000
go run main.go -name=b -port=8080
go run main.go -name=c -port=8089
go run main.go -name=d -port=8090

