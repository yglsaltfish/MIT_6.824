package main

import (
	"fmt"
)

func prin(i int){
    for j := 1; j < i ; j++{
        fmt.Print(j)
    }
}

func main() {
    
    for i:= 0 ; i < 10; i++{
        go prin(i)
    }
    fmt.Print("321")
}