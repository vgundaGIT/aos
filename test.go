package main

import "fmt"

type hm struct {
	names []string
}

func main() {
	fmt.Println("Hello World")
	hm_var1 := hm{}
	hm_var1.names = append(hm_var1.names, "Madonna")
	fmt.Println(hm_var1.names)
}
