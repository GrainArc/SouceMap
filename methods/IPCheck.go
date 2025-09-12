package methods

import (
	"fmt"
	"net"
	"sync"
	"time"
)

func APICheck(ip string) bool {
	address := fmt.Sprintf("%s:%s", ip, "8181")
	conn, err := net.DialTimeout("tcp", address, 1*time.Second)
	if err != nil {
		return false
	}
	defer conn.Close()
	return true
}

func ping(ip string, wg *sync.WaitGroup, results chan<- string) {
	defer wg.Done()

	aa := APICheck(ip)
	if aa == true {
		results <- ip
	}

}

func GetIP(net string) []string {

	var wg sync.WaitGroup
	results := make(chan string)
	// 假设局域网的网段为 192.168.1.0/24
	for i := 1; i < 255; i++ {
		ip := fmt.Sprintf("%s.%d", net, i)
		wg.Add(1)
		go ping(ip, &wg, results)
	}

	go func() {
		wg.Wait()
		close(results)
	}()
	var aa []string
	for ip := range results {
		aa = append(aa, ip)

	}
	return aa
}
