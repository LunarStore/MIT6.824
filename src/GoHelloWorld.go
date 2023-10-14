package main

import (
	"fmt"
	"log"
	"os"
	"time"
)

type MyClass struct {
	arr [10]int
	num int
	ptr *int
}

func (mc MyClass) Set(n int) bool {
	// mc.arr[3] = 9
	mc.num = 12
	// fmt.Printf("%v, %p, %v\n", mc.ptr, mc, mc.num)
	fmt.Printf("%v, %p, %v\n", mc.ptr, &mc, mc.num)
	return true
}

func TickerTest() {

	timer := time.NewTimer(0)
	// for {
	// 	select {
	// 	case <-timer.C:
	// 		fmt.Println("3秒执行任务")
	// 		timer.Reset(time.Second * 1)
	// 	}
	// }

	timer.Stop() // 这里来提高 timer 的回收
	fmt.Println("living")
	// timer.Reset(3 * time.Second)
	rt := timer.Reset(-1)
	select {
	case <-timer.C:
		fmt.Printf("3秒执行任务 rt = %v", rt)
	}
}

func MapTest() {
	mp := make(map[int]bool)
	if val, ok := mp[12]; ok {
		fmt.Printf("%v ----\n", val)
	}

	if val, ok := mp[12]; ok {
		fmt.Printf("%v ++++\n", val)
	}

	mp[12] = true
	if val, ok := mp[12]; ok {
		fmt.Printf("%v }}}}\n", val)
	}
	delete(mp, 12)

	fmt.Printf("%v ]]]]\n", len(mp))

	fmt.Printf("---%v\n", mp[123])
	fmt.Printf("%v ]]]]\n", len(mp))
}

func FileTest() {
	file, err := os.Create("testFile")
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	file.Write([]byte("123456"))
}

func SelectTest() {
	timer := time.NewTimer(0)
	n := 0
	for {
		fmt.Printf("loop...\n")
		select {
		case <-timer.C:
			if n != 2 {
				fmt.Printf("n = %v\n", n)
				n++
				timer.Reset(2 * time.Second)
				break
			} else {
				fmt.Printf("n = %v continue...", n)
				timer.Reset(1 * time.Second)
				continue
			}

			fmt.Printf("reach\n")
		}
	}
}

// 对称协程不存在父子关系
func ChannelTest() {
	ch := make(chan int, 1)

	go func() {
		fmt.Printf("---father....---\n")
		go func() {
			fmt.Printf("---son....---\n")
			time.Sleep(4 * time.Second)
			fmt.Printf("---son exit....---\n")
		}()

		go func() {
			fmt.Printf("receive wait\n")
			select {
			case a := <-ch:
				fmt.Printf("receive %v\n", a)
			}
		}()
		time.Sleep(2 * time.Second)

		ch <- 1
		// ch <- 2
		fmt.Printf("---father exit....---\n")
	}()
	a, err := <-ch
	if err == true {
		fmt.Printf("---%v---\n", a)
	} else {
		fmt.Printf("Fail!\n")
	}
	time.Sleep(3 * time.Second)
}

func TimerTest() {
	// timer := time.NewTimer(3 * time.Second)

	// go func() {
	// 	select {
	// 	case <-timer.C:
	// 		fmt.Printf("1wake!")
	// 	}
	// }()

	// go func() {
	// 	select {
	// 	case <-timer.C:
	// 		fmt.Printf("2wake!")
	// 	}
	// }()

	// <-timer.C
	// fmt.Printf("3wake!")

	tm := time.NewTimer(0)

	// time.Sleep(2 * time.Second)

	// tm.Stop()

	// tm.Reset(1 * time.Second)

	// time.Sleep(2 * time.Second)
	select {
	case <-tm.C:
		fmt.Printf("ok!\n")
	}
	<-tm.C
	// fmt.Printf("len(chan) == %v\n", len(tm.C))
	// <-tm.C
}

func ArrayTest() { //浅复制

	ar := []int{1, 2, 3}
	arCp := ar

	arCp[0] = 9
	fmt.Printf("<%p>:%v---<%p>:%v\n", &ar, ar[0], &arCp, arCp[0])
}

func SliceTest() {
	arr := []int{1, 2}
	fmt.Printf("%v, %v\n", arr[2:], nil)
}

func main() {
	var v int
	var ptr *MyClass
	mc := MyClass{}
	ptr = &mc
	mc.ptr = &v
	mc.Set(2)
	fmt.Printf("%v, %T, %p, %v\n", &v, mc, &mc, mc.num)
	ptr.Set(5)

	arr := [][]int{}
	arr1 := make([][]int, 10)
	fmt.Printf("%T, %T", arr, arr1)

	{
		defer fmt.Printf("defer OK\n")
	}

	time.Sleep(2 * time.Second)
	fmt.Printf("main exit\n")

	// TickerTest()
	// MapTest()
	// FileTest()
	// SelectTest()
	// ChannelTest()
	// TimerTest()
	// ArrayTest()
	// SliceTest()
}
