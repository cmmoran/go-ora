package main

import (
	"database/sql"
	"fmt"
	"github.com/sijms/go-ora/dbms"
	_ "github.com/sijms/go-ora/v2"
	go_ora "github.com/sijms/go-ora/v2"
	"os"
	"sync"
	"time"
)

type test1 struct {
	Id   int64  `udt:"test_id"`
	Name string `udt:"test_name"`
	Data string `udt:"data"`
}

func createUDT(conn *sql.DB) error {
	t := time.Now()
	sqlText := `create or replace TYPE TEST_TYPE1 IS OBJECT 
(
    TEST_ID NUMBER(10, 0),
    TEST_NAME VARCHAR2(10),
	DATA      CLOB
)`
	_, err := conn.Exec(sqlText)
	if err != nil {
		return err
	}
	fmt.Println("Finish create UDT: ", time.Now().Sub(t))
	return nil
}

func dropUDT(conn *sql.DB) error {
	t := time.Now()
	_, err := conn.Exec("drop type TEST_TYPE1")
	if err != nil {
		return err
	}
	fmt.Println("Finish drop UDT: ", time.Now().Sub(t))
	return nil
}

func main() {
	fmt.Println("DBMS AQ example - single consumer")
	singleConsumer()
	fmt.Println("DBMS AQ example - multiple consumer")
	multipleConsumer()
}

func singleConsumer() {
	conn, err := sql.Open("oracle", os.Getenv("DSN"))
	if err != nil {
		fmt.Println("can't connect: ", err)
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println("can't close: ", err)
		}
	}()
	err = createUDT(conn)
	if err != nil {
		fmt.Println("can't create UDT: ", err)
		return
	}
	defer func() {
		err = dropUDT(conn)
		if err != nil {
			fmt.Println("can't drop UDT: ", err)
		}
	}()
	t := time.Now()
	err = go_ora.RegisterType(conn, "TEST_TYPE1", "", test1{})
	if err != nil {
		fmt.Println("can't register type: ", err)
		return
	}
	fmt.Println("Finish register type: ", time.Now().Sub(t))
	aq := dbms.NewAQ(conn, "GO_ORA_QU", "TEST_TYPE1")
	t = time.Now()
	err = aq.Create()
	if err != nil {
		fmt.Println("can't create queue: ", err)
		return
	}
	fmt.Println("Finish create queue: ", time.Now().Sub(t))
	defer func() {
		t = time.Now()
		err = aq.Drop()
		if err != nil {
			fmt.Println("can't drop queue: ", err)
		}
		fmt.Println("Finish drop queue: ", time.Now().Sub(t))
	}()
	t = time.Now()
	err = aq.Start(true, true)
	if err != nil {
		fmt.Println("can't enable queue: ", err)
		return
	}
	fmt.Println("Finish start queue: ", time.Now().Sub(t))
	defer func() {
		t = time.Now()
		err = aq.Stop(true, true)
		if err != nil {
			fmt.Println("can't stop queue: ", err)
		}
		fmt.Println("Finish stop queue: ", time.Now().Sub(t))
	}()
	t = time.Now()
	messageID, err := aq.Enqueue(test1{
		Id:   11,
		Name: "TEST",
		Data: "DATA",
	})
	if err != nil {
		fmt.Println("can't enqueue: ", err)
		return
	}
	fmt.Println("Finish  enqueue: ", time.Now().Sub(t))
	fmt.Println("enqueue message id: ", messageID)
	var test test1
	t = time.Now()
	messageID, err = aq.Dequeue(&test, 100)
	if err != nil {
		fmt.Println("can't dequeue: ", err)
		return
	}
	fmt.Println("Finish dequeue: ", time.Now().Sub(t))
	fmt.Println("dequeue message id: ", messageID)
	fmt.Println("message: ", test)
}

func multipleConsumer() {
	conn, err := sql.Open("oracle", os.Getenv("DSN"))
	if err != nil {
		fmt.Println("can't connect: ", err)
		return
	}
	defer func() {
		err = conn.Close()
		if err != nil {
			fmt.Println("can't close: ", err)
		}
	}()
	err = createUDT(conn)
	if err != nil {
		fmt.Println("can't create UDT: ", err)
		return
	}
	defer func() {
		err = dropUDT(conn)
		if err != nil {
			fmt.Println("can't drop UDT: ", err)
		}
	}()
	t := time.Now()
	err = go_ora.RegisterType(conn, "TEST_TYPE1", "", test1{})
	if err != nil {
		fmt.Println("can't register type: ", err)
		return
	}
	fmt.Println("Finish register type: ", time.Now().Sub(t))
	aq := dbms.NewAQ(conn, "GO_ORA_QU", "TEST_TYPE1", true)
	t = time.Now()
	err = aq.Create()
	if err != nil {
		fmt.Println("can't create queue: ", err)
		return
	}
	fmt.Println("Finish create queue: ", time.Now().Sub(t))
	defer func() {
		t = time.Now()
		err = aq.Drop()
		if err != nil {
			fmt.Println("can't drop queue: ", err)
		}
		fmt.Println("Finish drop queue: ", time.Now().Sub(t))
	}()
	t = time.Now()
	err = aq.Start(true, true)
	if err != nil {
		fmt.Println("can't enable queue: ", err)
		return
	}
	fmt.Println("Finish start queue: ", time.Now().Sub(t))
	defer func() {
		t = time.Now()
		err = aq.Stop(true, true)
		if err != nil {
			fmt.Println("can't stop queue: ", err)
		}
		fmt.Println("Finish stop queue: ", time.Now().Sub(t))
	}()
	t = time.Now()
	for i := 0; i < 10; i++ {
		if err = aq.Subscribe(fmt.Sprintf("SUB_%d", i)); err != nil {
			fmt.Println("can't subscribe: ", err)
			return
		}
	}
	go func() {
		time.Sleep(3 * time.Second)
		messageID, ierr := aq.Enqueue(test1{
			Id:   11,
			Name: "TEST",
			Data: "DATA",
		})
		if ierr != nil {
			fmt.Println("can't enqueue: ", ierr)
			return
		}
		fmt.Println("Finish  enqueue: ", time.Now().Sub(t))
		fmt.Println("enqueue message id: ", messageID)
	}()
	wg := &sync.WaitGroup{}
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(iwg *sync.WaitGroup, subscriberId int) {
			var test test1
			t = time.Now()
			fmt.Println("waiting for enqueued message...")
			messageID, ierr := aq.DequeueForSubscriber(fmt.Sprintf("SUB_%d", subscriberId), &test, 100)
			if ierr != nil {
				fmt.Println("can't dequeue: ", ierr)
				return
			}
			fmt.Println("Finish dequeue: ", time.Now().Sub(t))
			fmt.Println("dequeue message id: ", messageID)
			fmt.Println(fmt.Sprintf("SUB_%d message: ", subscriberId), test)
			iwg.Done()
		}(wg, i)
	}

	wg.Wait()
	fmt.Println("Finished all dequeue workers: ", time.Now().Sub(t))
}
