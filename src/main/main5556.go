package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
	"bufio"
	"bytes"
	"encoding/binary"
	"math"
	"container/list"
	"io/ioutil"
	"encoding/json"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
)
//配置文件
type Config struct {
	Port string
	MeterIds [][][]byte
}

type OutData struct {
	Data1 [112]byte
	Data2 [240]byte
	Data3 [16]byte
}

var sMap1 map[byte] []byte
var sMap2 map[byte] []byte
var sMap3 map[byte] []byte


/**
读取配置文件
 */
func ReadConfig()  Config{
	 //filePath := "C:\\work\\go3\\gostu\\src\\main\\config\\4conf.json"
	filePath := "/root/work/go/readwrite/gostu/src/main/config/5conf.json"
	file,err := ioutil.ReadFile(filePath)
	if err!=nil{
		fmt.Println("config file err:",err)
		os.Exit(1)
	}
	config := Config{}
	err2 := json.Unmarshal(file,&config)
	if err2!=nil{
		fmt.Println("json err:",err2)
		os.Exit(1)
	}
	return config
}

func insert(li *list.List)  {
	//sq_command := "insert into tbl_power_info_v2(P_A_DIANYA,P_B_DIANYA,P_C_DIANYA,P_UAB_XIANDIANYA,P_UBC_XIANDIANYA,P_UCA_XIANDIANYA,P_A_DIANLIU,P_B_DIANLIU,P_C_DIANLIU,P_A_YGGL,P_B_YGGL,P_C_YGGL,P_HXYGGL,P_A_WGGL,P_B_WGGL,P_C_WGGL,P_HXWGGL,P_A_SZGL,P_B_SZGL,P_C_SZGL,P_HXSZGL,P_A_GLYS,P_B_GLYS,P_C_GLYS,P_HXGLYS,P_DWPL,P_BY_KwhZ,P_BY_KwhJ,P_BY_KwhF,P_BY_KwhP,P_BY_KwhG,P_BY_HKwhZ,P_BY_HKwhJ,P_BY_HKwhF,P_BY_HKwhP,P_BY_HKwhG, P_BY_KvarhZ,P_BY_KvarhJ,P_BY_KvarhF,P_BY_KvarhP,P_BY_KvarhG, P_BY_HKvarhZ,P_BY_HKvarhJ,P_BY_HKvarhF,P_BY_HKvarhP,P_BY_HKvarhG, P_SY_KwhZ,P_SY_KwhJ,P_SY_KwhF,P_SY_KwhP,P_SY_KwhG, P_SY_HKwhZ,P_SY_HKwhJ,P_SY_HKwhF,P_SY_HKwhP,P_SY_HKwhG, P_SY_KvarhZ,P_SY_KvarhJ,P_SY_KvarhF,P_SY_KvarhP,P_SY_KvarhG, P_SY_HKvarhZ,P_SY_HKvarhJ,P_SY_HKvarhF,P_SY_HKvarhP,P_SY_HKvarhG, P_SSY_KwhZ,P_SSY_KwhJ,P_SSY_KwhF,P_SSY_KwhP,P_SSY_KwhG, P_SSY_HKwhZ,P_SSY_HKwhJ,P_SSY_HKwhF,P_SSY_HKwhP,P_SSY_HKwhG, P_SSY_KvarhZ,P_SSY_KvarhJ,P_SSY_KvarhF,P_SSY_KvarhP,P_SSY_KvarhG, P_SSY_HKvarhZ,P_SSY_HKvarhJ,P_SSY_HKvarhF,P_SSY_HKvarhP,P_SSY_HKvarhG,P_TIME,P_CODE,P_ZXYGDN,P_FXYGDN,P_ZXWGDN,P_FXWGDN)values(?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?,?, ?,?, ?,?, ?, ?,?, ?,?,?, ?,?, ?,?, ?, ?,?, ?,?,?, ?,?, ?,?, ?, ?,?, ?,?,?, ?,?, ?,?, ?, ?,?, ?,?,?, ?,?, ?,?, ?, ?,?, ?,?,?, ?,?,?,?, ?,?)"
	sqlstring := "insert into tbl_power_info_v2(P_A_DIANYA,P_B_DIANYA,P_C_DIANYA,P_UAB_XIANDIANYA,P_UBC_XIANDIANYA,P_UCA_XIANDIANYA,P_A_DIANLIU,P_B_DIANLIU,P_C_DIANLIU,P_A_YGGL,P_B_YGGL,P_C_YGGL,P_HXYGGL,P_A_WGGL,P_B_WGGL,P_C_WGGL,P_HXWGGL,P_A_SZGL,P_B_SZGL,P_C_SZGL,P_HXSZGL,P_A_GLYS,P_B_GLYS,P_C_GLYS,P_HXGLYS,P_DWPL,P_BY_KwhZ,P_BY_KwhJ,P_BY_KwhF,P_BY_KwhP,P_BY_KwhG,P_BY_HKwhZ,P_BY_HKwhJ,P_BY_HKwhF,P_BY_HKwhP,P_BY_HKwhG, P_BY_KvarhZ,P_BY_KvarhJ,P_BY_KvarhF,P_BY_KvarhP,P_BY_KvarhG, P_BY_HKvarhZ,P_BY_HKvarhJ,P_BY_HKvarhF,P_BY_HKvarhP,P_BY_HKvarhG, P_SY_KwhZ,P_SY_KwhJ,P_SY_KwhF,P_SY_KwhP,P_SY_KwhG, P_SY_HKwhZ,P_SY_HKwhJ,P_SY_HKwhF,P_SY_HKwhP,P_SY_HKwhG, P_SY_KvarhZ,P_SY_KvarhJ,P_SY_KvarhF,P_SY_KvarhP,P_SY_KvarhG, P_SY_HKvarhZ,P_SY_HKvarhJ,P_SY_HKvarhF,P_SY_HKvarhP,P_SY_HKvarhG, P_SSY_KwhZ,P_SSY_KwhJ,P_SSY_KwhF,P_SSY_KwhP,P_SSY_KwhG, P_SSY_HKwhZ,P_SSY_HKwhJ,P_SSY_HKwhF,P_SSY_HKwhP,P_SSY_HKwhG, P_SSY_KvarhZ,P_SSY_KvarhJ,P_SSY_KvarhF,P_SSY_KvarhP,P_SSY_KvarhG, P_SSY_HKvarhZ,P_SSY_HKvarhJ,P_SSY_HKvarhF,P_SSY_HKvarhP,P_SSY_HKvarhG,P_CODE,P_TIME,P_ZXYGDN,P_FXYGDN,P_ZXWGDN,P_FXWGDN)"
	//sqlstring += fmt.Sprintf("values(%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s, %s,%s,%s, %s,%s,%s,%s, %s,%s)",li)
	sqlstring += "values("
	for e :=li.Front();e!=nil;e=e.Next(){
		sqlstring += fmt.Sprint(e.Value) +","
	}
	sqlstring = sqlstring[:len(sqlstring)-1] + ")"
	fmt.Println(sqlstring)

	// sqlstring += fmt.Sprintf("values(%v)",li)
	db, e:= sql.Open("mysql", "root:Aa651830@tcp(120.27.227.95:3306)/huzhou?charset=utf8")
	db.SetMaxIdleConns(1000)
	defer db.Close()
	if e!=nil{
		fmt.Print(e)
	}
	//err := db.Ping()
	//if err != nil {
	//	panic(err.Error()) // proper error handling instead of panic in your app
	//}

	row,err2:=db.Query(sqlstring)
	defer row.Close()
	if err2!=nil{
		fmt.Println(err2)
	}
	err3 := row.Err()
	if err3!=nil{
		fmt.Printf(err3.Error())
	}
	fmt.Print("写入成功")
}

var host = flag.String("host","","host")
var port = flag.String("port","6100","port")

func main()  {

	sMap1 = make(map[byte] []byte)
	sMap2 = make(map[byte] []byte)
	sMap3 = make(map[byte] []byte)

	port := ReadConfig().Port
	var l net.Listener
	var err error
	l,err = net.Listen("tcp",""+":"+port)
	if err !=nil{
		fmt.Println("Errpr listening:",err)
		os.Exit(1)
	}
	defer l.Close()
	fmt.Println("Listening on "+":"+port)

	for{
		conn,err := l.Accept()
		if err !=nil{
			fmt.Println("Error accepting:",err)
			os.Exit(1)
		}
		fmt.Println("received messag ?->? \n",conn.RemoteAddr(),conn.LocalAddr())

		go handleRead(conn)
		go handleWrite(conn)
		//go handleRequest(conn)
	}
}

//整理数据，将数据放入数据库
func HandData(num byte)  bool{
	value1,ok1 :=sMap1[num]
	value2,ok2 :=sMap2[num]
	value3,ok3 :=sMap3[num]
	if ok1&&ok2&&ok3{
		l1:=parseBytes(value1)
		l2:=parseBytes(value2)
		l1.PushBackList(l2)
		l1.PushBack(num)
		l1.PushBack(time.Now().Format("20060102150405"))
		l3:=parseBytes2int(value3)
		l1.PushBackList(l3)
		for e := l1.Front(); e != nil; e = e.Next() {
			fmt.Print(e.Value) //输出list的值,01234
		}
		//写入数据库
		insert(l1)
		defer func() {
			if x := recover();x!=nil{
				fmt.Println("inset,err,flag")
				return
			}
		}()
		//fmt.Print(l1)
		//删除键值
		delete(sMap1,num)
		delete(sMap2,num)
		delete(sMap3,num)
		fmt.Println("读取完毕一个")
		return true
	}
	return false
}


func handleWrite(conn net.Conn)  {
	defer conn.Close()
	for{
		meterOrders := ReadConfig().MeterIds
		for i:=0;i<len(meterOrders);i++{
			for j:=0;j<len(meterOrders[i]);j++ {
				fmt.Printf("write:% X\n",meterOrders[i][j])
				_, e := conn.Write(meterOrders[i][j])
				if e != nil {
					fmt.Println("write err:", e)
					return;
				}
				time.Sleep(20*time.Second)
			}
		}
		time.Sleep(5*time.Second)
	}
}

func handleRead(conn net.Conn)  {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		peekbytes,err := reader.Peek(3)
		fmt.Printf("% X\n",peekbytes)
		if err!=nil{
			fmt.Println("err2222:",err)
			break
		}

		if bytes.Contains(peekbytes,[]byte{0x03,0x68}){
			//读取 字节
			dianbiaoNum,_ := reader.ReadByte()
			//丢弃5个字节
			fmt.Print(dianbiaoNum)
			reader.Discard(4)
			recvbytes := make([]byte,0x68)
			reader.Read(recvbytes)
			fmt.Printf("% X\n",recvbytes)
			sMap1[dianbiaoNum] = recvbytes
			continue
		}
		if bytes.Contains(peekbytes,[]byte{0x03,240}){

			//读取 字节
			dianbiaoNum,_ := reader.ReadByte()
			//丢弃5个字节
			fmt.Print(dianbiaoNum)
			reader.Discard(4)
			recvbytes := make([]byte,0,240)
			tmp := make([]byte,240)
			total :=0
			for{
				n,err := reader.Read(tmp)
				if err!=nil{
					break
				}
				recvbytes = append(recvbytes,tmp[:n]...)
				total += n;
				if total>=240{
					break
				}
			}
			fmt.Println("totalsize",len(recvbytes))
			fmt.Printf("% X\n",recvbytes)
			sMap2[dianbiaoNum] = recvbytes
			continue
		}
		if bytes.Contains(peekbytes,[]byte{0x03,16}){
			//读取 字节
			dianbiaoNum,_ := reader.ReadByte()
			//丢弃5个字节
			fmt.Print(dianbiaoNum)
			reader.Discard(4)
			recvbytes := make([]byte,16)
			reader.Read(recvbytes)
			fmt.Printf("% X\n",recvbytes)
			sMap3[dianbiaoNum] = recvbytes
			//处理，将数据送入数据库
			if HandData(dianbiaoNum){
			}
			continue
		}

		//将字节取出不做处理
		recvbytes :=make([]byte,reader.Buffered())
		reader.Read(recvbytes)
		fmt.Printf("heart:% X\n",recvbytes)
		time.Sleep(1*time.Second)
	}
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.BigEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func parseBytes2int(bytes []byte) *list.List{
	l := list.New()
	n:= len(bytes)
	for i:=0;i<n;i+=4{
		curBytes := bytes[i:i+4]
		fmt.Printf("% X\n",curBytes)
		bits:=binary.BigEndian.Uint32(curBytes)
		fmt.Println(bits)
		l.PushBack(float32(bits)/1000.0)
	}
	return l
}


func parseBytes(bytes []byte)  *list.List{
	l := list.New()
	n := len(bytes)
	for i:=0;i<n;i+=4{
		curBytes := bytes[i:i+4]
		fmt.Printf("% X\n",curBytes)
		curFolat := ByteToFloat32(curBytes)
		fmt.Println(curFolat)
		l.PushBack(curFolat)
	}
	return l
}

