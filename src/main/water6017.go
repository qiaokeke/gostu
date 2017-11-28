package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
	"bytes"
	"io/ioutil"
	"encoding/json"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
)
//配置文件
type Config struct {
	Port string
	MeterIds [][][]byte
}

/**
读取配置文件
 */
func ReadConfig()  Config{
	//filePath := "C:\\work\\go\\gostu\\src\\main\\config\\conf6011.json"
	filePath := "/root/work/go/readwrite/gostu/src/main/config/conf6017.json"
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

/**
插入到数据库
 */
func insert(W_ADDRESS string,W_READINGS string,W_TIME string)  {
	sqlstring := "insert into tbl_water_info(W_ADDRESS,W_READINGS,W_TIME)"
	sqlstring += "values("+W_ADDRESS+","+W_READINGS+","+W_TIME+")"
	fmt.Println(sqlstring)

	db, e:= sql.Open("mysql", "root:Aa651830@tcp(120.27.227.95:3306)/huzhou?charset=utf8")
	db.SetMaxIdleConns(1000)
	defer db.Close()
	if e!=nil{
		fmt.Print(e)
		return
	}

	row,err2:=db.Query(sqlstring)
	defer row.Close()
	if err2!=nil{
		fmt.Println(err2)
	}
	fmt.Print("写入成功")
}

//byte转16进制字符串
func ByteToHex(data []byte) string {
	buffer := new(bytes.Buffer)
	for _, b := range data {
		s := strconv.FormatInt(int64(b&0xff), 16)
		if len(s) == 1 {
			buffer.WriteString("0")
		}
		buffer.WriteString(s)
	}
	return buffer.String()
}
/**
解析水表数据
 */
func ParseWaterData(bytes []byte)  float64{
	//－33H
	r :=0.0
	for j:=0;j<4;j++ {
		hexs := strconv.FormatInt(int64((bytes[j]-0x33)&0xff),16)
		//fmt.Println(hexs)
		i, err := strconv.Atoi(hexs)
		if err != nil {
			fmt.Println(err)
			return 0
		}
		k := 0.0
		if j==0{
			k=0.01
		}
		if j==1{
			k=1
		}
		if j==2{
			k=100
		}
		if j==3{
			k=10000
		}
		r += k * float64(i)
	}
	fmt.Println(r)
	return r
}

/**
解析水表编号
 */
func ParseWaterId(bytes []byte)  string{
	str := ""
	for i:=5;i>=0;i--{
		hexs := strconv.FormatInt(int64((bytes[i])&0xff),16)
		if len(hexs)==1{
			str+="0"
		}
		str += hexs
	}
	fmt.Println(str)
	return str
}

var host = flag.String("host","","host")
var port = flag.String("port","6100","port")

func main()  {
	ParseWaterData([]byte{0x33,0x3A,0x34,0x33})
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
	}
}

//整理数据，将数据放入数据库
func HandData(bytes []byte) {
	recvBytes:=bytes[0:18]
	data := ParseWaterData(recvBytes[12:16])
	id := ParseWaterId(recvBytes[1:7])
	insert(id,fmt.Sprintf("%.2f",data),time.Now().Format("20060102150405"))
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
				time.Sleep(30*time.Second)
			}
		}
		time.Sleep(5*time.Second)
	}
}

func handleRead(conn net.Conn)  {
	defer conn.Close()
	for{
			recvBytes := make([]byte,0,100)
			tmp := make([]byte,100)
			n,err := conn.Read(tmp)
			if err!=nil{
				fmt.Println(err)
				break
			}
			if n<=0{
				continue
			}
			fmt.Printf("% X\n",tmp[0:n])

			if bytes.HasPrefix(tmp,[]byte{0x68}){
				recvBytes = append(recvBytes,tmp[:n]...)
				if n < 18{
					for{
						i,err := conn.Read(tmp)
						if err!=nil{
							fmt.Println(err)
							break
						}
						if i<=0{
							continue
						}
						fmt.Printf("% X\n",tmp[0:i])
						recvBytes = append(recvBytes,tmp[:i]...)
						n +=i
						if n>=18{
							break
						}
					}
				}
				fmt.Printf("% X\n",recvBytes)
				go HandData(recvBytes)
			}
	}
}





