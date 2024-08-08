package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	// 设置随机种子
	rand.Seed(time.Now().UnixNano())

	// 连接到数据库
	dsn := "(127.0.0.1:15306)/d1"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// 验证数据库连接
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	// 记录已插入数据的 ID
	var insertedIDs []int

	// 随机插入、更新和删除数据
	for i := 0; i < 20000; i++ {
		t := rand.Intn(1000)
		switch {
		case 1 <= t && t <= 800:
			id := insertData(db)
			if id != -1 {
				insertedIDs = append(insertedIDs, id)
			}
		case 801 <= t && t <= 900:
			if len(insertedIDs) > 0 {
				updateData(db, insertedIDs)
			}
		case 901 <= t && t <= 998:
			if len(insertedIDs) > 0 {
				insertedIDs = deleteData(db, insertedIDs)
			}
		case 999 <= t && t <= 1000:
			crashAndRestartVTGate(db)
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func insertData(db *sql.DB) int {
	c2 := fmt.Sprintf("RandomText%d", rand.Intn(1000000)+100000000)
	result, err := db.Exec("INSERT INTO t1 (c2) VALUES (?)", c2)
	if err != nil {
		log.Println("Insert Error:", err)
		return -1
	}
	id, err := result.LastInsertId()
	if err != nil {
		log.Println("Insert Error:", err)
		return -1
	}
	log.Println("Inserted:", c2, "with ID:", id)
	return int(id)
}

func updateData(db *sql.DB, ids []int) {
	id := ids[rand.Intn(len(ids))]
	c2 := fmt.Sprintf("UpdatedText%d", rand.Intn(1000))
	_, err := db.Exec("UPDATE t1 SET c2 = ? WHERE c1 = ?", c2, id)
	if err != nil {
		log.Println("Update Error:", err)
	} else {
		log.Println("Updated ID:", id, "to", c2)
	}
}

func deleteData(db *sql.DB, ids []int) []int {
	idx := rand.Intn(len(ids))
	id := ids[idx]
	_, err := db.Exec("DELETE FROM t1 WHERE c1 = ?", id)
	if err != nil {
		log.Println("Delete Error:", err)
		return ids
	}
	log.Println("Deleted ID:", id)
	// 删除已记录的ID
	return append(ids[:idx], ids[idx+1:]...)
}

func crashAndRestartVTGate(db *sql.DB) {
	fmt.Printf("crash VTGate\n")
	// 关闭vtgate并重启，这里也可能返回失败，执行完后，其他并发执行了导致vtgate关闭，导致connection返回失败
	db.Exec("set @put_failpoint='vitess.io/vitess/go/vt/vtgate/CrashVTGate=return(true)';")

	// 由于并发原因，其他先执行，导致这里返回错误
	db.Exec("show failpoints;")

	//// 恢复vtgate
	//
	//// 设置工作目录
	//// 设置环境变量
	//cmd := exec.Command("../../common/scripts/vtgate-up.sh")
	//cmd.Env = append(os.Environ(), "CELL=zone1")
	//cmd.Env = append(os.Environ(), "CMD_FLAGS=\"--vschema_ddl_authorized_users % \"")
	//
	//// 运行命令
	//output, err := cmd.CombinedOutput()
	//if err != nil {
	//	log.Fatalf("Command execution failed: %v", err)
	//}
	//print(output)

	hostname := "127.0.0.1" // 替换为实际的主机名
	webPort := "15001"      // 替换为实际的端口

	fmt.Println("Waiting for vtgate to be up...")

	for {
		resp, err := http.Get(fmt.Sprintf("http://%s:%s/debug/status", hostname, webPort))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("vtgate is up!")
}
