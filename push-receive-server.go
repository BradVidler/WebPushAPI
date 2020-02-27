package main

import (
    "log"
//_ "github.com/go-sql-driver/mysql"
	"time"
	"sync"

	_ "github.com/ziutek/mymysql/native" // Native engine
	// _ "github.com/ziutek/mymysql/thrsafe" // Thread safe engine

	//"math/rand"
	//"errors"
//	"github.com/frrakn/go-spintax"
	//"strings"
	//"database/sql"
	"github.com/SherClockHolmes/webpush-go"
	"encoding/json"
	"bytes"
	"strconv"
	//"gopkg.in/robfig/cron.v2"
	"net/http"
	"github.com/ziutek/mymysql/mysql"
)

const (
	vapidPrivateKey = `FAKE-VAPID-KEY`
)

var myClient *http.Client

var totalSent = 0
var totalUnsubs = 0
var totalErrors = 0

//this is used to limit the max number of goroutines
var sem = make(chan struct{}, 5000)

type push_struct struct {
    Title string
    Message string
    Image string
    Banner string
    Url string
    Badge string
}

func dowork(rw http.ResponseWriter, req *http.Request) {
	defer func() {
		recover()
        
        var wg sync.WaitGroup
        
        wg.Wait()
        
        if req.Method == "POST" {
            decoder := json.NewDecoder(req.Body)
            var p push_struct
            err := decoder.Decode(&p)
            if err != nil {
                panic(err)
            }
            
            log.Println(p.Title)
            log.Println(p.Message)
            log.Println(p.Image)
            log.Println(p.Banner)
            log.Println(p.Url)
            log.Println(p.Badge)
            
            log.Print("Running...")
            totalSent = 0
            totalUnsubs = 0
            totalErrors = 0
            start := time.Now()
            
            var userids = make([] int, 0) // stores userids to unsubscribe after each run

            //Message ID is the same for all users since it is just the current date/time.
            msgid := time.Now()

            db := mysql.New("tcp", "", "127.0.0.1:3306", "username", "password", "database")

            err = db.Connect()
            if err != nil {
                //log.Print("(ln. 260) Cannot connect to db!: " + err.Error())
            }

            //grab all active users
            rows, res, err := db.Query("select userid, source, subdata, geo, placement from PushUsers where subscribed = 1")
            if err != nil {
                //log.Print("(ln. 266) Cannot get users from db!: " + err.Error())
            }

            var rowNum = 0

            //iterate users
            for _, row := range rows {

                //Extract column data
                useridcol := res.Map("userid")
                sourcecol := res.Map("source")
                subdatacol := res.Map("subdata")
                //geocol := res.Map("geo")
                placementcol := res.Map("placement")

                userid := row.Int(useridcol)
                source := row.Str(sourcecol)
                subdata := row.Str(subdatacol)
                //geo := row.Str(geocol)
                placement := row.Str(placementcol)

                // Decode subscription
                s := webpush.Subscription{}
                if err := json.NewDecoder(bytes.NewBufferString(subdata)).Decode(&s); err != nil {
                    //log.Print("(ln. 297) Cannot decode subscription!: " + err.Error())
                }
                //log.Print(s)

                //Every 7500 messages we wait until the requests are done to not flood our server with too many open connections
                rowNum++
                if rowNum%7500 == 0 {
                    wg.Wait()
                }

                wg.Add(1)

                // Send notifications using Go routines
                go func(sub webpush.Subscription, vapkey string, goUserid int, goSource string, goPlacement string) {

                    if err != nil {
                        log.Fatal(err)
                    }

                    var payload []byte = []byte(`{
                                "heading":"` + p.Title + `",
                                "body":"` + p.Message + `",
                                "icon":"` + p.Image + `",
                                "badge":"` + p.Badge + `",
                                "category":"` + "" + `",
                                "catid":"12345",
                                "msgid":"` + msgid.Format("2006-01-02 15:04:05") + `",
                                "source":"` + goSource + `",
                                "sourceid":"12345",
                                "placement":"` + placement + `",
                                "banner":"` + p.Banner + `",
                                "url":"` + p.Url + `",
                                "updateflag":"` + "1" + `",
                                "userid":"` + strconv.Itoa(goUserid) + `"
                              }`)

                    r, err := webpush.SendNotification([]byte(payload), &sub, &webpush.Options{
                        //HTTPClient:      client,
                        Topic:           "new_event",
                        TTL:             0,
                        Urgency:         "high",
                        VAPIDPrivateKey: vapkey,
                    })
                    if err != nil {
                        //log.Print("(ln. 430) Cannot send notification!: " + err.Error())
                        //userids = append(userids, goUserid)
                        totalErrors++
                    }
                    if r != nil {
                        //log.Print(r.Status)
                        totalSent++
                        if r.Status == `410 Gone` { //unsubscribed
                            userids = append(userids, goUserid)
                            totalUnsubs++
                        } else if r.Status == `201 Created` { //success
                            //do nothing
                        } else if r.Status == `404 Not Found` { //expired/unsubbed
                            userids = append(userids, goUserid)
                            totalUnsubs++
                        } else {
                            totalErrors++
                            //log.Print(r.Status)
                        }

                    }
                    wg.Done()
                }(s, vapidPrivateKey, userid, source, placement)

            }

            wg.Wait()

            stmt, err := db.Prepare("UPDATE PushUsers SET subscribed = ?, unsub_date = now() WHERE userid = ?")
            if err != nil {
                //log.Print("(ln. 459) Cannot unsusbscribe user in db!: " + err.Error())
            }

            for _, uid := range userids {
                //log.Print(i)
                //log.Print(uid)

                _, err := stmt.Run(0,uid)
                if err != nil {
                    //log.Print(err.Error())
                }

            }
            //stmt.Close()

            db.Close()
            
            elapsed := time.Since(start)
            log.Printf("Execution time: %s", elapsed)
            log.Printf("Total Sent: " + strconv.Itoa(totalSent) + " Total Unsubs: " + strconv.Itoa(totalUnsubs) + " Total Errors: " + strconv.Itoa(totalErrors))
            log.Print("===================================================================================")
	    }
	}()
}

func main() {
    
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 250

    // Wait for message to be received and then send it when it comes in
    http.HandleFunc("/push", dowork)
    log.Fatal(http.ListenAndServe(":8082", nil))
}