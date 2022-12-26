package main

import (
	"context"
	"flag"
	"fmt"
	"go-batch-event-stream/config"
	"go-batch-event-stream/database"
	"go-batch-event-stream/queue"
	"go-batch-event-stream/service"
	"go-batch-event-stream/util"
	"io"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"time"

	"github.com/go-co-op/gocron"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	mongoClient database.MongoClient
	err         error
	conf        config.AppConfiguration
)

//rmq handler as closure
func rmqToPublish() func() queue.Client {
	return func() queue.Client {
		//log.Info("MESSAGE QUEUE SETUP")
		mqClient, err := queue.NewAMQPConnectionWithTLS(conf.Env, conf.RabbitMQ)
		if err != nil {
			util.FailOnError("ERROR - fail to connect to rmq", err)
		}
		return mqClient
	}
}

func mongoDBHandler(ctx context.Context) database.MongoClient {
	mongoClient, err = database.NewMongoClient(ctx, conf.Mongo.URI)
	if err != nil {
		util.FailOnError("ERROR - Fail to have Mongo client", err)
	}
	return mongoClient
}

func orderApprovalStatusValidation() {
	startTM := time.Now()

	ctx, stop := context.WithTimeout(context.Background(), 120*time.Second)
	defer stop()

	mongoClient = mongoDBHandler(ctx)
	defer mongoClient.DisconnectMongoClient(ctx)
	db := mongoClient.GetDatabase(conf.Mongo.Database)
	batchProcessCollection := db.Collection(conf.Mongo.Collection.DQBatch)

	batchProcessStore := service.BatchProcessStore{C: batchProcessCollection}
	gdqBatch, err := batchProcessStore.GetBatchProcessInfo("order_pending_for_approval")
	if err != nil {
		util.FailOnError("ERROR - Fail to get Batch Info", err)
	}
	startRunDatetime := gdqBatch.NextRunTime
	curDatetimeUTC := time.Now().UTC() //.Add(-time.Hour * 48)
	lastModified := curDatetimeUTC.Format("2006-01-02T15:04:05")
	//one hour past to current UTC time
	endRunDatetime := curDatetimeUTC.Add(-time.Minute * 60).Format("2006-01-02T15:04:05.999")

	//for profiling purpose only
	if conf.Env == "dit" {
		startRunDatetime = "2022-12-13T14:59:00.789"
		endRunDatetime = "2022-12-15T16:04:44.862"
	}
	//
	//Batch Stream - Commitment Dates Validation
	orderApprovalPending := db.Collection(gdqBatch.CollectionName)
	orderApprovalPendingCollection := service.OrderApprovalPendingCol{C: orderApprovalPending}

	mqClientOnUse := rmqToPublish()
	orderApprovalPendingCollection.StreamOrdersForApproval(startRunDatetime, endRunDatetime, "Q.ABC.ORDER_APPROVAL_STATUS_EVAL.XYZ", mqClientOnUse)

	_, err = batchProcessStore.UpdateBatchProcess(gdqBatch.ID, lastModified, endRunDatetime)
	util.FailOnError("ERROR - Fail to update Batch Info", err)

	endTime := time.Now()
	diff := endTime.Sub(startTM)
	log.Println("TOTAL TIME TAKEN: ", diff)
}

func setCronJobs() {
	fmt.Println("------------------------------------- BATCH INFO -------------------------------------")
	s := gocron.NewScheduler(time.UTC)
	jobName := "order_pending_for_approval"
	job, err := s.Every(10).Seconds().Tag(jobName).Do(func() {
		log.Info("--- JOB RUNNING (" + jobName + ") ---")
		orderApprovalStatusValidation()
		log.Println()
		s.LimitRunsTo(5)
	})
	//job.SetEventListeners()
	if err != nil {
		util.FailOnError("ERROR - Fail to setup Cron job", err)
	}
	log.Info("[JOB-01]: "+job.Tags()[0], " - SCHEDULED")
	job.SingletonMode()
	s.StartBlocking()
}

//run - function to run cronJobs for batchEventStream
func run(configEnv *string) {
	if configEnv == nil {
		log.Errorln("ERROR - Config environment needs to be set for process to start...")
		log.Errorln("Please use the argument -config=<environment name> along with this command")
		return
	} else if *configEnv == "" {
		log.Errorln("ERROR - Config environment needs to be set for process to start...")
		log.Errorln("Please use the argument -config=<environment name> along with this command")
		return
	}
	envStr := fmt.Sprintf("%v", *configEnv)
	log.Info("STARTING APPLICATION - GO BATCH EVENT STREAM..!")
	log.Info("SETTING RUNNING ENVIRONMENT = " + envStr)
	conf = config.GetConfig(envStr)
	setCronJobs()
}

func initLogger(logDir *string) {
	fPath := ""
	if *logDir != "" {
		fPath = fmt.Sprintf("%vaccess.log", *logDir)
	} else {
		wd, _ := os.Getwd()
		fPath = wd + "-access.log"
	}
	lumberjackLogger := &lumberjack.Logger{
		Filename:   filepath.ToSlash(fPath),
		MaxSize:    1, // MB
		MaxBackups: 10,
		MaxAge:     10, // days
	}

	// Fork writing into two outputs
	multiWriter := io.MultiWriter(os.Stderr, lumberjackLogger)

	logFormatter := new(log.TextFormatter)
	logFormatter.TimestampFormat = time.RFC1123Z // or RFC3339
	logFormatter.FullTimestamp = true

	log.SetFormatter(logFormatter)
	log.SetLevel(log.InfoLevel)
	log.SetOutput(multiWriter)
}

func runProfiler() {
	fmt.Println("No Profiling")

	// go func() {
	// 	fmt.Println("Profiling")
	// 	log.Info(http.ListenAndServe(":6060", nil))
	// }()

}

func main() {
	// cpu profiling
	//defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	// memory profiling
	defer profile.Start(profile.MemProfile, profile.MemProfileRate(1), profile.ProfilePath("."+time.Now().String())).Stop()
	configEnv := flag.String("config", "", "environment")
	logDir := flag.String("logdir", "", "logging directory")
	flag.Parse()
	initLogger(logDir)
	runProfiler()
	run(configEnv)
}
