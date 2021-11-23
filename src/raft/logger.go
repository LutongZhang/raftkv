package raft

import (
	rotateLogs "github.com/lestrrat-go/file-rotatelogs"
	log "github.com/sirupsen/logrus"
	"time"

	//"time"
)

func init() {

}

func (rf *Raft)initLogger(){
	log.SetFormatter(&log.TextFormatter{
		//ForceColors:               true,
		EnvironmentOverrideColors: true,
		TimestampFormat:           "2006-01-02 15:04:05",
		FullTimestamp:true,
		//DisableLevelTruncation:true,
	})

	path := "./raft_log/raft.log"
	logfile,_ :=rotateLogs.New(
		path+".%Y%m%d%H%M",
		rotateLogs.WithLinkName(path),
		rotateLogs.WithMaxAge(time.Duration(12)*time.Hour),
		rotateLogs.WithRotationTime(time.Duration(6)*time.Hour),
		)
	log.SetOutput(logfile)

	//show code line
	log.SetReportCaller(true)

	log.SetLevel(log.InfoLevel)
}

func (rf *Raft)log_info(args ...interface{}){
	log.WithFields(log.Fields{"R":roleStr(rf.role),"T":rf.currentTerm,"me":rf.me}).Info(args...)
}

func (rf *Raft)log_infof(str string, args ...interface{}){
	log.WithFields(log.Fields{"R":roleStr(rf.role),"T":rf.currentTerm,"me":rf.me}).Infof(str,args...)
}

func (rf *Raft)log_error(args ...interface{}){
	log.WithFields(log.Fields{"R":roleStr(rf.role),"T":rf.currentTerm,"me":rf.me}).Error(args...)
}