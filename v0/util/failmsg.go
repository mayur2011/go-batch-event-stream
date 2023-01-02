package util

import log "github.com/sirupsen/logrus"

func FailOnError(msg string, err error) {
	if err != nil {
		log.Fatalln("MESSAGE:", msg, "ERROR:", err)
	}
}
