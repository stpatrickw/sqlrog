package sqlrog

import (
	nested "github.com/antonfisher/nested-logrus-formatter"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func GetLogger() *logrus.Logger {
	if logger == nil {
		logger = logrus.New()
		logger.SetFormatter(&nested.Formatter{
			HideKeys:        true,
			TimestampFormat: "2006-01-02 15:04:05",
		})
	}
	return logger
}

func Log(level string, msg string) {
	log := GetLogger()
	switch level {
	case "info":
		log.Info(msg)
	case "warn":
		log.Warn(msg)
	case "error":
		log.Error(msg)
	}
}

func Logln(level string, msg string) {
	log := GetLogger()
	switch level {
	case "info":
		log.Infoln(msg)
	case "warn":
		log.Warnln(msg)
	case "error":
		log.Errorln(msg)
	}
}
