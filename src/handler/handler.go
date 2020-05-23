package handler

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

/*******************************************************************************************
*  InitZap  Initialises  path to logger,formating of log ,log-rotation policy
*
*
**********************************************************************************************/

func InitZapLog() *zap.Logger {
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   "prod.log",
		MaxSize:    1, // megabytes
		MaxBackups: 30,
		MaxAge:     30, // days
	})

	config := zapcore.EncoderConfig{
		MessageKey: viper.GetString("MessageKey"),

		LevelKey:    viper.GetString("LevelKey"),
		EncodeLevel: zapcore.CapitalLevelEncoder,

		TimeKey:    viper.GetString("TimeKey"),
		EncodeTime: zapcore.ISO8601TimeEncoder,

		CallerKey:    viper.GetString("CallerKey"),
		EncodeCaller: zapcore.ShortCallerEncoder,
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(config),
		zapcore.NewMultiWriteSyncer(w),
		zap.InfoLevel,
	)
	logger := zap.New(core, zap.AddCaller(), zap.Development())
	logger.Sugar()
	return logger
}

/*****************************************************************************************
*   InitViper sets the file the from which we reading constants in our code
*
*
********************************************************************************************/

func InitViper() bool {

	viper.SetConfigName("config")

	viper.AddConfigPath(".")

	viper.AutomaticEnv()

	viper.SetConfigType("yml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err.Error())
		return false
	}
	return true
}
