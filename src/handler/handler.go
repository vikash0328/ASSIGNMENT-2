package handler

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

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
