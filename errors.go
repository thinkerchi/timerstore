package timerstore

import (
	"fmt"
)

// TimerError 自定义error类型, 业务得到此包里的error时使用断言到此类型Error
type TimerError struct {
	Code uint32
	Msg  string
}

func (t *TimerError) Error() string {
	return fmt.Sprintf("code: %d, msg: %s", t.Code, t.Msg)
}

var (
	errDuplicate      = &TimerError{2, "duplicate registered"}
	errUnkownProvider = &TimerError{2, "provider is unknown"}
)
