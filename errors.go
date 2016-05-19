package goque

import "fmt"

type ErrorSlice []error

func NewErrorSlice() ErrorSlice {
	return ErrorSlice([]error{})
}

func (s ErrorSlice) Error() string { return fmt.Sprintln(s) }
