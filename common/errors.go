package common

import "strings"

func MultiError(errs []error) string {
	if len(errs) == 0 {
		return ""
	}
	msgs := make([]string, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			msgs = append(msgs, err.Error())
		}
	}
	return strings.Join(msgs, "\n")
}
