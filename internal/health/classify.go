package health

import (
	"context"
	"net/http"
)

type Outcome uint8

const (
	Success Outcome = iota
	Failure
	Ignored
)

func (o Outcome) String() string {
	switch o {
	case Success:
		return "success"
	case Failure:
		return "failure"
	case Ignored:
		return "ignored"
	}
	return "unknown"
}

func Classify(reqCtx context.Context, resp *http.Response, err error) Outcome {
	if err == nil {
		switch {
		case resp.StatusCode >= 500:
			return Failure
		case resp.StatusCode >= 400:
			return Ignored
		default:
			return Success
		}
	}
	if reqCtx.Err() == context.Canceled {
		return Ignored
	}
	return Failure
}
