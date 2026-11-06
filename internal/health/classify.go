package health

import (
	"context"
	"net/http"
)

// Outcome classifies one completed upstream request for passive health tracking.
type Outcome uint8

// Outcome values. Failure counts toward ejection, Success resets the consecutive failure count, and Ignored changes nothing.
const (
	Success Outcome = iota
	Failure
	Ignored
)

// String returns the lowercase name of the outcome, or "unknown" for any other value.
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

// Classify maps the result of one upstream request to an Outcome. A transport error is a Failure, unless the client canceled the request, which is Ignored. A response with status 500 or higher is a Failure, one with status 400 to 499 is Ignored, and any other response is a Success.
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
