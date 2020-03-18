package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"time"
)

func NewRequestThrottler() *RequestThrottler {
	return &RequestThrottler{
		serviceThrottles:   make(map[string]<-chan time.Time),
		operationThrottles: make(map[string]map[string]<-chan time.Time),
	}
}

type RequestThrottler struct {
	// throttles applies to all operations of services.
	serviceThrottles map[string]<-chan time.Time

	// throttles applies to specific operation of services.
	operationThrottles map[string]map[string]<-chan time.Time
}

// WithServiceThrottleRate configure throttler with rates applies to all operations of services.
func (t *RequestThrottler) WithServiceThrottleRate(serviceName string, rate time.Duration) *RequestThrottler {
	throttle := time.Tick(rate)
	t.serviceThrottles[serviceName] = throttle
	return t
}

// WithOperationThrottleRate configure throttler with rates applies to specific operation of services.
func (t *RequestThrottler) WithOperationThrottleRate(serviceName string, operationName string, rate time.Duration) *RequestThrottler {
	if _, ok := t.operationThrottles[serviceName]; !ok {
		t.operationThrottles[serviceName] = make(map[string]<-chan time.Time)
	}
	throttle := time.Tick(rate)
	t.operationThrottles[serviceName][operationName] = throttle
	return t
}

// AttachToSession will apply request throttler to a aws session
func (t *RequestThrottler) AttachToSession(sess *session.Session) {
	sess.Handlers.Sign.PushFrontNamed(request.NamedHandler{
		Name: "request-throttler",
		Fn:   t.beforeSign,
	})
}

// beforeSign is added to the Sign chain; called before each request
func (t *RequestThrottler) beforeSign(r *request.Request) {
	svcName := r.ClientInfo.ServiceName
	opName := operationName(r)
	fmt.Println(svcName, opName)
	if throttle, ok := t.serviceThrottles[svcName]; ok {
		<-throttle
	}
	if opThrottles, ok := t.operationThrottles[svcName]; ok {
		if throttle, ok := opThrottles[opName]; ok {
			<-throttle
		}
	}
}

// operationName returns operation name of aws request
func operationName(r *request.Request) string {
	name := "?"
	if r.Operation != nil {
		name = r.Operation.Name
	}
	return name
}
