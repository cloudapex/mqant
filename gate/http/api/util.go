package httpgateapi

import (
	"fmt"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"strings"

	api "github.com/liangdas/mqant/gate/http/proto"
)

func RequestToProto(r *http.Request) (*api.Request, error) {
	if err := r.ParseForm(); err != nil {
		return nil, fmt.Errorf("Error parsing form: %v", err)
	}

	req := &api.Request{
		Path:   r.URL.Path,
		Method: r.Method,
		Header: make(map[string]*api.Pair),
		Get:    make(map[string]*api.Pair),
		Post:   make(map[string]*api.Pair),
		Url:    r.URL.String(),
	}

	ct, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		ct = "text/plain; charset=UTF-8" //default CT is text/plain
		r.Header.Set("Content-Type", ct)
	}

	//set the body:
	if r.Body != nil {
		switch ct {
		case "application/x-www-form-urlencoded":
			// expect form vals in Post data
		default:

			data, _ := ioutil.ReadAll(r.Body)
			req.Body = string(data)
		}
	}

	// Set X-Forwarded-For if it does not exist
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if prior, ok := r.Header["X-Forwarded-For"]; ok {
			ip = strings.Join(prior, ", ") + ", " + ip
		}

		// Set the header
		req.Header["X-Forwarded-For"] = &api.Pair{
			Key:    "X-Forwarded-For",
			Values: []string{ip},
		}
	}

	// Host is stripped from net/http Headers so let's add it
	req.Header["Host"] = &api.Pair{
		Key:    "Host",
		Values: []string{r.Host},
	}

	// Get data
	for key, vals := range r.URL.Query() {
		header, ok := req.Get[key]
		if !ok {
			header = &api.Pair{
				Key: key,
			}
			req.Get[key] = header
		}
		header.Values = vals
	}

	// Post data
	for key, vals := range r.PostForm {
		header, ok := req.Post[key]
		if !ok {
			header = &api.Pair{
				Key: key,
			}
			req.Post[key] = header
		}
		header.Values = vals
	}

	for key, vals := range r.Header {
		header, ok := req.Header[key]
		if !ok {
			header = &api.Pair{
				Key: key,
			}
			req.Header[key] = header
		}
		header.Values = vals
	}

	return req, nil
}
