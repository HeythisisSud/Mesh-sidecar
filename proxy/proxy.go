package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"

	"github.com/HeythisisSud/mesh-sidecar/members"
)

func NewProxy(node *members.Node) (http.Handler, error) {

	proxy := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			for _, value := range node.SnapShot() {
				if value.Status == "Alive" {
					host, _, err := net.SplitHostPort(value.Addr.String())
					if err!=nil{
						continue
					}
					httpPort := value.Addr.Port + 1000
					r.URL.Host = fmt.Sprintf("%s:%d", host, httpPort)
					r.URL.Scheme = "http"
					return

				}
			}
			r.URL.Host = "" // deliberately invalid
        	return

		},
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
    http.Error(w, "no healthy backend available", http.StatusServiceUnavailable)
}
	return proxy, nil
}
