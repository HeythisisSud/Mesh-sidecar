package proxy

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"sort"
	"sync/atomic"

	"github.com/HeythisisSud/mesh-sidecar/members"
)




func NewProxy(node *members.Node) (http.Handler, error) {
	var counter atomic.Uint64

	p := &httputil.ReverseProxy{
		Director: func(r *http.Request) {
			snapshot := node.SnapShot()
			var alive []members.MemberState
			for _, m := range snapshot {
				if m.Status == members.StatusAlive {
					alive = append(alive, m)
				}
			}
			if len(alive) == 0 {
				r.URL.Host = ""
				return
			}
			sort.Slice(alive, func(i, j int) bool { return alive[i].ID < alive[j].ID })
			idx := counter.Add(1) - 1
			target := alive[idx%uint64(len(alive))]
			host, _, err := net.SplitHostPort(target.Addr.String())
			if err != nil {
				r.URL.Host = ""
				return
			}
			appPort := target.Addr.Port + 1000
			r.URL.Host = fmt.Sprintf("%s:%d", host, appPort)
			r.URL.Scheme = "http"
		},
	}
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "no healthy backend available", http.StatusServiceUnavailable)
	}
	return p, nil
}
