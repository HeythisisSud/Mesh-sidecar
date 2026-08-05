package ebpf

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"

	ciliumebpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

type Redirector struct {
	objs RedirectObjects
	link link.Link
}

func NewRedirector(cgroupPath string) (*Redirector, error) {
	objs := RedirectObjects{}
	if err := LoadRedirectObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	l, err := link.AttachCgroup(link.CgroupOptions{
		Path:    cgroupPath,
		Attach:  ciliumebpf.AttachCGroupInet4Connect,
		Program: objs.RedirectConnect,
	})
	if err != nil {
		objs.Close()
		return nil, fmt.Errorf("attaching cgroup program: %w", err)
	}

	return &Redirector{objs: objs, link: l}, nil
}

// SetTarget tells the kernel to redirect connections aimed at
// origPort to destIP:destPort instead.
func (r *Redirector) SetTarget(origPort uint16, destIP net.IP, destPort uint16) error {
	ip4 := destIP.To4()
	if ip4 == nil {
		return fmt.Errorf("only IPv4 supported")
	}

	// key is the original port in network byte order, same as
	// what the kernel sees in ctx->user_port
	key := uint32(htons(origPort)) << 16

	target := RedirectRedirectTarget{
		Ip:   binary.BigEndian.Uint32(ip4),
		Port: uint32(htons(destPort)) << 16,
	}

	return r.objs.RedirectMap.Put(key, target)
}

// RemoveTarget removes a redirect rule for the given original port.
func (r *Redirector) RemoveTarget(origPort uint16) error {
	key := uint32(htons(origPort)) << 16
	return r.objs.RedirectMap.Delete(key)
}

func (r *Redirector) Close() {
	r.link.Close()
	r.objs.Close()
}

// DefaultCgroupPath returns the cgroup v2 mount point on this WSL2 system.
func DefaultCgroupPath() (string, error) {
	const path = "/sys/fs/cgroup/unified"
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("cgroup v2 not found at %s: %w", path, err)
	}
	return path, nil
}

// htons converts a uint16 from host to network byte order.
func htons(v uint16) uint16 {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return binary.LittleEndian.Uint16(b)
}
