package ebpf

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"syscall"

	ciliumebpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

const pinnedMapPath = "/sys/fs/bpf/mesh_redirect_map"

type Redirector struct {
	objs RedirectObjects
	link link.Link
}

func NewRedirector(cgroupPath string) (*Redirector, error) {
	os.Remove(pinnedMapPath)

	objs := RedirectObjects{}
	if err := LoadRedirectObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	if err := objs.RedirectMap.Pin(pinnedMapPath); err != nil {
		objs.Close()
		return nil, fmt.Errorf("pinning map: %w", err)
	}

	l, err := link.AttachCgroup(link.CgroupOptions{
		Path:    cgroupPath,
		Attach:  ciliumebpf.AttachCGroupInet4Connect,
		Program: objs.RedirectConnect,
	})
	if err != nil {
		os.Remove(pinnedMapPath)
		objs.Close()
		return nil, fmt.Errorf("attaching cgroup program: %w", err)
	}

	return &Redirector{objs: objs, link: l}, nil
}

func (r *Redirector) SetTarget(origPort uint16, destIP net.IP, destPort uint16) error {
	ip4 := destIP.To4()
	if ip4 == nil {
		return fmt.Errorf("only IPv4 supported")
	}

	key := uint32(origPort)

	target := RedirectRedirectTarget{
		
		Ip: binary.LittleEndian.Uint32(ip4),

	
		Port: uint32(htons(destPort)),
	}

	return r.objs.RedirectMap.Put(key, target)
}

func (r *Redirector) RemoveTarget(origPort uint16) error {
	return r.objs.RedirectMap.Delete(uint32(origPort))
}

func (r *Redirector) Close() {
	if r.link != nil {
		r.link.Close()
	}
	os.Remove(pinnedMapPath)
	r.objs.Close()
}

func DefaultCgroupPath() (string, error) {
	candidates := []string{
		"/sys/fs/cgroup",
		"/sys/fs/cgroup/unified",
	}
	for _, path := range candidates {
		if isCgroupV2Root(path) {
			return path, nil
		}
	}
	return "", fmt.Errorf("no cgroup v2 root found; add 'kernelCommandLine=cgroup_no_v1=all' to ~/.wslconfig")
}

func isCgroupV2Root(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	const cgroup2SuperMagic = 0x63677270
	return st.Type == cgroup2SuperMagic
}

func htons(v uint16) uint16 {
	b := [2]byte{}
	binary.BigEndian.PutUint16(b[:], v)
	return binary.LittleEndian.Uint16(b[:])
}