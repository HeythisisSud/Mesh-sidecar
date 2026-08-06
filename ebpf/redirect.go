package ebpf

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"

	ciliumebpf "github.com/cilium/ebpf"
	"github.com/cilium/ebpf/link"
)

const pinnedMapPath = "/sys/fs/bpf/mesh_redirect_map"

type Redirector struct {
	objs RedirectObjects
	link link.Link
}

func NewRedirector(cgroupPath string) (*Redirector, error) {
	// remove stale pin from a previous run so we always start fresh
	os.Remove(pinnedMapPath)

	objs := RedirectObjects{}
	if err := LoadRedirectObjects(&objs, nil); err != nil {
		return nil, fmt.Errorf("loading BPF objects: %w", err)
	}

	// pin the map to the BPF filesystem so other processes can
	// open it by path if needed in the future
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

// SetTarget tells the kernel to redirect connections aimed at
// origPort to destIP:destPort instead.
//
// Byte order notes:
//   - key: plain host-byte-order port number, matching what the C
//     program extracts via bpf_ntohs(ctx->user_port >> 16)
//   - Ip: little-endian uint32 -- this is what bpf_sock_addr->user_ip4
//     expects on a little-endian (x86) machine for 127.0.0.1 = 0x0100007f
//   - Port: network-byte-order port in the upper 16 bits of a uint32,
//     matching what bpf_sock_addr->user_port expects
func (r *Redirector) SetTarget(origPort uint16, destIP net.IP, destPort uint16) error {
	ip4 := destIP.To4()
	if ip4 == nil {
		return fmt.Errorf("only IPv4 supported")
	}

	key := uint32(origPort)

	// user_ip4 is stored little-endian on x86
	ipLE := binary.LittleEndian.Uint32(ip4)

	// user_port: network-byte-order port in upper 16 bits
	portVal := uint32(binary.BigEndian.Uint16([]byte{
		byte(destPort >> 8),
		byte(destPort),
	})) << 16

	log.Printf("SetTarget: key=%d ip=0x%08x port=0x%08x", origPort, ipLE, portVal)

	target := RedirectRedirectTarget{
		Ip:   ipLE,
		Port: portVal,
	}

	return r.objs.RedirectMap.Put(key, target)
}

// RemoveTarget removes a redirect rule for the given original port.
func (r *Redirector) RemoveTarget(origPort uint16) error {
	key := uint32(origPort)
	return r.objs.RedirectMap.Delete(key)
}

// Close detaches the cgroup program and cleans up all resources.
func (r *Redirector) Close() {
	if r.link != nil {
		r.link.Close()
		os.Remove(pinnedMapPath)
	}
	r.objs.Close()
}

// DefaultCgroupPath returns the cgroup v2 path where WSL2 processes live.
func DefaultCgroupPath() (string, error) {
	const path = "/sys/fs/cgroup/init.scope"
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("cgroup path not found at %s: %w", path, err)
	}
	return path, nil
}