package ebpf

//go:generate go run github.com/cilium/ebpf/cmd/bpf2go -cc clang -cflags "-O2 -g -Wall -target bpf -I/usr/include/x86_64-linux-gnu" Redirect ./redirect.c
