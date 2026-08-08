//go:build ignore

#include <linux/bpf.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

struct redirect_target {
    __u32 ip;
    __u32 port;
};

struct {
    __uint(type, BPF_MAP_TYPE_HASH);
    __uint(max_entries, 256);
    __type(key, __u32);
    __type(value, struct redirect_target);
} redirect_map SEC(".maps");

SEC("cgroup/connect4")
int redirect_connect(struct bpf_sock_addr *ctx) {
    // user_port holds htons(port) in the LOWER 16 bits on this kernel.
    // bpf_ntohs converts it back to host-byte-order port number.
    // No >> 16 shift -- that was wrong and threw away the real value.
    __u32 port = bpf_ntohs((__u16)ctx->user_port);

    // Only intercept loopback -- user_ip4 is little-endian on x86,
    // so 127.0.0.1 = [0x7f,0x00,0x00,0x01] read as LE = 0x0100007f
    if (ctx->user_ip4 != 0x0100007f) {
        return 1;
    }

    struct redirect_target *target = bpf_map_lookup_elem(&redirect_map, &port);
    if (!target) {
        return 1;
    }

    ctx->user_ip4  = target->ip;
    ctx->user_port = target->port;
    return 1;
}

char _license[] SEC("license") = "GPL";