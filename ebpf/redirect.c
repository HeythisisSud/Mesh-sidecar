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
    // Only intercept connections to 127.0.0.1
    // user_ip4 is in network byte order as little-endian: 127.0.0.1 = 0x0100007F
    if (ctx->user_ip4 != 0x0100007F) {
        return 1;
    }

    __u32 port = bpf_ntohs((__u16)(ctx->user_port));
    struct redirect_target *target = bpf_map_lookup_elem(&redirect_map, &port);
    if (!target) {
        return 1;
    }

    bpf_printk("redirecting %d -> 0x%x:0x%x\n", port, target->ip, target->port);
    ctx->user_ip4  = target->ip;
    ctx->user_port = target->port;
    return 1;
}
char _license[] SEC("license") = "GPL";
