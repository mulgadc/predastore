package topology

import (
	"strings"
	"testing"
)

func testHosts() []Host {
	return []Host{
		{ID: 1, BindAddr: "0.0.0.0:6660", PublicAddr: "10.11.12.1:6660", DataDir: "/var/lib/predastore"},
		{ID: 2, BindAddr: "0.0.0.0:6660", PublicAddr: "10.11.12.2:6660", DataDir: "/var/lib/predastore"},
	}
}

func testNodes() []Node {
	return []Node{
		{ID: 1, HostID: 1, Role: RoleShardStorage},
		{ID: 2, HostID: 1, Role: RoleStateReplica},
		{ID: 3, HostID: 2, Role: RoleShardStorage},
		{ID: 4, HostID: 2, Role: RoleStateReplica},
	}
}

func TestValidate(t *testing.T) {
	if err := Validate(testHosts(), testNodes()); err != nil {
		t.Fatalf("valid topology rejected: %v", err)
	}

	cases := []struct {
		name  string
		hosts []Host
		nodes []Node
		want  string
	}{
		{"no hosts", nil, testNodes(), "no hosts"},
		{"no nodes", testHosts(), nil, "no nodes"},
		{"dup host id", append(testHosts(), Host{ID: 1, BindAddr: "b", PublicAddr: "p", DataDir: "d"}), testNodes(), "duplicate host id 1"},
		{"dup node id", testHosts(), []Node{{ID: 1, HostID: 1, Role: RoleShardStorage}, {ID: 1, HostID: 1, Role: RoleStateReplica}}, "duplicate node id 1"},
		{"unknown host ref", testHosts(), []Node{{ID: 1, HostID: 9, Role: RoleShardStorage}}, "unknown host 9"},
		{"bad role", testHosts(), []Node{{ID: 1, HostID: 1, Role: "coordinator"}}, `unknown role "coordinator"`},
		{"missing bind", []Host{{ID: 1, PublicAddr: "p", DataDir: "d"}}, []Node{{ID: 1, HostID: 1, Role: RoleShardStorage}}, "missing bind_addr"},
		{"missing public", []Host{{ID: 1, BindAddr: "b", DataDir: "d"}}, []Node{{ID: 1, HostID: 1, Role: RoleShardStorage}}, "missing public_addr"},
		{"missing data dir", []Host{{ID: 1, BindAddr: "b", PublicAddr: "p"}}, []Node{{ID: 1, HostID: 1, Role: RoleShardStorage}}, "missing data_dir"},
		{"zero host id", []Host{{BindAddr: "b", PublicAddr: "p", DataDir: "d"}}, []Node{{ID: 1, Role: RoleShardStorage}}, "must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.hosts, tc.nodes)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestNewTopologyRejectsBadSelection(t *testing.T) {
	if _, err := NewTopology(testHosts(), testNodes(), nil); err == nil {
		t.Fatal("empty local selection accepted")
	}
	if _, err := NewTopology(testHosts(), testNodes(), []int{99}); err == nil {
		t.Fatal("unknown local node accepted")
	}
	if _, err := NewTopology(testHosts(), testNodes(), []int{1, 1}); err == nil {
		t.Fatal("duplicate local node accepted")
	}
	// Local nodes may not span hosts while some node runs elsewhere.
	if _, err := NewTopology(testHosts(), testNodes(), []int{1, 3}); err == nil {
		t.Fatal("empty pipe name accepted")
	}
}

func TestTopologyNodeAddr(t *testing.T) {
	topo, err := NewTopology(testHosts(), testNodes(), []int{1, 2})
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}

	// Local nodes resolve to their own in-process pipe endpoint.
	for _, id := range []int{1, 2} {
		addr, err := topo.NodeAddr(id)
		if err != nil {
			t.Fatalf("NodeAddr(%d): %v", id, err)
		}
		if want := NodeKey(id); addr.Network() != "pipe" || addr.String() != want {
			t.Fatalf("node %d resolved to %s/%s, want pipe/%s", id, addr.Network(), addr.String(), want)
		}
	}

	// Remote nodes resolve to their host's public address, keyed by node.
	for id, want := range map[int]string{3: "10.11.12.2:6660/node-3", 4: "10.11.12.2:6660/node-4"} {
		addr, err := topo.NodeAddr(id)
		if err != nil {
			t.Fatalf("NodeAddr(%d): %v", id, err)
		}
		if addr.Network() != "quic" || addr.String() != want {
			t.Fatalf("node %d resolved to %s/%s, want quic/%s", id, addr.Network(), addr.String(), want)
		}
	}

	if _, err := topo.NodeAddr(42); err == nil {
		t.Fatal("unknown node resolved")
	}
}

func TestTopologySelectors(t *testing.T) {
	topo, err := NewTopology(testHosts(), testNodes(), []int{2, 1})
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}

	local := topo.LocalNodes()
	if len(local) != 2 || local[0].ID != 1 || local[1].ID != 2 {
		t.Fatalf("LocalNodes = %+v", local)
	}

	replicas := topo.NodesByRole(RoleStateReplica)
	if len(replicas) != 2 || replicas[0].ID != 2 || replicas[1].ID != 4 {
		t.Fatalf("NodesByRole(state-replica) = %+v", replicas)
	}

	if !topo.NeedsNetwork() {
		t.Fatal("NeedsNetwork = false with remote nodes present")
	}
	if got := topo.LocalHost().BindAddr; got != "0.0.0.0:6660" {
		t.Fatalf("LocalHost().BindAddr = %v", got)
	}

	// A local node serves its pipe endpoint and this host's socket.
	addrs, err := topo.ListenAddrs(1)
	if err != nil {
		t.Fatalf("ListenAddrs: %v", err)
	}
	if len(addrs) != 2 || addrs[0].Network() != "pipe" || addrs[1].Network() != "quic" {
		t.Fatalf("ListenAddrs(1) = %v", addrs)
	}
	if _, err := topo.ListenAddrs(3); err == nil {
		t.Fatal("ListenAddrs resolved a node that runs elsewhere")
	}

	if !topo.IsLocal(1) || topo.IsLocal(3) {
		t.Fatal("IsLocal misclassified nodes")
	}
}

func TestTopologyAllLocal(t *testing.T) {
	topo, err := NewTopology(testHosts(), testNodes(), []int{1, 2, 3, 4})
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	// Peers all run locally: the process opens no network socket, and nodes
	// may span hosts because there is no socket to disambiguate.
	if topo.NeedsNetwork() {
		t.Fatal("NeedsNetwork = true with every node local")
	}
	addrs, err := topo.ListenAddrs(3)
	if err != nil {
		t.Fatalf("ListenAddrs: %v", err)
	}
	if len(addrs) != 1 || addrs[0].Network() != "pipe" {
		t.Fatalf("single-process node listens on %v, want one pipe address", addrs)
	}
	// Data directories still come from each node's own host.
	if got := topo.DataDir(3); got != "/var/lib/predastore/node-3" {
		t.Fatalf("DataDir(3) = %s", got)
	}
}
