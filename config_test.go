package predastore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// twoHostCluster is two hosts, each running a gate and two blob nodes, which is
// the shape that tells a per-host answer apart from a cluster-wide one.
func twoHostCluster() *Config {
	enabled := true

	return &Config{
		Region: "ap-southeast-2",
		RS:     RS{Data: 2, Parity: 1},
		Repair: Repair{Enabled: &enabled, Workers: 3, PageSize: 64, IntervalSeconds: 90},
		Hosts: []HostConfig{
			{
				ID: 1, Addr: "10.0.0.1", TLSCert: "cert", TLSKey: "key",
				Nodes: []NodeConfig{
					{ID: 1, Role: RoleGate, Port: 8443},
					{ID: 2, Role: RoleBlob},
					{ID: 3, Role: RoleBlob},
					{ID: 4, Role: RoleMeta},
				},
			},
			{
				ID: 2, Addr: "10.0.0.2", TLSCert: "cert", TLSKey: "key",
				Nodes: []NodeConfig{
					{ID: 5, Role: RoleGate, Port: 8443},
					{ID: 6, Role: RoleBlob},
					{ID: 7, Role: RoleBlob},
				},
			},
		},
	}
}

// TestGateRepairsOnlyItsOwnHostsBlobNodes is what makes the coordinator choice
// safe without an election: every blob node is swept by exactly one gate. A
// cluster-wide list here would have each gate rebuilding into every node.
func TestGateRepairsOnlyItsOwnHostsBlobNodes(t *testing.T) {
	t.Parallel()
	c := twoHostCluster()

	first := gateConfig(c, c.Hosts[0], c.Hosts[0].Nodes[0], nil, nil)
	second := gateConfig(c, c.Hosts[1], c.Hosts[1].Nodes[0], nil, nil)

	assert.Equal(t, []NodeID{2, 3}, first.LocalBlobNodeIDs)
	assert.Equal(t, []NodeID{6, 7}, second.LocalBlobNodeIDs)

	// The ring, by contrast, is every blob node in the cluster: placement is a
	// cluster-wide question and repair ownership is not.
	assert.Equal(t, []NodeID{2, 3, 6, 7}, first.BlobNodeIDs)
	assert.Equal(t, first.BlobNodeIDs, second.BlobNodeIDs)
}

func TestRepairSettingsReachTheGate(t *testing.T) {
	t.Parallel()
	c := twoHostCluster()

	got := gateConfig(c, c.Hosts[0], c.Hosts[0].Nodes[0], nil, nil).Repair
	assert.True(t, got.Enabled)
	assert.Equal(t, 3, got.Workers)
	assert.Equal(t, 64, got.PageSize)
	assert.Equal(t, 90*time.Second, got.Interval)
}

// TestRepairRunsUnlessItIsRefused keeps the default honest: a file that says
// nothing about repair sweeps anyway, because the redundancy window degraded
// writes open is closed by nothing else.
func TestRepairRunsUnlessItIsRefused(t *testing.T) {
	t.Parallel()
	c := twoHostCluster()
	c.Repair = Repair{}

	got := gateConfig(c, c.Hosts[0], c.Hosts[0].Nodes[0], nil, nil).Repair
	assert.True(t, got.Enabled)
	assert.Zero(t, got.Interval, "an unset interval must fall through to the sweep's own default")

	off := false
	c.Repair = Repair{Enabled: &off}
	assert.False(t, gateConfig(c, c.Hosts[0], c.Hosts[0].Nodes[0], nil, nil).Repair.Enabled,
		"an operator who refuses the sweep is still obeyed")
}

// TestGateOnlyHostRepairsForNothing covers the deployment where the gate does
// not share a process with any blob node. It is not a misconfiguration: there
// is simply nothing for it to sweep.
func TestGateOnlyHostRepairsForNothing(t *testing.T) {
	t.Parallel()
	c := twoHostCluster()
	c.Hosts[0].Nodes = []NodeConfig{{ID: 1, Role: RoleGate, Port: 8443}}

	got := gateConfig(c, c.Hosts[0], c.Hosts[0].Nodes[0], nil, nil)
	assert.Empty(t, got.LocalBlobNodeIDs)
	assert.NotEmpty(t, got.BlobNodeIDs, "the ring still spans the other host's nodes")
}
