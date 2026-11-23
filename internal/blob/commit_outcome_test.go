package blob

import (
	"errors"
	"testing"
)

// A gate upgraded before its peers reads commits from nodes that answer
// overtaken for an epoch they never prepared. Only a node that says it holds
// the generation may have its overtaken answer counted as durable.
func TestAnOvertakenAnswerCountsOnlyWhenTheNodeHoldsTheGeneration(t *testing.T) {
	cases := []struct {
		name       string
		resp       Response
		superseded bool
		err        error
	}{
		{"published", Response{}, false, nil},
		{"overtaken and held, from an upgraded node", Response{Superseded: true, Held: true}, true, nil},
		{"overtaken without held, from an older node", Response{Superseded: true}, true, ErrOvertakenUnproven},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			superseded, err := commitOutcome(&c.resp)
			if superseded != c.superseded || !errors.Is(err, c.err) {
				t.Fatalf("commitOutcome(%+v) = (%v, %v), want (%v, %v)", c.resp, superseded, err, c.superseded, c.err)
			}
		})
	}
}
