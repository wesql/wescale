package role

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_probeFuncEntry_String(t *testing.T) {
	type fields struct {
		name string
		fn   ProbeFunc
	}
	tests := []struct {
		fields fields
		want   string
	}{
		{
			fields: fields{
				name: "test",
				fn:   nil,
			},
			want: "test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			p := &probeFuncEntry{
				name: tt.fields.name,
				fn:   tt.fields.fn,
			}
			if got := p.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_reloadAutoProbeFuncList(t *testing.T) {
	autoRoleProbeImplementationList = []string{"http", "wesql", "mysql"}
	currentProbeFuncEntryList = make([]probeFuncEntry, 0)
	ProbeFuncMap = make(map[string]ProbeFunc)
	ProbeFuncMap["http"] = httpProbe
	ProbeFuncMap["wesql"] = wesqlProbe
	ProbeFuncMap["mysql"] = mysqlProbe
	reloadAutoProbeFuncList()

	assert.Equal(t, 3, len(currentProbeFuncEntryList))

	autoRoleProbeImplementationList = []string{"http", "wesql", "mysql", "unknown"}
	reloadAutoProbeFuncList()
	assert.Equal(t, 3, len(currentProbeFuncEntryList))
}
