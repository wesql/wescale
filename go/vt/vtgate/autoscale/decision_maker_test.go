package autoscale

import (
	"testing"
	"time"
)

func TestGetSecondsByDuration(t *testing.T) {
	type args struct {
		p time.Duration
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Test GetSecondsByDuration",
			args: args{
				p: time.Duration(5) * time.Minute,
			},
			want: 300,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSecondsByDuration(&tt.args.p); got != tt.want {
				t.Errorf("GetSecondsByDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}
