package messaging

import (
	"reflect"
	"testing"
)

func Test_parseJolieOutputOutgoing(t *testing.T) {
	type args struct {
		output []byte
	}
	tests := []struct {
		name string
		args args
		want *outboundMsgJolieOut
	}{
		struct {
			name string
			args args
			want *outboundMsgJolieOut
		}{
			name: "Simple ",
			args: args{
				output: []byte(`{
				"messageBody": "test123"
			}`),
			},
			want: &outboundMsgJolieOut{
				MessageBody: "test123",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseJolieOutputOutgoing(tt.args.output); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJolieOutputOutgoing() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJolieOutputIncoming(t *testing.T) {
	type args struct {
		output []byte
	}
	tests := []struct {
		name string
		args args
		want *inboundMsgJolieOut
	}{
		struct {
			name string
			args args
			want *inboundMsgJolieOut
		}{
			name: "Simple use case",
			args: args{
				output: []byte(`{
				"action": "drop",
				"reply": [
					{
						"to": 42,
						"message": "Hello, 42"
					}
				]
			}`),
			},
			want: &inboundMsgJolieOut{
				Action: "drop",
				Reply: []simpleMessage{
					simpleMessage{
						To:      42,
						Message: "Hello, 42",
					},
				},
			},
		},
		// struct { // Tests will not handle equality of empty lists well.
		// 	name string
		// 	args args
		// 	want *inboundMsgJolieOut
		// }{
		// 	name: "empty reply list",
		// 	args: args{
		// 		output: []byte(`{
		// 		"action": "drop"
		// 	}`),
		// 	},
		// 	want: &inboundMsgJolieOut{
		// 		Action: "drop",
		// 		Reply:  []simpleMessage{
		// 			// simpleMessage{
		// 			// 	To:      42,
		// 			// 	Message: "Hello, 42",
		// 			// },
		// 		},
		// 	},
		// },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseJolieOutputIncoming(tt.args.output); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJolieOutputIncoming() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJolieInputIncoming(t *testing.T) {
	type args struct {
		data *inboundMsgJolieIn
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseJolieInputIncoming(tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJolieInputIncoming() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parseJolieInputOutgoing(t *testing.T) {
	type args struct {
		data *outboundMsgJolieIn
	}
	tests := []struct {
		name string
		args args
		want []byte
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseJolieInputOutgoing(tt.args.data); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseJolieInputOutgoing() = %v, want %v", got, tt.want)
			}
		})
	}
}
