package mesh

import (
	"context"
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"reflect"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"testing"
)

func Test_meshSelectorDesignator_Designate(t *testing.T) {
	type fields struct {
		k8sClient client.Client
	}
	type args struct {
		ctx context.Context
		obj v1.Object
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *appmesh.Mesh
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &meshSelectorDesignator{
				k8sClient: tt.fields.k8sClient,
			}
			got, err := d.Designate(tt.args.ctx, tt.args.obj)
			if (err != nil) != tt.wantErr {
				t.Errorf("Designate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Designate() got = %v, want %v", got, tt.want)
			}
		})
	}
}
