package mesh

import (
	"context"
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MembershipDesignator designates mesh membership for namespaced AppMesh CRs
type MembershipDesignator interface {
	// Designate will choose a mesh for given namespaced AppMesh CR.
	Designate(ctx context.Context, obj metav1.Object) (*appmesh.Mesh, error)
}

// meshSelectorDesignator designates mesh membership based on selectors on mesh.
type meshSelectorDesignator struct {
	k8sClient client.Client
}

func (d *meshSelectorDesignator) Designate(ctx context.Context, obj metav1.Object) (*appmesh.Mesh, error) {
	objNS := corev1.Namespace{}
	if err := d.k8sClient.Get(ctx, types.NamespacedName{Name: obj.GetNamespace()}, &objNS); err != nil {
		return nil, errors.Wrapf(err, "failed to get namespace: %s", objNS.GetNamespace())
	}
	meshList := appmesh.MeshList{}
	if err := d.k8sClient.List(ctx, &meshList); err != nil {
		return nil, errors.Wrap(err, "failed to list meshes in cluster")
	}
	var meshesDesignated []*appmesh.Mesh
	for _, meshObj := range meshList.Items {
		selector, err := metav1.LabelSelectorAsSelector(meshObj.Spec.NamespaceSelector)
		if err != nil {
			return nil, err
		}
		if selector.Matches(labels.Set(objNS.Labels)) {
			meshesDesignated = append(meshesDesignated, meshObj.DeepCopy())
		}
	}
	if len(meshesDesignated) != 1 {
		return nil, errors.Errorf("expecting 1 but find %d matching mesh", len(meshesDesignated))
	}
	return meshesDesignated[0], nil
}
