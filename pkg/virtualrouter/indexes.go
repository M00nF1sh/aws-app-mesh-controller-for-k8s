package virtualrouter

import (
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/k8s"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

const VirtualNodeReferenceIndex = "virtualNodeReference"

// BuildVirtualNodeReferenceIndexes returns the values for IndexKeyVirtualNodeReference.
func BuildVirtualNodeReferenceIndexes(vr *appmesh.VirtualRouter) []string {
	vnRefs := extractVirtualNodeReferences(vr)

	vnRefIndexValues := sets.NewString()
	for _, vnRef := range vnRefs {
		vnRefIndexValues.Insert(buildVirtualNodeReferenceIndexKeyForVNRef(vr, vnRef))
	}
	return vnRefIndexValues.List()
}

// buildVirtualNodeReferenceIndexKeyForVNRef calculates the virtualNodeReference index key for a virtualNodeReference
// which is the full-qualified name of virtualNode.
func buildVirtualNodeReferenceIndexKeyForVNRef(vr *appmesh.VirtualRouter, vnRef appmesh.VirtualNodeReference) string {
	namespace := vr.GetNamespace()
	if vnRef.Namespace != nil && len(*vnRef.Namespace) != 0 {
		namespace = *vnRef.Namespace
	}
	vnKey := types.NamespacedName{Namespace: namespace, Name: vnRef.Name}
	return vnKey.String()
}

// buildVirtualNodeReferenceIndexKeyForVN calculated the virtualNodeReference index key for a virtualNode
// which is the full-qualified name of virtualNode.
func buildVirtualNodeReferenceIndexKeyForVN(vn *appmesh.VirtualNode) string {
	vnKey := k8s.NamespacedName(vn)
	return vnKey.String()
}
