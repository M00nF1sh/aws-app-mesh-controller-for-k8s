package virtualrouter

import (
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/references"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const (
	ReferenceKindVirtualNode = "VirtualNode"
)

// extractVirtualNodeReferences extracts all virtualNodeReferences for this virtualRouter
func extractVirtualNodeReferences(vr *appmesh.VirtualRouter) []appmesh.VirtualNodeReference {
	var vnRefs []appmesh.VirtualNodeReference
	for _, route := range vr.Spec.Routes {
		if route.GRPCRoute != nil {
			for _, target := range route.GRPCRoute.Action.WeightedTargets {
				vnRefs = append(vnRefs, target.VirtualNodeRef)
			}
		}
		if route.HTTPRoute != nil {
			for _, target := range route.HTTPRoute.Action.WeightedTargets {
				vnRefs = append(vnRefs, target.VirtualNodeRef)
			}
		}
		if route.HTTP2Route != nil {
			for _, target := range route.HTTP2Route.Action.WeightedTargets {
				vnRefs = append(vnRefs, target.VirtualNodeRef)
			}
		}
		if route.TCPRoute != nil {
			for _, target := range route.TCPRoute.Action.WeightedTargets {
				vnRefs = append(vnRefs, target.VirtualNodeRef)
			}
		}
	}
	return vnRefs
}

func VirtualNodeReferenceIndexFunc(obj runtime.Object) []types.NamespacedName {
	vr := obj.(*appmesh.VirtualRouter)
	vnRefs := extractVirtualNodeReferences(vr)
	var vnKeys []types.NamespacedName
	for _, vnRef := range vnRefs {
		vnKeys = append(vnKeys, references.ObjectKeyForVirtualNodeReference(vr, vnRef))
	}
	return vnKeys
}
