package virtualrouter

import (
	"context"
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/aws/services"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/conversions"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/equality"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/k8s"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	appmeshsdk "github.com/aws/aws-sdk-go/service/appmesh"
	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/util/sets"
)

type routesManager interface {
	create(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (map[string]*appmeshsdk.RouteData, error)
	update(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (map[string]*appmeshsdk.RouteData, error)
	cleanup(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter) error
}

// newDefaultRoutesManager constructs new routesManager
func newDefaultRoutesManager(appMeshSDK services.AppMesh, log logr.Logger) routesManager {
	return &defaultRoutesManager{
		appMeshSDK: appMeshSDK,
		log:        log,
	}
}

type defaultRoutesManager struct {
	appMeshSDK services.AppMesh
	log        logr.Logger
}

func (m *defaultRoutesManager) create(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (map[string]*appmeshsdk.RouteData, error) {
	return m.reconcile(ctx, ms, vr, vnByRefHash, vr.Spec.Routes, nil)
}

func (m *defaultRoutesManager) update(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (map[string]*appmeshsdk.RouteData, error) {
	sdkRouteRefs, err := m.listSDKRouteRefs(ctx, ms, vr)
	if err != nil {
		return nil, err
	}
	return m.reconcile(ctx, ms, vr, vnByRefHash, vr.Spec.Routes, sdkRouteRefs)
}

func (m *defaultRoutesManager) cleanup(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter) error {
	sdkRouteRefs, err := m.listSDKRouteRefs(ctx, ms, vr)
	if err != nil {
		return err
	}
	_, err = m.reconcile(ctx, ms, vr, nil, nil, sdkRouteRefs)
	return err
}

// reconcile will make AppMesh routes(sdkRouteRefs) matches routes.
func (m *defaultRoutesManager) reconcile(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode,
	routes []appmesh.Route, sdkRouteRefs []*appmeshsdk.RouteRef) (map[string]*appmeshsdk.RouteData, error) {

	matchedRouteAndSDKRouteRefs, unmatchedRoutes, unmatchedSDKRouteRefs := matchRoutesAgainstSDKRouteRefs(routes, sdkRouteRefs)
	sdkRouteByName := make(map[string]*appmeshsdk.RouteData)

	for _, route := range unmatchedRoutes {
		sdkRoute, err := m.createSDKRoute(ctx, ms, vr, route, vnByRefHash)
		if err != nil {
			return nil, err
		}
		sdkRouteByName[aws.StringValue(route.Name)] = sdkRoute
	}

	for _, routeAndSDKRouteRef := range matchedRouteAndSDKRouteRefs {
		route := routeAndSDKRouteRef.route
		sdkRouteRef := routeAndSDKRouteRef.sdkRouteRef
		sdkRoute, err := m.findSDKRoute(ctx, sdkRouteRef)
		if err != nil {
			return nil, err
		}
		if sdkRoute == nil {
			return nil, errors.Errorf("route not found: %v", aws.StringValue(sdkRouteRef.RouteName))
		}
		sdkRoute, err = m.updateSDKRoute(ctx, sdkRoute, vr, route, vnByRefHash)
		if err != nil {
			return nil, err
		}
		sdkRouteByName[aws.StringValue(route.Name)] = sdkRoute
	}

	for _, sdkRouteRef := range unmatchedSDKRouteRefs {
		sdkRoute, err := m.findSDKRoute(ctx, sdkRouteRef)
		if err != nil {
			return nil, err
		}
		if sdkRoute == nil {
			return nil, errors.Errorf("route not found: %v", aws.StringValue(sdkRouteRef.RouteName))
		}
		if err = m.deleteSDKRoute(ctx, sdkRoute); err != nil {
			return nil, err
		}
	}
	return sdkRouteByName, nil
}

func (m *defaultRoutesManager) listSDKRouteRefs(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter) ([]*appmeshsdk.RouteRef, error) {
	var sdkRouteRefs []*appmeshsdk.RouteRef
	if err := m.appMeshSDK.ListRoutesPagesWithContext(ctx, &appmeshsdk.ListRoutesInput{
		MeshName:          ms.Spec.AWSName,
		MeshOwner:         ms.Spec.MeshOwner,
		VirtualRouterName: vr.Spec.AWSName,
	}, func(output *appmeshsdk.ListRoutesOutput, b bool) bool {
		sdkRouteRefs = append(sdkRouteRefs, output.Routes...)
		return true
	}); err != nil {
		return nil, err
	}
	return sdkRouteRefs, nil
}

func (m *defaultRoutesManager) findSDKRoute(ctx context.Context, sdkRouteRef *appmeshsdk.RouteRef) (*appmeshsdk.RouteData, error) {
	resp, err := m.appMeshSDK.DescribeRouteWithContext(ctx, &appmeshsdk.DescribeRouteInput{
		MeshName:          sdkRouteRef.MeshName,
		MeshOwner:         sdkRouteRef.MeshOwner,
		VirtualRouterName: sdkRouteRef.VirtualRouterName,
		RouteName:         sdkRouteRef.RouteName,
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "NotFoundException" {
			return nil, nil
		}
		return nil, err
	}
	return resp.Route, nil
}

func (m *defaultRoutesManager) createSDKRoute(ctx context.Context, ms *appmesh.Mesh, vr *appmesh.VirtualRouter, route appmesh.Route, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (*appmeshsdk.RouteData, error) {
	sdkRouteSpec, err := buildSDKRouteSpec(route, vnByRefHash)
	if err != nil {
		return nil, err
	}
	resp, err := m.appMeshSDK.CreateRouteWithContext(ctx, &appmeshsdk.CreateRouteInput{
		MeshName:          ms.Spec.AWSName,
		MeshOwner:         ms.Spec.MeshOwner,
		VirtualRouterName: vr.Spec.AWSName,
		RouteName:         route.Name,
		Spec:              sdkRouteSpec,
		Tags:              nil,
	})
	if err != nil {
		return nil, err
	}
	return resp.Route, nil
}

func (m *defaultRoutesManager) updateSDKRoute(ctx context.Context, sdkRoute *appmeshsdk.RouteData, vr *appmesh.VirtualRouter, route appmesh.Route, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (*appmeshsdk.RouteData, error) {
	actualSDKRouteSpec := sdkRoute.Spec
	desiredSDKRouteSpec, err := buildSDKRouteSpec(route, vnByRefHash)
	if err != nil {
		return nil, err
	}

	// If an optional field is not set, AWS will provide default settings that will be in actualSDKRouteSpec.
	// We use IgnoreLeftHandUnset when doing the equality check here to allow AWS sever-side to provide default settings.
	opts := equality.IgnoreLeftHandUnset()
	if cmp.Equal(desiredSDKRouteSpec, actualSDKRouteSpec, opts) {
		return sdkRoute, nil
	}
	diff := cmp.Diff(desiredSDKRouteSpec, actualSDKRouteSpec, opts)
	m.log.V(2).Info("routeSpec changed",
		"virtualRouter", k8s.NamespacedName(vr),
		"route", route.Name,
		"actualSDKRouteSpec", actualSDKRouteSpec,
		"desiredSDKRouteSpec", desiredSDKRouteSpec,
		"diff", diff,
	)
	resp, err := m.appMeshSDK.UpdateRouteWithContext(ctx, &appmeshsdk.UpdateRouteInput{
		MeshName:          sdkRoute.MeshName,
		MeshOwner:         sdkRoute.Metadata.MeshOwner,
		VirtualRouterName: sdkRoute.VirtualRouterName,
		RouteName:         sdkRoute.RouteName,
		Spec:              desiredSDKRouteSpec,
	})
	if err != nil {
		return nil, err
	}
	return resp.Route, nil
}

func (m *defaultRoutesManager) deleteSDKRoute(ctx context.Context, sdkRoute *appmeshsdk.RouteData) error {
	_, err := m.appMeshSDK.DeleteRouteWithContext(ctx, &appmeshsdk.DeleteRouteInput{
		MeshName:          sdkRoute.MeshName,
		MeshOwner:         sdkRoute.Metadata.MeshOwner,
		VirtualRouterName: sdkRoute.VirtualRouterName,
		RouteName:         sdkRoute.RouteName,
	})
	if err != nil {
		return err
	}
	return nil
}

type routeAndSDKRouteRef struct {
	route       appmesh.Route
	sdkRouteRef *appmeshsdk.RouteRef
}

// matchRoutesAgainstSDKRouteRefs will match routes against sdkRouteRefs.
// return matched routeAndSDKRouteRef, not matched routes and not matched sdkRouteRefs
func matchRoutesAgainstSDKRouteRefs(routes []appmesh.Route, sdkRouteRefs []*appmeshsdk.RouteRef) ([]routeAndSDKRouteRef, []appmesh.Route, []*appmeshsdk.RouteRef) {
	routeByName := make(map[string]appmesh.Route, len(routes))
	sdkRouteRefByName := make(map[string]*appmeshsdk.RouteRef, len(sdkRouteRefs))
	for _, route := range routes {
		routeByName[aws.StringValue(route.Name)] = route
	}
	for _, sdkRouteRef := range sdkRouteRefs {
		sdkRouteRefByName[aws.StringValue(sdkRouteRef.RouteName)] = sdkRouteRef
	}
	routeNameSet := sets.StringKeySet(routeByName)
	sdkRouteRefNameSet := sets.StringKeySet(sdkRouteRefByName)
	matchedNameSet := routeNameSet.Intersection(sdkRouteRefNameSet)
	unmatchedRouteNameSet := routeNameSet.Difference(sdkRouteRefNameSet)
	unmatchedSDKRouteRefNameSet := sdkRouteRefNameSet.Difference(routeNameSet)

	matchedRouteAndSDKRouteRef := make([]routeAndSDKRouteRef, 0, len(matchedNameSet))
	for name := range matchedNameSet {
		matchedRouteAndSDKRouteRef = append(matchedRouteAndSDKRouteRef, routeAndSDKRouteRef{
			route:       routeByName[name],
			sdkRouteRef: sdkRouteRefByName[name],
		})
	}

	unmatchedRoutes := make([]appmesh.Route, 0, len(unmatchedRouteNameSet))
	for name := range unmatchedRouteNameSet {
		unmatchedRoutes = append(unmatchedRoutes, routeByName[name])
	}

	unmatchedSDKRouteRefs := make([]*appmeshsdk.RouteRef, 0, len(unmatchedSDKRouteRefNameSet))
	for name := range unmatchedSDKRouteRefNameSet {
		unmatchedSDKRouteRefs = append(unmatchedSDKRouteRefs, sdkRouteRefByName[name])
	}

	return matchedRouteAndSDKRouteRef, unmatchedRoutes, unmatchedSDKRouteRefs
}

func buildSDKRouteSpec(route appmesh.Route, vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) (*appmeshsdk.RouteSpec, error) {
	sdkVNRefConvertFunc := buildSDKVirtualNodeReferenceConvertFunc(vnByRefHash)
	converter := conversion.NewConverter(conversion.DefaultNameFunc)
	converter.RegisterUntypedConversionFunc((*appmesh.Route)(nil), (*appmeshsdk.RouteSpec)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return conversions.Convert_CRD_Route_To_SDK_RouteSpec(a.(*appmesh.Route), b.(*appmeshsdk.RouteSpec), scope)
	})
	converter.RegisterUntypedConversionFunc((*appmesh.VirtualNodeReference)(nil), (*string)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return sdkVNRefConvertFunc(a.(*appmesh.VirtualNodeReference), b.(*string), scope)
	})
	sdkRouteSpec := &appmeshsdk.RouteSpec{}
	if err := converter.Convert(&route, sdkRouteSpec, conversion.DestFromSource, nil); err != nil {
		return nil, err
	}
	return sdkRouteSpec, nil
}

// buildSDKVirtualNodeReferenceConvertFunc builds sdkVirtualNodeReferenceConvertFunc by given vn and vnRef mapping.
func buildSDKVirtualNodeReferenceConvertFunc(vnByRefHash map[vnReferenceHash]*appmesh.VirtualNode) sdkVirtualNodeReferenceConvertFunc {
	return func(vnRef *appmesh.VirtualNodeReference, vnAWSName *string, scope conversion.Scope) error {
		vn, ok := vnByRefHash[virtualNodeReferenceToHash(*vnRef)]
		if !ok {
			return errors.Errorf("unexpected VirtualNodeReference: %v", *vnRef)
		}
		*vnAWSName = aws.StringValue(vn.Spec.AWSName)
		return nil
	}
}

// hashKey for virtualNodeReference.
type vnReferenceHash struct {
	namespace string
	name      string
}

// virtualNodeReferenceToHash turns vnRef into a stable hashKey.
func virtualNodeReferenceToHash(vnRef appmesh.VirtualNodeReference) vnReferenceHash {
	return vnReferenceHash{
		namespace: aws.StringValue(vnRef.Namespace),
		name:      vnRef.Name,
	}
}

type sdkVirtualNodeReferenceConvertFunc func(vnRef *appmesh.VirtualNodeReference, vnAWSName *string, scope conversion.Scope) error

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