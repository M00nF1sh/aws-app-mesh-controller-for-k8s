package virtualrouter

import (
	"context"
	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/k8s"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/virtualnode/ref"
	"github.com/go-logr/logr"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
)

func NewEnqueueRequestsForVirtualNodeEvents(k8sClient client.Client, log logr.Logger) *enqueueRequestsForVirtualNodeEvents {
	return &enqueueRequestsForVirtualNodeEvents{
		k8sClient: k8sClient,
		log:       log,
	}
}

var _ handler.EventHandler = (*enqueueRequestsForVirtualNodeEvents)(nil)

type enqueueRequestsForVirtualNodeEvents struct {
	k8sClient client.Client
	log       logr.Logger
}

// Create is called in response to an create event
func (h *enqueueRequestsForVirtualNodeEvents) Create(e event.CreateEvent, queue workqueue.RateLimitingInterface) {
	// no-op
}

// Update is called in response to an update event
func (h *enqueueRequestsForVirtualNodeEvents) Update(e event.UpdateEvent, queue workqueue.RateLimitingInterface) {
	// virtualNode reconcile depends on mesh is active or not.
	// so we only need to trigger virtualNode reconcile if mesh's active status changed.
	vnOld := e.ObjectOld.(*appmesh.VirtualNode)
	vnNew := e.ObjectNew.(*appmesh.VirtualNode)

	if ref.IsVirtualNodeActive(vnOld) != ref.IsVirtualNodeActive(vnNew) {
		h.enqueueVirtualRoutersForVirtualNode(context.Background(), queue, vnNew)
	}
}

// Delete is called in response to a delete event
func (h *enqueueRequestsForVirtualNodeEvents) Delete(e event.DeleteEvent, queue workqueue.RateLimitingInterface) {
	// no-op
}

// Generic is called in response to an event of an unknown type or a synthetic event triggered as a cron or
// external trigger request
func (h *enqueueRequestsForVirtualNodeEvents) Generic(e event.GenericEvent, queue workqueue.RateLimitingInterface) {
	// no-op
}

func (h *enqueueRequestsForVirtualNodeEvents) enqueueVirtualRoutersForVirtualNode(ctx context.Context, queue workqueue.RateLimitingInterface, vn *appmesh.VirtualNode) {
	vnRefIndexKey := buildVirtualNodeReferenceIndexKeyForVN(vn)
	vrList := &appmesh.VirtualRouterList{}
	if err := h.k8sClient.List(ctx, vrList, client.MatchingFields{VirtualNodeReferenceIndex: vnRefIndexKey}); err != nil {
		h.log.Error(err, "failed to enqueue virtualRouters for virtualNode events",
			"virtualNode", k8s.NamespacedName(vn))
		return
	}

	for _, vr := range vrList.Items {
		queue.Add(ctrl.Request{NamespacedName: k8s.NamespacedName(&vr)})
	}
}
