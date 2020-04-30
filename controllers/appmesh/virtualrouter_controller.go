/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/k8s"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/runtime"
	"github.com/aws/aws-app-mesh-controller-for-k8s/pkg/virtualrouter"
	"github.com/go-logr/logr"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/source"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	appmesh "github.com/aws/aws-app-mesh-controller-for-k8s/apis/appmesh/v1beta2"
)

func NewVirtualRouterReconciler(
	k8sClient client.Client,
	fieldIndexer client.FieldIndexer,
	finalizerManager k8s.FinalizerManager,
	vrResourceManager virtualrouter.ResourceManager,
	log logr.Logger) *virtualRouterReconciler {
	return &virtualRouterReconciler{
		k8sClient:         k8sClient,
		fieldIndexer:      fieldIndexer,
		finalizerManager:  finalizerManager,
		vrResourceManager: vrResourceManager,
		log:               log,
	}
}

// virtualRouterReconciler reconciles a VirtualRouter object
type virtualRouterReconciler struct {
	k8sClient         client.Client
	fieldIndexer      client.FieldIndexer
	finalizerManager  k8s.FinalizerManager
	vrResourceManager virtualrouter.ResourceManager

	log logr.Logger
}

// +kubebuilder:rbac:groups=appmesh.k8s.aws,resources=virtualrouters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=appmesh.k8s.aws,resources=virtualrouters/status,verbs=get;update;patch

func (r *virtualRouterReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	return runtime.HandleReconcileError(r.reconcile(req), r.log)
}

func (r *virtualRouterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.fieldIndexer.IndexField(&appmesh.VirtualRouter{}, virtualrouter.VirtualNodeReferenceIndex, func(obj apiruntime.Object) []string {
		return virtualrouter.BuildVirtualNodeReferenceIndexes(obj.(*appmesh.VirtualRouter))
	})
	meshEventsHandler := virtualrouter.NewEnqueueRequestsForMeshEvents(r.k8sClient, r.log)
	virtualNodeEventsHandler := virtualrouter.NewEnqueueRequestsForVirtualNodeEvents(r.k8sClient, r.log)
	return ctrl.NewControllerManagedBy(mgr).
		For(&appmesh.VirtualRouter{}).
		Watches(&source.Kind{Type: &appmesh.Mesh{}}, meshEventsHandler).
		Watches(&source.Kind{Type: &appmesh.VirtualNode{}}, virtualNodeEventsHandler).
		Complete(r)
}

func (r *virtualRouterReconciler) reconcile(req ctrl.Request) error {
	ctx := context.Background()
	vr := &appmesh.VirtualRouter{}
	if err := r.k8sClient.Get(ctx, req.NamespacedName, vr); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !vr.DeletionTimestamp.IsZero() {
		return r.cleanupVirtualRouter(ctx, vr)
	}
	return r.reconcileVirtualRouter(ctx, vr)
}

func (r *virtualRouterReconciler) reconcileVirtualRouter(ctx context.Context, vr *appmesh.VirtualRouter) error {
	if err := r.finalizerManager.AddFinalizers(ctx, vr, k8s.FinalizerAWSAppMeshResources); err != nil {
		return err
	}
	if err := r.vrResourceManager.Reconcile(ctx, vr); err != nil {
		return err
	}
	return nil
}

func (r *virtualRouterReconciler) cleanupVirtualRouter(ctx context.Context, vr *appmesh.VirtualRouter) error {
	if k8s.HasFinalizer(vr, k8s.FinalizerAWSAppMeshResources) {
		if err := r.vrResourceManager.Cleanup(ctx, vr); err != nil {
			return err
		}
		if err := r.finalizerManager.RemoveFinalizers(ctx, vr, k8s.FinalizerAWSAppMeshResources); err != nil {
			return err
		}
	}
	return nil
}
