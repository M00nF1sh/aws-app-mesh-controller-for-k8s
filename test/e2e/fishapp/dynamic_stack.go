package fishapp

import (
	"context"
	"encoding/json"
	"fmt"
	appmeshv1beta1 "github.com/aws/aws-app-mesh-controller-for-k8s/pkg/apis/appmesh/v1beta1"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/fishapp/shared"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/framework"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/framework/collection"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/framework/k8s"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/framework/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/servicediscovery"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
	"io/ioutil"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	connectivityCheckRate                  = time.Second / 100
	connectivityCheckProxyPort             = 8899
	connectivityCheckUniformDistributionSL = 0.001 // Significance level that traffic to targets are uniform distributed.
	appMeshOperationRate                   = time.Second * 2
)

// A dynamic generated stack designed to test app mesh integration :D
// Suppose given configuration below:
//		5 VirtualServicesCount
//		10 VirtualNodesCount
//		2 RoutesCountPerVirtualRouter
//		2 TargetsCountPerRoute
//		4 BackendsCountPerVirtualNode
// We will generate virtual service configuration & virtual node configuration follows:
// =======virtual services =========
//	vs1 -> /path1 -> vn1(50)
//				  -> vn2(50)
//		-> /path2 -> vn3(50)
//				  -> vn4(50)
//	vs2 -> /path1 -> vn5(50)
//				  -> vn6(50)
//		-> /path2 -> vn7(50)
//				  -> vn8(50)
//	vs3 -> /path1 -> vn9(50)
//				  -> vn10(50)
//		-> /path2 -> vn1(50)
//				  -> vn2(50)
//	vs4 -> /path1 -> vn3(50)
//				  -> vn4(50)
//		-> /path2 -> vn5(50)
//				  -> vn6(50)
//	vs5 -> /path1 -> vn7(50)
//				  -> vn8(50)
//		-> /path2 -> vn9(50)
//				  -> vn10(50)
// =======virtual nodes =========
//  vn1 -> vs1,vs2,vs3,vs4
//  vn2 -> vs5,vs1,vs2,vs3
//  vn3 -> vs4,vs5,vs1,vs2
//  ...
//
// then we validate each virtual node can access each virtual service at every path, and calculates the target distribution
type DynamicStack struct {
	// service discovery type
	ServiceDiscoveryType shared.ServiceDiscoveryType

	// number of virtual service
	VirtualServicesCount int

	// number of virtual nodes count
	VirtualNodesCount int

	// number of routes per virtual router
	RoutesCountPerVirtualRouter int

	// number of targets per route
	TargetsCountPerRoute int

	// number of backends per virtual node
	BackendsCountPerVirtualNode int

	// number of replicas per virtual node
	ReplicasPerVirtualNode int32

	// how many time to check connectivity per URL
	ConnectivityCheckPerURL int

	// ====== runtime variables ======
	mesh              *appmeshv1beta1.Mesh
	namespace         *corev1.Namespace
	cloudMapNamespace string

	createdNodeVNs  []*appmeshv1beta1.VirtualNode
	createdNodeDPs  []*appsv1.Deployment
	createdNodeSVCs []*corev1.Service

	createdServiceVSs  []*appmeshv1beta1.VirtualService
	createdServiceSVCs []*corev1.Service
}

// one entry of connectivity check result.
type connectivityCheckEntry struct {
	srcVirtualNode    types.NamespacedName
	srcPod            types.NamespacedName
	dstVirtualService types.NamespacedName
	dstURL            string

	retHTTPStatusCode int
	retHTTPBody       string
	retErr            error
}

// expects the stack can be deployed to namespace successfully
func (s *DynamicStack) Deploy(ctx context.Context, f *framework.Framework) {
	s.createMeshAndNamespace(ctx, f)
	if s.ServiceDiscoveryType == shared.CloudMapServiceDiscovery {
		s.createCloudMapNamespace(ctx, f)
	}
	mb := &shared.ManifestBuilder{
		MeshName:             s.mesh.Name,
		Namespace:            s.namespace.Name,
		ServiceDiscoveryType: s.ServiceDiscoveryType,
		CloudMapNamespace:    s.cloudMapNamespace,
	}
	s.createResourcesForNodes(ctx, f, mb)
	s.createResourcesForServices(ctx, f, mb)
	s.grantVirtualNodesBackendAccess(ctx, f)
}

// expects the stack can be cleaned up from namespace successfully
func (s *DynamicStack) Cleanup(ctx context.Context, f *framework.Framework) {
	var deletionErrors []error
	if errs := s.revokeVirtualNodeBackendAccess(ctx, f); len(errs) != 0 {
		deletionErrors = append(deletionErrors, errs...)
	}
	if errs := s.deleteResourcesForServices(ctx, f); len(errs) != 0 {
		deletionErrors = append(deletionErrors, errs...)
	}
	if errs := s.deleteResourcesForNodes(ctx, f); len(errs) != 0 {
		deletionErrors = append(deletionErrors, errs...)
	}
	if s.ServiceDiscoveryType == shared.CloudMapServiceDiscovery {
		if errs := s.deleteCloudMapNamespace(ctx, f); len(errs) != 0 {
			deletionErrors = append(deletionErrors, errs...)
		}
	}
	if errs := s.deleteMeshAndNamespace(ctx, f); len(errs) != 0 {
		deletionErrors = append(deletionErrors, errs...)
	}
	Expect(len(deletionErrors)).To(BeZero())
}

// Check connectivity and routing works correctly
func (s *DynamicStack) Check(ctx context.Context, f *framework.Framework) {
	vsByName := make(map[string]*appmeshv1beta1.VirtualService)
	for i := 0; i != s.VirtualServicesCount; i++ {
		vs := s.createdServiceVSs[i]
		vsByName[vs.Name] = vs
	}

	var checkErrors []error
	httpRetByPodRequest := make(map[string]map[string]map[string]int) // pod, url, serviceNode, count
	for i := 0; i != s.VirtualNodesCount; i++ {
		dp := s.createdNodeDPs[i]
		vn := s.createdNodeVNs[i]
		var vsList []*appmeshv1beta1.VirtualService
		for _, backend := range vn.Spec.Backends {
			vsList = append(vsList, vsByName[backend.VirtualService.VirtualServiceName])
		}
		checkEntriesRet := s.checkDeploymentVirtualServiceConnectivity(ctx, f, vn, dp, vsList)
		for _, checkEntry := range checkEntriesRet {
			if checkEntry.retErr != nil {
				checkErrors = append(checkErrors, errors.Wrapf(checkEntry.retErr,
					"expect HTTP call from pod %s to %s succeed",
					checkEntry.srcPod.String(), checkEntry.dstURL))
				continue
			}
			if checkEntry.retHTTPStatusCode != http.StatusOK {
				checkErrors = append(checkErrors, errors.Errorf(
					"expect HTTP call from pod %s to %s be to have status_code 200, got %v",
					checkEntry.srcPod.String(), checkEntry.dstURL, checkEntry.retHTTPStatusCode))
				continue
			}
			podKey := checkEntry.srcPod.String()
			if _, ok := httpRetByPodRequest[podKey]; !ok {
				httpRetByPodRequest[podKey] = make(map[string]map[string]int)
			}
			if _, ok := httpRetByPodRequest[podKey][checkEntry.dstURL]; !ok {
				httpRetByPodRequest[podKey][checkEntry.dstURL] = make(map[string]int)
			}
			httpRetByPodRequest[podKey][checkEntry.dstURL][checkEntry.retHTTPBody] += 1
		}
	}

	prettyJSON, err := json.MarshalIndent(httpRetByPodRequest, "", "    ")
	Expect(err).NotTo(HaveOccurred())
	utils.Logf("%v", string(prettyJSON))

	uniformDist := distuv.ChiSquared{K: float64(s.TargetsCountPerRoute - 1)}
	var expectedHTTPRetCounts []float64
	for i := 0; i != s.TargetsCountPerRoute; i++ {
		expectedHTTPRetCounts = append(expectedHTTPRetCounts, float64(s.ConnectivityCheckPerURL)/float64(s.TargetsCountPerRoute))
	}
	for pod, httpRetCountMap := range httpRetByPodRequest {
		for url, retCountMap := range httpRetCountMap {
			var actualHTTPRetCounts []float64
			for _, count := range retCountMap {
				actualHTTPRetCounts = append(actualHTTPRetCounts, float64(count))
			}
			chiSqStatics := stat.ChiSquare(actualHTTPRetCounts, expectedHTTPRetCounts)
			pv := 1 - uniformDist.CDF(chiSqStatics)
			if pv < connectivityCheckUniformDistributionSL {
				utils.Logf(
					"expect HTTP call from pod %s to %s be uniform distributed under significance level %v, got pValue: %v",
					pod, url, connectivityCheckUniformDistributionSL, pv)
			}
		}
	}

	for _, err := range checkErrors {
		utils.Logf("check error: %v", err)
	}

	if s.ServiceDiscoveryType != shared.CloudMapServiceDiscovery {
		Expect(len(checkErrors)).To(BeZero())
	}
}

func (s *DynamicStack) createMeshAndNamespace(ctx context.Context, f *framework.Framework) {
	By("create a mesh", func() {
		meshName := fmt.Sprintf("%s-%s", f.Options.ClusterName, utils.RandomDNS1123Label(6))
		mesh, err := f.K8sMeshClient.AppmeshV1beta1().Meshes().Create(&appmeshv1beta1.Mesh{
			ObjectMeta: metav1.ObjectMeta{
				Name: meshName,
			},
			Spec: appmeshv1beta1.MeshSpec{},
		})
		Expect(err).NotTo(HaveOccurred())
		s.mesh = mesh
	})

	By(fmt.Sprintf("wait for mesh %s become active", s.mesh.Name), func() {
		mesh, err := f.MeshManager.WaitUntilMeshActive(ctx, s.mesh)
		Expect(err).NotTo(HaveOccurred())
		s.mesh = mesh
	})

	By("allocates a namespace", func() {
		namespace, err := f.NSManager.AllocateNamespace(ctx, "appmesh")
		Expect(err).NotTo(HaveOccurred())
		s.namespace = namespace
	})

	By("label namespace with appMesh inject", func() {
		namespace := s.namespace.DeepCopy()
		namespace.Labels = collection.MergeStringMap(s.namespace.Labels, map[string]string{
			"appmesh.k8s.aws/sidecarInjectorWebhook": "enabled",
		})
		patch, err := k8s.CreateStrategicTwoWayMergePatch(s.namespace, namespace, corev1.Namespace{})
		Expect(err).NotTo(HaveOccurred())
		namespace, err = f.K8sClient.CoreV1().Namespaces().Patch(namespace.Name, types.StrategicMergePatchType, patch)
		Expect(err).NotTo(HaveOccurred())
		s.namespace = namespace
	})
}

func (s *DynamicStack) deleteMeshAndNamespace(ctx context.Context, f *framework.Framework) []error {
	var deletionErrors []error
	if s.namespace != nil {
		By(fmt.Sprintf("delete namespace: %s", s.namespace.Name), func() {
			foregroundDeletion := metav1.DeletePropagationForeground
			if err := f.K8sClient.CoreV1().Namespaces().Delete(s.namespace.Name, &metav1.DeleteOptions{
				GracePeriodSeconds: aws.Int64(0),
				PropagationPolicy:  &foregroundDeletion,
			}); err != nil {
				utils.Logf("failed to delete namespace: %s due to %v", s.namespace.Name, err)
				deletionErrors = append(deletionErrors, err)
			}
		})
	}
	if s.mesh != nil {
		By(fmt.Sprintf("delete mesh %s", s.mesh.Name), func() {
			foregroundDeletion := metav1.DeletePropagationForeground
			if err := f.K8sMeshClient.AppmeshV1beta1().Meshes().Delete(s.mesh.Name, &metav1.DeleteOptions{
				GracePeriodSeconds: aws.Int64(0),
				PropagationPolicy:  &foregroundDeletion,
			}); err != nil {
				utils.Logf("failed to delete mesh: %s due to %v", s.mesh.Name, err)
				deletionErrors = append(deletionErrors, err)
			}
		})
	}
	return deletionErrors
}

func (s *DynamicStack) createCloudMapNamespace(ctx context.Context, f *framework.Framework) {
	cmNamespace := fmt.Sprintf("%s-%s", f.Options.ClusterName, utils.RandomDNS1123Label(6))
	By(fmt.Sprintf("create cloudMap namespace %s", cmNamespace), func() {
		resp, err := f.SDClient.CreatePrivateDnsNamespaceWithContext(ctx, &servicediscovery.CreatePrivateDnsNamespaceInput{
			Name: aws.String(cmNamespace),
			Vpc:  aws.String(f.Options.AWSVPCID),
		})
		Expect(err).NotTo(HaveOccurred())
		s.cloudMapNamespace = cmNamespace
		utils.Logf("created cloudMap namespace %s with operationID %s", cmNamespace, aws.StringValue(resp.OperationId))
	})
}

func (s *DynamicStack) deleteCloudMapNamespace(ctx context.Context, f *framework.Framework) []error {
	var deletionErrors []error
	if s.cloudMapNamespace != "" {
		By(fmt.Sprintf("delete cloudMap namespace %s", s.cloudMapNamespace), func() {
			var cmNamespaceID string
			f.SDClient.ListNamespacesPagesWithContext(ctx, &servicediscovery.ListNamespacesInput{}, func(output *servicediscovery.ListNamespacesOutput, b bool) bool {
				for _, ns := range output.Namespaces {
					if aws.StringValue(ns.Name) == s.cloudMapNamespace {
						cmNamespaceID = aws.StringValue(ns.Id)
						return true
					}
				}
				return false
			})
			if cmNamespaceID == "" {
				err := errors.Errorf("cannot find cloudMap namespace with name %s", s.cloudMapNamespace)
				utils.Logf("failed to delete cloudMap namespace: %s due to %v", s.cloudMapNamespace, err)
				deletionErrors = append(deletionErrors, err)
				return
			}

			// hummm, let's fix the controller bug in test cases first xD:
			// https://github.com/aws/aws-app-mesh-controller-for-k8s/issues/107
			// https://github.com/aws/aws-app-mesh-controller-for-k8s/issues/131
			By(fmt.Sprintf("[bug workaround] clean up resources in cloudMap namespace %s", s.cloudMapNamespace), func() {
				// give controller a break to deregister instance xD
				time.Sleep(1 * time.Minute)
				var cmServiceIDs []string
				f.SDClient.ListServicesPagesWithContext(ctx, &servicediscovery.ListServicesInput{
					Filters: []*servicediscovery.ServiceFilter{
						{
							Condition: aws.String(servicediscovery.FilterConditionEq),
							Name:      aws.String("NAMESPACE_ID"),
							Values:    aws.StringSlice([]string{cmNamespaceID}),
						},
					},
				}, func(output *servicediscovery.ListServicesOutput, b bool) bool {
					for _, svc := range output.Services {
						cmServiceIDs = append(cmServiceIDs, aws.StringValue(svc.Id))
					}
					return false
				})
				for _, cmServiceID := range cmServiceIDs {
					var cmInstanceIDs []string
					f.SDClient.ListInstancesPagesWithContext(ctx, &servicediscovery.ListInstancesInput{
						ServiceId: aws.String(cmServiceID),
					}, func(output *servicediscovery.ListInstancesOutput, b bool) bool {
						for _, ins := range output.Instances {
							cmInstanceIDs = append(cmInstanceIDs, aws.StringValue(ins.Id))
						}
						return false
					})

					for _, cmInstanceID := range cmInstanceIDs {
						if _, err := f.SDClient.DeregisterInstanceWithContext(ctx, &servicediscovery.DeregisterInstanceInput{
							ServiceId:  aws.String(cmServiceID),
							InstanceId: aws.String(cmInstanceID),
						}); err != nil {
							utils.Logf("failed to deregister instance due to %v", err)
							deletionErrors = append(deletionErrors, err)
						}
					}
					time.Sleep(30 * time.Second)

					if _, err := f.SDClient.DeleteServiceWithContext(ctx, &servicediscovery.DeleteServiceInput{
						Id: aws.String(cmServiceID),
					}); err != nil {
						utils.Logf("failed to deregister instance due to %v", err)
						deletionErrors = append(deletionErrors, err)
					}
				}
			})
			time.Sleep(30 * time.Second)
			if _, err := f.SDClient.DeleteNamespaceWithContext(ctx, &servicediscovery.DeleteNamespaceInput{
				Id: aws.String(cmNamespaceID),
			}); err != nil {
				utils.Logf("failed to delete cloudMap namespace: %s due to %v", s.cloudMapNamespace, err)
				deletionErrors = append(deletionErrors, err)
			}
		})
	}
	return deletionErrors
}

func (s *DynamicStack) createResourcesForNodes(ctx context.Context, f *framework.Framework, mb *shared.ManifestBuilder) {
	throttle := time.Tick(appMeshOperationRate)

	By("create all resources for nodes", func() {
		s.createdNodeVNs = make([]*appmeshv1beta1.VirtualNode, s.VirtualNodesCount)
		s.createdNodeDPs = make([]*appsv1.Deployment, s.VirtualNodesCount)
		s.createdNodeSVCs = make([]*corev1.Service, s.VirtualNodesCount)

		var err error

		for i := 0; i != s.VirtualNodesCount; i++ {
			instanceName := fmt.Sprintf("node-%d", i)
			By(fmt.Sprintf("create VirtualNode for node #%d", i), func() {
				vn := mb.BuildNodeVirtualNode(instanceName, nil)

				<-throttle
				vn, err = f.K8sMeshClient.AppmeshV1beta1().VirtualNodes(s.namespace.Name).Create(vn)
				Expect(err).NotTo(HaveOccurred())
				s.createdNodeVNs[i] = vn
			})

			By(fmt.Sprintf("create Deployment for node #%d", i), func() {
				dp := mb.BuildNodeDeployment(instanceName, s.ReplicasPerVirtualNode)
				dp, err = f.K8sClient.AppsV1().Deployments(s.namespace.Name).Create(dp)
				Expect(err).NotTo(HaveOccurred())
				s.createdNodeDPs[i] = dp
			})

			By(fmt.Sprintf("create Service for node #%d", i), func() {
				svc := mb.BuildNodeService(instanceName)
				svc, err := f.K8sClient.CoreV1().Services(s.namespace.Name).Create(svc)
				Expect(err).NotTo(HaveOccurred())
				s.createdNodeSVCs[i] = svc
			})
		}

		By("wait all VirtualNodes become active", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i := 0; i != s.VirtualNodesCount; i++ {
				wg.Add(1)
				go func(nodeIndex int) {
					defer wg.Done()
					dp, err := f.VNManager.WaitUntilVirtualNodeActive(ctx, s.createdNodeVNs[nodeIndex])
					if err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
					s.createdNodeVNs[nodeIndex] = dp
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all VirtualNodes become active due to errors: %v", waitErrors)
			}
		})

		By("wait all deployments become ready", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i := 0; i != s.VirtualNodesCount; i++ {
				wg.Add(1)
				go func(nodeIndex int) {
					defer wg.Done()
					dp, err := f.DPManager.WaitUntilDeploymentReady(ctx, s.createdNodeDPs[nodeIndex])
					if err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
					s.createdNodeDPs[nodeIndex] = dp
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all deployments become ready due to errors: %v", waitErrors)
			}
		})
	})
}

func (s *DynamicStack) deleteResourcesForNodes(ctx context.Context, f *framework.Framework) []error {
	throttle := time.Tick(appMeshOperationRate)

	var deletionErrors []error
	By("delete all resources for nodes", func() {
		for i, svc := range s.createdNodeSVCs {
			if svc == nil {
				continue
			}
			By(fmt.Sprintf("delete Service for node #%d", i), func() {
				if err := f.K8sClient.CoreV1().Services(svc.Namespace).Delete(svc.Name, &metav1.DeleteOptions{}); err != nil {
					utils.Logf("failed to delete Service: %s/%s due to %v", svc.Namespace, svc.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}
		for i, dp := range s.createdNodeDPs {
			if dp == nil {
				continue
			}
			By(fmt.Sprintf("delete Deployment for node #%d", i), func() {
				foregroundDeletion := metav1.DeletePropagationForeground
				if err := f.K8sClient.AppsV1().Deployments(dp.Namespace).Delete(dp.Name, &metav1.DeleteOptions{
					GracePeriodSeconds: aws.Int64(0),
					PropagationPolicy:  &foregroundDeletion,
				}); err != nil {
					utils.Logf("failed to delete Deployment: %s/%s due to %v", dp.Namespace, dp.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}
		for i, vn := range s.createdNodeVNs {
			if vn == nil {
				continue
			}
			By(fmt.Sprintf("delete VirtualNode for node #%d", i), func() {
				<-throttle
				if err := f.K8sMeshClient.AppmeshV1beta1().VirtualNodes(vn.Namespace).Delete(vn.Name, &metav1.DeleteOptions{}); err != nil {
					utils.Logf("failed to delete VirtualNode: %s/%s due to %v", vn.Namespace, vn.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}

		By("wait all deployments become deleted", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i, dp := range s.createdNodeDPs {
				if dp == nil {
					continue
				}
				wg.Add(1)
				go func(nodeIndex int) {
					defer wg.Done()
					if err := f.DPManager.WaitUntilDeploymentDeleted(ctx, s.createdNodeDPs[nodeIndex]); err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all deployments become deleted due to errors: %v", waitErrors)
			}
		})

		By("wait all VirtualNodes become deleted", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i, vn := range s.createdNodeVNs {
				if vn == nil {
					continue
				}
				wg.Add(1)
				go func(nodeIndex int) {
					defer wg.Done()
					if err := f.VNManager.WaitUntilVirtualNodeDeleted(ctx, s.createdNodeVNs[nodeIndex]); err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all VirtualNodes become active due to errors: %v", waitErrors)
			}
		})
	})
	return deletionErrors
}

func (s *DynamicStack) createResourcesForServices(ctx context.Context, f *framework.Framework, mb *shared.ManifestBuilder) {
	throttle := time.Tick(appMeshOperationRate)

	By("create all resources for services", func() {
		s.createdServiceVSs = make([]*appmeshv1beta1.VirtualService, s.VirtualServicesCount)
		s.createdServiceSVCs = make([]*corev1.Service, s.VirtualServicesCount)

		var err error
		nextVirtualNodeIndex := 0
		for i := 0; i != s.VirtualServicesCount; i++ {
			instanceName := fmt.Sprintf("service-%d", i)
			By(fmt.Sprintf("create VirtualService for service #%d", i), func() {
				var routeCfgs []shared.RouteToWeightedVirtualNodes
				for routeIndex := 0; routeIndex != s.RoutesCountPerVirtualRouter; routeIndex++ {
					var weightedTargets []shared.WeightedVirtualNode
					for targetIndex := 0; targetIndex != s.TargetsCountPerRoute; targetIndex++ {
						weightedTargets = append(weightedTargets, shared.WeightedVirtualNode{
							VirtualNodeName: s.createdNodeVNs[nextVirtualNodeIndex].Name,
							Weight:          1,
						})
						nextVirtualNodeIndex = (nextVirtualNodeIndex + 1) % s.VirtualNodesCount
					}
					routeCfgs = append(routeCfgs, shared.RouteToWeightedVirtualNodes{
						Path:            fmt.Sprintf("/path-%d", routeIndex),
						WeightedTargets: weightedTargets,
					})
				}
				vs := mb.BuildServiceVirtualService(instanceName, routeCfgs)
				<-throttle
				vs, err := f.K8sMeshClient.AppmeshV1beta1().VirtualServices(s.namespace.Name).Create(vs)
				Expect(err).NotTo(HaveOccurred())
				s.createdServiceVSs[i] = vs
			})

			By(fmt.Sprintf("create Service for service #%d", i), func() {
				svc := mb.BuildServiceService(instanceName)
				svc, err = f.K8sClient.CoreV1().Services(s.namespace.Name).Create(svc)
				Expect(err).NotTo(HaveOccurred())
				s.createdServiceSVCs[i] = svc
			})
		}

		By("wait all VirtualService become active", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i := 0; i != s.VirtualServicesCount; i++ {
				wg.Add(1)
				go func(serviceIndex int) {
					defer wg.Done()
					vs, err := f.VSManager.WaitUntilVirtualServiceActive(ctx, s.createdServiceVSs[serviceIndex])
					if err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
					s.createdServiceVSs[serviceIndex] = vs
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all VirtualService become active due to errors: %v", waitErrors)
			}
		})
	})
}

func (s *DynamicStack) deleteResourcesForServices(ctx context.Context, f *framework.Framework) []error {
	throttle := time.Tick(appMeshOperationRate)

	var deletionErrors []error
	By("delete all resources for services", func() {
		for i, svc := range s.createdServiceSVCs {
			if svc == nil {
				continue
			}

			By(fmt.Sprintf("delete Service for service #%d", i), func() {
				if err := f.K8sClient.CoreV1().Services(svc.Namespace).Delete(svc.Name, &metav1.DeleteOptions{}); err != nil {
					utils.Logf("failed to delete Service: %s/%s due to %v", svc.Namespace, svc.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}
		for i, vs := range s.createdServiceVSs {
			if vs == nil {
				continue
			}
			By(fmt.Sprintf("delete VirtualService for service #%d", i), func() {
				<-throttle
				if err := f.K8sMeshClient.AppmeshV1beta1().VirtualServices(vs.Namespace).Delete(vs.Name, &metav1.DeleteOptions{}); err != nil {
					utils.Logf("failed to delete VirtualService: %s/%s due to %v", vs.Namespace, vs.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}

		By("wait all VirtualService become deleted", func() {
			var waitErrors []string
			waitErrorsMutex := &sync.Mutex{}
			var wg sync.WaitGroup
			for i, vs := range s.createdServiceVSs {
				if vs == nil {
					continue
				}
				wg.Add(1)
				go func(serviceIndex int) {
					defer wg.Done()
					if err := f.VSManager.WaitUntilVirtualServiceDeleted(ctx, s.createdServiceVSs[serviceIndex]); err != nil {
						waitErrorsMutex.Lock()
						waitErrors = append(waitErrors, err.Error())
						waitErrorsMutex.Unlock()
						return
					}
				}(i)
			}
			wg.Wait()
			if len(waitErrors) != 0 {
				utils.Failf("failed to wait all VirtualService become deleted due to errors: %v", waitErrors)
			}
		})
	})
	return deletionErrors
}

func (s *DynamicStack) grantVirtualNodesBackendAccess(ctx context.Context, f *framework.Framework) {
	throttle := time.Tick(appMeshOperationRate)

	By("granting VirtualNodes backend access", func() {
		nextVirtualServiceIndex := 0
		for i, vn := range s.createdNodeVNs {
			if vn == nil {
				continue
			}
			By(fmt.Sprintf("granting VirtualNode backend access for node #%d", i), func() {
				var vnBackends []appmeshv1beta1.Backend
				for backendIndex := 0; backendIndex != s.BackendsCountPerVirtualNode; backendIndex++ {
					vnBackends = append(vnBackends, appmeshv1beta1.Backend{
						VirtualService: appmeshv1beta1.VirtualServiceBackend{
							VirtualServiceName: s.createdServiceVSs[nextVirtualServiceIndex].Name,
						},
					})
					nextVirtualServiceIndex = (nextVirtualServiceIndex + 1) % s.VirtualServicesCount
				}

				vnNew := vn.DeepCopy()
				vnNew.Spec.Backends = vnBackends
				patch, err := k8s.CreateJSONMergePatch(vn, vnNew, appmeshv1beta1.VirtualNode{})
				Expect(err).NotTo(HaveOccurred())
				<-throttle
				vnNew, err = f.K8sMeshClient.AppmeshV1beta1().VirtualNodes(vnNew.Namespace).Patch(vnNew.Name, types.MergePatchType, patch)
				Expect(err).NotTo(HaveOccurred())
				s.createdNodeVNs[i] = vnNew
			})
		}
	})
}

func (s *DynamicStack) revokeVirtualNodeBackendAccess(ctx context.Context, f *framework.Framework) []error {
	throttle := time.Tick(appMeshOperationRate)

	var deletionErrors []error
	By("revoking VirtualNodes backend access", func() {
		for i, vn := range s.createdNodeVNs {
			if vn == nil || len(vn.Spec.Backends) == 0 {
				continue
			}
			By(fmt.Sprintf("revoking VirtualNode backend access for node #%d", i), func() {
				vnNew := vn.DeepCopy()
				vnNew.Spec.Backends = nil
				patch, err := k8s.CreateJSONMergePatch(vn, vnNew, appmeshv1beta1.VirtualNode{})
				if err != nil {
					utils.Logf("failed to revoke VirtualNode backend access : %s/%s due to %v", vn.Namespace, vn.Name, err)
					deletionErrors = append(deletionErrors, err)
					return
				}
				<-throttle
				vnNew, err = f.K8sMeshClient.AppmeshV1beta1().VirtualNodes(vnNew.Namespace).Patch(vnNew.Name, types.MergePatchType, patch)
				if err != nil {
					utils.Logf("failed to revoke VirtualNode backend access : %s/%s due to %v", vn.Namespace, vn.Name, err)
					deletionErrors = append(deletionErrors, err)
				}
			})
		}
	})
	return deletionErrors
}

func (s *DynamicStack) checkDeploymentVirtualServiceConnectivity(ctx context.Context, f *framework.Framework,
	vn *appmeshv1beta1.VirtualNode, dp *appsv1.Deployment, vsList []*appmeshv1beta1.VirtualService) []connectivityCheckEntry {
	sel := labels.Set(dp.Spec.Selector.MatchLabels)
	opts := metav1.ListOptions{LabelSelector: sel.AsSelector().String()}
	pods, err := f.K8sClient.CoreV1().Pods(dp.Namespace).List(opts)
	Expect(err).NotTo(HaveOccurred())
	Expect(len(pods.Items)).NotTo(BeZero())

	var checkEntriesRet []connectivityCheckEntry
	for i := range pods.Items {
		pod := pods.Items[i].DeepCopy()
		By(fmt.Sprintf("check pod %s/%s connectivity to services", pod.Namespace, pod.Name), func() {
			podConnectivityCheckResult := s.checkPodVirtualServiceConnectivity(ctx, f, vn, pod, vsList)
			checkEntriesRet = append(checkEntriesRet, podConnectivityCheckResult...)
		})
	}
	return checkEntriesRet
}

func (s *DynamicStack) checkPodVirtualServiceConnectivity(ctx context.Context, f *framework.Framework,
	vn *appmeshv1beta1.VirtualNode, pod *corev1.Pod, vsList []*appmeshv1beta1.VirtualService) []connectivityCheckEntry {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var checkEntries []connectivityCheckEntry
	for _, vs := range vsList {
		for _, route := range vs.Spec.Routes {
			path := route.Http.Match.Prefix
			checkEntries = append(checkEntries, connectivityCheckEntry{
				srcVirtualNode:    k8s.NamespacedName(vn),
				srcPod:            k8s.NamespacedName(pod),
				dstVirtualService: k8s.NamespacedName(vs),
				dstURL:            fmt.Sprintf("http://%s:%d%s", vs.Name, shared.AppContainerPort, path),
			})
		}
	}

	pfErrChan := make(chan error)
	pfReadyChan := make(chan struct{})
	portForwarder, err := k8s.NewPortForwarder(ctx, f.RestCfg, pod, []string{fmt.Sprintf("%d:%d", connectivityCheckProxyPort, shared.HttpProxyContainerPort)}, pfReadyChan)
	Expect(err).NotTo(HaveOccurred())
	go func() {
		pfErrChan <- portForwarder.ForwardPorts()
	}()
	proxyURL, err := url.Parse(fmt.Sprintf("http://localhost:%d", connectivityCheckProxyPort))
	Expect(err).NotTo(HaveOccurred())
	proxyClient := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}

	checkEntriesRetChan := make(chan connectivityCheckEntry)
	go func() {
		var wg sync.WaitGroup
		throttle := time.Tick(connectivityCheckRate)
		for _, entry := range checkEntries {
			for i := 0; i != s.ConnectivityCheckPerURL; i++ {
				<-throttle
				wg.Add(1)
				go func(entry connectivityCheckEntry) {
					defer wg.Done()
					<-pfReadyChan
					resp, err := proxyClient.Get(entry.dstURL)
					if err != nil {
						entry.retErr = err
						checkEntriesRetChan <- entry
						return
					}
					entry.retHTTPStatusCode = resp.StatusCode
					body, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						entry.retErr = err
						checkEntriesRetChan <- entry
						return
					}
					entry.retHTTPBody = string(body)
					checkEntriesRetChan <- entry
				}(entry)
			}
		}
		wg.Wait()
		close(checkEntriesRetChan)
	}()

	var checkEntriesRet []connectivityCheckEntry
	for ret := range checkEntriesRetChan {
		checkEntriesRet = append(checkEntriesRet, ret)
	}
	cancel()
	Expect(<-pfErrChan).To(BeNil())
	return checkEntriesRet
}
