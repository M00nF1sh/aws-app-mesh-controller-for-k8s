package namespace

import (
	"context"
	"fmt"
	"github.com/aws/aws-app-mesh-controller-for-k8s/test/e2e/framework/utils"
	corev1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

type Manager interface {
	AllocateNamespace(ctx context.Context, baseName string) (*corev1.Namespace, error)
}

func NewManager(cs kubernetes.Interface) Manager {
	return &defaultManager{
		cs: cs,
	}
}

type defaultManager struct {
	cs kubernetes.Interface
}

func (m *defaultManager) AllocateNamespace(ctx context.Context, baseName string) (*corev1.Namespace, error) {
	name, err := m.findAvailableNamespaceName(ctx, baseName)
	if err != nil {
		return nil, err
	}
	namespaceObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}

	var namespace *corev1.Namespace
	if err := wait.PollImmediateUntil(utils.PollIntervalShort, func() (bool, error) {
		var err error
		namespace, err = m.cs.CoreV1().Namespaces().Create(namespaceObj)
		if err != nil {
			utils.Logf("Unexpected error while creating namespace: %v", err)
			return false, nil
		}
		return true, nil
	}, ctx.Done()); err != nil {
		return nil, err
	}
	return namespace, nil
}

// findAvailableNamespaceName random namespace name starting with baseName.
func (m *defaultManager) findAvailableNamespaceName(ctx context.Context, baseName string) (string, error) {
	var name string
	err := wait.PollImmediateUntil(utils.PollIntervalShort, func() (bool, error) {
		name = fmt.Sprintf("%v-%v", baseName, utils.RandomDNS1123Label(6))
		_, err := m.cs.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
		if err == nil {
			// Already taken
			return false, nil
		}
		if apierrs.IsNotFound(err) {
			return true, nil
		}
		utils.Logf("Unexpected error while getting namespace: %v", err)
		return false, nil
	}, ctx.Done())

	return name, err
}
