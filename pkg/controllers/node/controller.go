/*
Copyright 2025 The CloudPilot AI Authors.

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

package node

import (
	"context"
	"strings"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	podutil "sigs.k8s.io/karpenter/pkg/utils/pod"
)

type Controller struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
}

const (
	orphanUnregisteredGracePeriod = 2 * time.Minute
	readyDeletionGracePeriod      = 10 * time.Minute
	instanceNamePrefix            = "karpenter-"
)

func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

func (c *Controller) Reconcile(ctx context.Context, node *corev1.Node) (reconcile.Result, error) {
	if hasUnregisteredTaint(node) {
		return c.reconcileUnregistered(ctx, node)
	}

	// Only consider emptiness for nodes that are Ready.
	readyCond, ok := lo.Find(node.Status.Conditions, func(cond corev1.NodeCondition) bool {
		return cond.Type == corev1.NodeReady
	})
	if !ok || readyCond.Status != corev1.ConditionTrue {
		return reconcile.Result{}, nil
	}

	// Require the node to be Ready for a while before checking emptiness.
	if time.Since(readyCond.LastTransitionTime.Time) < 3*time.Minute {
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// check if the node is managed by karpenter
	if !nodeutils.IsManaged(node, c.cloudProvider) {
		return reconcile.Result{}, nil
	}

	// check if the node is empty
	if !c.isEmpty(node) {
		return reconcile.Result{}, nil
	}

	nodePoolName := node.Labels[karpv1.NodePoolLabelKey]
	if nodePoolName == "" {
		return reconcile.Result{}, nil
	}

	nodePool := &karpv1.NodePool{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodePoolName}, nodePool); err != nil {
		if apierrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		return reconcile.Result{Requeue: true}, err
	}

	if nodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		return reconcile.Result{}, nil
	}

	// ensure the node has been Ready for a minimum duration before deleting
	if readyFor := time.Since(readyCond.LastTransitionTime.Time); readyFor < readyDeletionGracePeriod {
		return reconcile.Result{RequeueAfter: 30 * time.Second}, nil
	}

	log.FromContext(ctx).Info("deleting empty node", "node", node.Name)
	// delete the node
	if err := c.kubeClient.Delete(ctx, node); err != nil {
		return reconcile.Result{Requeue: true}, err
	}

	return reconcile.Result{}, nil
}

func (c *Controller) reconcileUnregistered(ctx context.Context, node *corev1.Node) (reconcile.Result, error) {
	if time.Since(node.CreationTimestamp.Time) < orphanUnregisteredGracePeriod {
		return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !strings.HasPrefix(node.Name, instanceNamePrefix) {
		return reconcile.Result{}, nil
	}

	nodeClaimName := strings.TrimPrefix(node.Name, instanceNamePrefix)
	var nodeClaim karpv1.NodeClaim
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: nodeClaimName}, &nodeClaim); err == nil {
		return reconcile.Result{}, nil
	} else if !apierrors.IsNotFound(err) {
		return reconcile.Result{Requeue: true}, err
	}

	if node.Spec.ProviderID != "" {
		orphanClaim := &karpv1.NodeClaim{}
		orphanClaim.Status.ProviderID = node.Spec.ProviderID
		if err := c.cloudProvider.Delete(ctx, orphanClaim); err != nil {
			log.FromContext(ctx).Error(err, "failed to delete orphan instance", "node", node.Name)
		}
	}

	log.FromContext(ctx).Info("deleting orphan unregistered node", "node", node.Name)
	if err := c.kubeClient.Delete(ctx, node); err != nil && !apierrors.IsNotFound(err) {
		return reconcile.Result{Requeue: true}, err
	}

	return reconcile.Result{}, nil
}

func hasUnregisteredTaint(node *corev1.Node) bool {
	return lo.ContainsBy(node.Spec.Taints, func(t corev1.Taint) bool {
		return t.Key == karpv1.UnregisteredTaintKey
	})
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("node.emptiness").
		For(&corev1.Node{}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

var ignorePodsByLabel = map[string]string{
	"k8s-app": "konnectivity-agent",
}

func (c *Controller) isEmpty(node *corev1.Node) bool {
	var pods corev1.PodList
	if err := c.kubeClient.List(context.Background(), &pods, client.InNamespace(node.Namespace), client.MatchingFields{"spec.nodeName": node.Name}); err != nil {
		return false
	}

	filteredPods := lo.Filter(pods.Items, func(pod corev1.Pod, _ int) bool {
		reschedualbe := podutil.IsReschedulable(&pod)
		if !reschedualbe {
			return false
		}

		for key, value := range ignorePodsByLabel {
			if ignore, ok := pod.Labels[key]; ok && ignore == value {
				return false
			}
		}

		return true
	})

	return len(filteredPods) == 0
}
