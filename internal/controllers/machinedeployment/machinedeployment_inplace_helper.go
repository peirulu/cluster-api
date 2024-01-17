package machinedeployment

import (
	"context"

	"github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/internal/controllers/machinedeployment/mdutil"
)

// getAllMachineSetsAndSyncRevision returns all the machine sets for the provided deployment (new and all old).
// This function uses v1.12.1's rollout planner to properly compute desired state for all MachineSets,
// which includes propagating in-place mutable fields, updating revision annotations, and creating new
// MachineSets if needed.
func (r *Reconciler) getAllMachineSetsAndSyncRevision(ctx context.Context, md *clusterv1.MachineDeployment, msList []*clusterv1.MachineSet, createIfNotExisted, templateExists bool) (*clusterv1.MachineSet, []*clusterv1.MachineSet, error) {
	// Use the v1.12.1 rollout planner to:
	// 1. Find newMS and oldMSs
	// 2. Compute desired state with in-place mutable fields propagated
	// 3. Handle revision annotations
	// 4. Create newMS if needed
	planner := newRolloutPlanner(r.Client, r.RuntimeClient, r.canUpdateMachineSetCache)
	if err := planner.init(ctx, md, msList, nil, createIfNotExisted, templateExists); err != nil {
		return nil, nil, err
	}

	// If no new MachineSet was found/created, return early
	// This can happen when createIfNotExisted is false
	if planner.newMS == nil {
		return nil, planner.oldMSs, nil
	}

	// Apply the planner's computed state to MachineSets
	// This will:
	// - Create new MachineSet if it doesn't exist
	// - Update existing MachineSets to propagate in-place mutable fields
	// - Sync revision annotations
	if err := r.createOrUpdateMachineSetsAndSyncMachineDeploymentRevision(ctx, planner); err != nil {
		return nil, nil, err
	}

	return planner.newMS, planner.oldMSs, nil
}

// reconcileNewMachineSet handles reconciliation of the new MachineSet for in-place upgrade strategy.
// It scales the new MachineSet up if needed.
func (r *Reconciler) reconcileNewMachineSet(ctx context.Context, allMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet, deployment *clusterv1.MachineDeployment) error {
	if deployment.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineDeployment %v is nil, this is unexpected", client.ObjectKeyFromObject(deployment))
	}

	if newMS.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected", client.ObjectKeyFromObject(newMS))
	}

	if *(newMS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// Scaling not required.
		return nil
	}

	if *(newMS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// Scale down.
		return r.scaleMachineSet(ctx, newMS, *(deployment.Spec.Replicas), deployment)
	}

	// v1.12.1's NewMSNewReplicas now returns 3 values: (replicas, reason, error)
	newReplicasCount, _, err := mdutil.NewMSNewReplicas(deployment, allMSs, *newMS.Spec.Replicas)
	if err != nil {
		return err
	}
	return r.scaleMachineSet(ctx, newMS, newReplicasCount, deployment)
}

// reconcileOldMachineSets handles reconciliation of old MachineSets for in-place upgrade strategy.
// It scales down old MachineSets if the new MachineSet is ready.
func (r *Reconciler) reconcileOldMachineSets(ctx context.Context, allMSs []*clusterv1.MachineSet, oldMSs []*clusterv1.MachineSet, newMS *clusterv1.MachineSet, deployment *clusterv1.MachineDeployment) error {
	if deployment.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineDeployment %v is nil, this is unexpected",
			client.ObjectKeyFromObject(deployment))
	}

	if newMS.Spec.Replicas == nil {
		return errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected",
			client.ObjectKeyFromObject(newMS))
	}

	oldMachinesCount := mdutil.GetReplicaCountForMachineSets(oldMSs)
	if oldMachinesCount == 0 {
		// Can't scale down further
		return nil
	}

	// For in-place upgrades, we can scale down old MachineSets once the new MachineSet
	// has the desired number of replicas and they are available.
	// This is simpler than the rolling update strategy because we're not creating new machines,
	// just updating existing ones in place.
	if *(newMS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// New MachineSet has desired replicas, scale down old MachineSets
		for _, oldMS := range oldMSs {
			if oldMS.Spec.Replicas == nil {
				return errors.Errorf("spec.replicas for MachineSet %v is nil, this is unexpected",
					client.ObjectKeyFromObject(oldMS))
			}
			if *(oldMS.Spec.Replicas) == 0 {
				continue
			}
			if err := r.scaleMachineSet(ctx, oldMS, 0, deployment); err != nil {
				return err
			}
		}
	}

	return nil
}
