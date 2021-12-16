/*
Copyright 2020 The Flux authors

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
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/go-logr/logr"
	helmgetter "helm.sh/helm/v3/pkg/getter"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/fluxcd/pkg/apis/meta"
	"github.com/fluxcd/pkg/runtime/conditions"
	helper "github.com/fluxcd/pkg/runtime/controller"
	"github.com/fluxcd/pkg/runtime/events"
	"github.com/fluxcd/pkg/runtime/patch"
	"github.com/fluxcd/pkg/runtime/predicates"

	sourcev1 "github.com/fluxcd/source-controller/api/v1beta1"
	"github.com/fluxcd/source-controller/internal/helm/getter"
	"github.com/fluxcd/source-controller/internal/helm/repository"
)

type reconcileResult int

const (
	// resultEmpty indicates a reconcile result which does not requeue.
	resultEmpty reconcileResult = iota
	// resultRequeue indicates a reconcile result which should immediately
	// requeue.
	resultRequeue
	// resultSuccess indicates a reconcile result which should be
	// requeued on the interval as defined on the reconciled object.
	resultSuccess
)

// buildReconcileResult is a helper function to convert a given reconcileResult
// and error into the return values of a Reconcile function.
func buildReconcileResult(obj *sourcev1.HelmRepository, rr reconcileResult, err error) (ctrl.Result, error) {
	// NOTE: The return values can be modified based on the error type.
	// For example, if an error signifies a short requeue period that's
	// not equal to the requeue period of the object, the error can be checked
	// and an appropriate result with the period can be returned.
	//
	// Example:
	//  if e, ok := err.(*waitError); ok {
	//	  return ctrl.Result{RequeueAfter: shortWaitDuration}, err
	//  }

	switch rr {
	case resultRequeue:
		return ctrl.Result{Requeue: true}, err
	case resultSuccess:
		return ctrl.Result{RequeueAfter: obj.GetRequeueAfter()}, err
	default:
		return ctrl.Result{}, err
	}
}

// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=source.toolkit.fluxcd.io,resources=helmrepositories/finalizers,verbs=get;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// HelmRepositoryReconciler reconciles a HelmRepository object
type HelmRepositoryReconciler struct {
	client.Client
	helper.Events
	helper.Metrics

	Getters helmgetter.Providers
	Storage *Storage
}

type HelmRepositoryReconcilerOptions struct {
	MaxConcurrentReconciles int
}

// helmRepoReconcilerFunc is the function type for all the helm repository
// reconciler functions.
type helmRepoReconcilerFunc func(ctx context.Context, obj *sourcev1.HelmRepository, artifact *sourcev1.Artifact, repo *repository.ChartRepository) (reconcileResult, error)

func (r *HelmRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return r.SetupWithManagerAndOptions(mgr, HelmRepositoryReconcilerOptions{})
}

func (r *HelmRepositoryReconciler) SetupWithManagerAndOptions(mgr ctrl.Manager, opts HelmRepositoryReconcilerOptions) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sourcev1.HelmRepository{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, predicates.ReconcileRequestedPredicate{})).
		WithOptions(controller.Options{MaxConcurrentReconciles: opts.MaxConcurrentReconciles}).
		Complete(r)
}

func (r *HelmRepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
	start := time.Now()
	log := logr.FromContext(ctx)

	// Fetch the HelmRepository
	obj := &sourcev1.HelmRepository{}
	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Record suspended status metric
	r.RecordSuspend(ctx, obj, obj.Spec.Suspend)

	// Return early if the object is suspended
	if obj.Spec.Suspend {
		log.Info("Reconciliation is suspended for this object")
		return ctrl.Result{}, nil
	}

	// Initialize the patch helper
	patchHelper, err := patch.NewHelper(obj, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Result of the sub-reconciliation.
	var recResult reconcileResult

	// Always attempt to patch the object after each reconciliation.
	// NOTE: This deferred block only modifies the named return error. The
	// result from the reconciliation remain the same. Any requeue attributes
	// set in the result will continue to be effective.
	defer func() {
		retErr = r.summarizeAndPatch(ctx, obj, patchHelper, recResult, retErr)

		// Always record readiness and duration metrics
		r.Metrics.RecordReadiness(ctx, obj)
		r.Metrics.RecordDuration(ctx, obj, start)
	}()

	// Add finalizer first if not exist to avoid the race condition
	// between init and delete
	if !controllerutil.ContainsFinalizer(obj, sourcev1.SourceFinalizer) {
		controllerutil.AddFinalizer(obj, sourcev1.SourceFinalizer)
		return ctrl.Result{Requeue: true}, nil
	}

	// Examine if the object is under deletion
	if !obj.ObjectMeta.DeletionTimestamp.IsZero() {
		res, err := r.reconcileDelete(ctx, obj)
		return buildReconcileResult(obj, res, err)
	}

	// Reconcile actual object
	reconcilers := []helmRepoReconcilerFunc{
		r.reconcileStorage,
		r.reconcileSource,
		r.reconcileArtifact,
	}
	recResult, err = r.reconcile(ctx, obj, reconcilers)
	return buildReconcileResult(obj, recResult, err)
}

// summarizeAndPatch analyzes the object conditions to create a summary of the
// status conditions and patches the object with the calculated summary. The
// reconciler error type is also used to determine the conditions and the
// returned error.
func (r *HelmRepositoryReconciler) summarizeAndPatch(ctx context.Context, obj *sourcev1.HelmRepository, patchHelper *patch.Helper, result reconcileResult, reconcileErr error) error {
	// Remove reconciling condition on successful reconciliation.
	if reconcileErr == nil && result == resultSuccess {
		conditions.Delete(obj, meta.ReconcilingCondition)
	}

	// Record the value of the reconciliation request, if any
	if v, ok := meta.ReconcileAnnotationValue(obj.GetAnnotations()); ok {
		obj.Status.SetLastHandledReconcileRequest(v)
	}

	// Summarize Ready condition
	conditions.SetSummary(obj,
		meta.ReadyCondition,
		conditions.WithConditions(
			sourcev1.FetchFailedCondition,
			sourcev1.ArtifactOutdatedCondition,
			sourcev1.ArtifactUnavailableCondition,
			meta.ReconcilingCondition,
		),
		conditions.WithNegativePolarityConditions(
			sourcev1.FetchFailedCondition,
			sourcev1.ArtifactOutdatedCondition,
			sourcev1.ArtifactUnavailableCondition,
			meta.ReconcilingCondition,
		),
	)

	// Patch the object, ignoring conflicts on the conditions owned by this controller
	patchOpts := []patch.Option{
		patch.WithOwnedConditions{
			Conditions: []string{
				sourcev1.FetchFailedCondition,
				sourcev1.ArtifactOutdatedCondition,
				sourcev1.ArtifactUnavailableCondition,
				meta.ReadyCondition,
				meta.ReconcilingCondition,
				meta.StalledCondition,
			},
		},
	}

	// Analyze the reconcile error.
	switch t := reconcileErr.(type) {
	case *stallingError:
		// The current generation has been reconciled successfully and it has
		// resulted in a stalled state. Return no error to stop further
		// requeuing.
		patchOpts = append(patchOpts, patch.WithStatusObservedGeneration{})
		conditions.MarkFalse(obj, meta.ReadyCondition, t.Reason, t.Error())
		conditions.MarkStalled(obj, t.Reason, t.Error())
		reconcileErr = nil
	case nil:
		// The reconcile didn't result in any error, we are not in stalled
		// state. If a requeue is requested, the current generation has not been
		// reconciled successfully.
		if result != resultRequeue {
			patchOpts = append(patchOpts, patch.WithStatusObservedGeneration{})
		}
		conditions.Delete(obj, meta.StalledCondition)
	default:
		// The reconcile resulted in some error, but we are no longer in stalled
		// state.
		conditions.Delete(obj, meta.StalledCondition)
	}

	// Finally, patch the resource
	if err := patchHelper.Patch(ctx, obj, patchOpts...); err != nil {
		// Ignore patch error "not found" when the object is being deleted.
		if !obj.ObjectMeta.DeletionTimestamp.IsZero() {
			err = kerrors.FilterOut(err, func(e error) bool { return apierrors.IsNotFound(e) })
		}
		reconcileErr = kerrors.NewAggregate([]error{reconcileErr, err})
	}

	return reconcileErr
}

func (r *HelmRepositoryReconciler) reconcile(ctx context.Context, obj *sourcev1.HelmRepository, reconcilers []helmRepoReconcilerFunc) (reconcileResult, error) {
	var chartRepo repository.ChartRepository
	var artifact sourcev1.Artifact

	// Run the sub-reconcilers and build the result of reconciliation.
	var res reconcileResult
	var resErr error
	for _, rec := range reconcilers {
		recResult, err := rec(ctx, obj, &artifact, &chartRepo)
		// Prioritize requeue request in the result.
		res = lowestRequeuingResult(res, recResult)
		if err != nil {
			resErr = err
			break
		}
	}
	return res, resErr
}

// reconcileStorage ensures the current state of the storage matches the desired and previously observed state.
//
// All artifacts for the resource except for the current one are garbage collected from the storage.
// If the artifact in the Status object of the resource disappeared from storage, it is removed from the object.
// If the object does not have an artifact in its Status object, a v1beta1.ArtifactUnavailableCondition is set.
// If the hostname of any of the URLs on the object do not match the current storage server hostname, they are updated.
//
// The caller should assume a failure if an error is returned, or the Result is zero.
func (r *HelmRepositoryReconciler) reconcileStorage(ctx context.Context, obj *sourcev1.HelmRepository, artifact *sourcev1.Artifact, chartRepo *repository.ChartRepository) (reconcileResult, error) {
	// Garbage collect previous advertised artifact(s) from storage
	_ = r.garbageCollect(ctx, obj)

	// Determine if the advertised artifact is still in storage
	if artifact := obj.GetArtifact(); artifact != nil && !r.Storage.ArtifactExist(*artifact) {
		obj.Status.Artifact = nil
		obj.Status.URL = ""
	}

	// Record that we do not have an artifact
	if obj.GetArtifact() == nil {
		markNegativeCondition(obj, sourcev1.ArtifactUnavailableCondition, "NoArtifact", "No artifact for resource in storage")
		return resultSuccess, nil
	}
	conditions.Delete(obj, sourcev1.ArtifactUnavailableCondition)

	// Always update URLs to ensure hostname is up-to-date
	// TODO(hidde): we may want to send out an event only if we notice the URL has changed
	r.Storage.SetArtifactURL(obj.GetArtifact())
	obj.Status.URL = r.Storage.SetHostname(obj.Status.URL)

	return resultSuccess, nil
}

// reconcileSource ensures the upstream Helm repository can be reached and downloaded out using the declared
// configuration, and stores a new artifact in the storage.
//
// The Helm repository index is downloaded using the defined configuration, and in case of an error during this process
// (including transient errors), it records v1beta1.FetchFailedCondition=True and returns early.
// On a successful write of a new artifact, the artifact in the status of the given object is set, and the symlink in
// the storage is updated to its path.
//
// The caller should assume a failure if an error is returned, or the Result is zero.
func (r *HelmRepositoryReconciler) reconcileSource(ctx context.Context, obj *sourcev1.HelmRepository, artifact *sourcev1.Artifact, chartRepo *repository.ChartRepository) (reconcileResult, error) {
	// Configure Helm client to access repository
	clientOpts := []helmgetter.Option{
		helmgetter.WithTimeout(obj.Spec.Timeout.Duration),
		helmgetter.WithURL(obj.Spec.URL),
		helmgetter.WithPassCredentialsAll(obj.Spec.PassCredentials),
	}

	// Configure any authentication related options
	if obj.Spec.SecretRef != nil {
		// Attempt to retrieve secret
		name := types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      obj.Spec.SecretRef.Name,
		}
		var secret corev1.Secret
		if err := r.Client.Get(ctx, name, &secret); err != nil {
			markNegativeCondition(obj, sourcev1.FetchFailedCondition, sourcev1.AuthenticationFailedReason, "Failed to get secret '%s': %s", name.String(), err.Error())
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.AuthenticationFailedReason,
				"Failed to get secret '%s': %s", name.String(), err.Error())
			return resultEmpty, err
		}

		// Get client options from secret
		tmpDir, err := os.MkdirTemp("", fmt.Sprintf("%s-%s-auth-", obj.Name, obj.Namespace))
		if err != nil {
			markNegativeCondition(obj, sourcev1.FetchFailedCondition, sourcev1.StorageOperationFailedReason, "Failed to create temporary directory for credentials: %s", err.Error())
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.StorageOperationFailedReason,
				"Failed to create temporary directory for credentials: %s", err.Error())
			return resultEmpty, err
		}
		defer os.RemoveAll(tmpDir)

		// Construct actual options
		opts, err := getter.ClientOptionsFromSecret(tmpDir, secret)
		if err != nil {
			markNegativeCondition(obj, sourcev1.FetchFailedCondition, sourcev1.AuthenticationFailedReason, "Failed to configure Helm client with secret data: %s", err)
			r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.AuthenticationFailedReason,
				"Failed to configure Helm client with secret data: %s", err)
			// Return err as the content of the secret may change.
			return resultEmpty, err
		}
		clientOpts = append(clientOpts, opts...)
	}

	// Construct Helm chart repository with options and download index
	newChartRepo, err := repository.NewChartRepository(obj.Spec.URL, "", r.Getters, clientOpts)
	if err != nil {
		switch err.(type) {
		case *url.Error:
			ctrl.LoggerFrom(ctx).Error(err, "invalid Helm repository URL")
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, sourcev1.URLInvalidReason,
				"Invalid Helm repository URL: %s", err.Error())
			return resultEmpty, &stallingError{Err: err, Reason: sourcev1.URLInvalidReason}
		default:
			ctrl.LoggerFrom(ctx).Error(err, "failed to construct Helm client")
			conditions.MarkTrue(obj, sourcev1.FetchFailedCondition, meta.FailedReason,
				"Failed to construct Helm client: %s", err.Error())
			return resultEmpty, &stallingError{Err: err, Reason: meta.FailedReason}
		}
	}
	checksum, err := newChartRepo.CacheIndex()
	if err != nil {
		markNegativeCondition(obj, sourcev1.FetchFailedCondition, meta.FailedReason, "Failed to download Helm repository index: %s", err.Error())
		// Coin flip on transient or persistent error, return error and hope for the best
		return resultEmpty, err
	}
	*chartRepo = *newChartRepo

	// Load the cached repository index to ensure it passes validation.
	if err := chartRepo.LoadFromCache(); err != nil {
		markNegativeCondition(obj, sourcev1.FetchFailedCondition, sourcev1.IndexationFailedReason, "Failed to load Helm repository from cache: %s", err.Error())
		r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.FetchFailedCondition,
			"Failed to load Helm repository from cache: %s", err.Error())
		return resultEmpty, err
	}
	defer chartRepo.Unload()

	// Mark observations about the revision on the object.
	if !obj.GetArtifact().HasRevision(checksum) {
		markNegativeCondition(obj, sourcev1.ArtifactOutdatedCondition, "NewRevision", "New index revision '%s'", checksum)
	}

	conditions.Delete(obj, sourcev1.FetchFailedCondition)

	// Create potential new artifact.
	*artifact = r.Storage.NewArtifactFor(obj.Kind,
		obj.ObjectMeta.GetObjectMeta(),
		chartRepo.Checksum,
		fmt.Sprintf("index-%s.yaml", checksum))

	return resultSuccess, nil
}

func (r *HelmRepositoryReconciler) reconcileArtifact(ctx context.Context, obj *sourcev1.HelmRepository, artifact *sourcev1.Artifact, chartRepo *repository.ChartRepository) (reconcileResult, error) {
	// Always restore the Ready condition in case it got removed due to a transient error.
	defer func() {
		if obj.GetArtifact() != nil {
			conditions.Delete(obj, sourcev1.ArtifactUnavailableCondition)
		}
		if obj.GetArtifact().HasRevision(artifact.Revision) {
			conditions.Delete(obj, sourcev1.ArtifactOutdatedCondition)
			conditions.MarkTrue(obj, meta.ReadyCondition, meta.SucceededReason,
				"Stored artifact for revision '%s'", artifact.Revision)
		}
	}()

	if obj.GetArtifact().HasRevision(artifact.Revision) {
		ctrl.LoggerFrom(ctx).Info(fmt.Sprintf("Already up to date, current revision '%s'", artifact.Revision))
		return resultSuccess, nil
	}

	// Mark reconciling because the current state and the desired states don't
	// match and they have to be reconciled.
	conditions.MarkReconciling(obj, "CreateArtifact", "Creating new artifact")

	// Clear cache at the very end.
	defer chartRepo.RemoveCache()

	// Create artifact dir.
	if err := r.Storage.MkdirAll(*artifact); err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to create artifact directory")
		conditions.MarkReconciling(obj, sourcev1.StorageOperationFailedReason, "failed to create artifact directory")
		return resultEmpty, err
	}

	// Acquire lock.
	unlock, err := r.Storage.Lock(*artifact)
	if err != nil {
		ctrl.LoggerFrom(ctx).Error(err, "failed to acquire lock for artifact")
		conditions.MarkReconciling(obj, sourcev1.StorageOperationFailedReason, "failed to acquire lock for artifact")
		return resultEmpty, err
	}
	defer unlock()

	// Save artifact to storage.
	if err = r.Storage.CopyFromPath(artifact, chartRepo.CachePath); err != nil {
		r.Eventf(ctx, obj, events.EventSeverityError, sourcev1.StorageOperationFailedReason,
			"Unable to save artifact to storage: %s", err)
		conditions.MarkReconciling(obj, sourcev1.StorageOperationFailedReason, "unable to save artifact to storage")
		return resultEmpty, err
	}

	// Record it on the object.
	obj.Status.Artifact = artifact.DeepCopy()

	// Update index symlink.
	indexURL, err := r.Storage.Symlink(*artifact, "index.yaml")
	if err != nil {
		r.Events.Eventf(ctx, obj, events.EventSeverityError, sourcev1.StorageOperationFailedReason,
			"Failed to update status URL symlink: %s", err)
	}

	if indexURL != "" {
		obj.Status.URL = indexURL
	}
	return resultSuccess, nil
}

// reconcileDelete handles the delete of an object. It first garbage collects all artifacts for the object from the
// artifact storage, if successful, the finalizer is removed from the object.
// func (r *HelmRepositoryReconciler) reconcileDelete(ctx context.Context, obj *sourcev1.HelmRepository) (ctrl.Result, error) {
func (r *HelmRepositoryReconciler) reconcileDelete(ctx context.Context, obj *sourcev1.HelmRepository) (reconcileResult, error) {
	// Garbage collect the resource's artifacts
	if err := r.garbageCollect(ctx, obj); err != nil {
		// Return the error so we retry the failed garbage collection
		return resultEmpty, err
	}

	// Remove our finalizer from the list
	controllerutil.RemoveFinalizer(obj, sourcev1.SourceFinalizer)

	// Stop reconciliation as the object is being deleted
	return resultEmpty, nil
}

// garbageCollect performs a garbage collection for the given v1beta1.HelmRepository. It removes all but the current
// artifact except for when the deletion timestamp is set, which will result in the removal of all artifacts for the
// resource.
func (r *HelmRepositoryReconciler) garbageCollect(ctx context.Context, obj *sourcev1.HelmRepository) error {
	if !obj.DeletionTimestamp.IsZero() {
		if err := r.Storage.RemoveAll(r.Storage.NewArtifactFor(obj.Kind, obj.GetObjectMeta(), "", "*")); err != nil {
			r.Eventf(ctx, obj, events.EventSeverityError, "GarbageCollectionFailed",
				"Garbage collection for deleted resource failed: %s", err)
			return err
		}
		obj.Status.Artifact = nil
		// TODO(hidde): we should only push this event if we actually garbage collected something
		r.Eventf(ctx, obj, events.EventSeverityInfo, "GarbageCollectionSucceeded",
			"Garbage collected artifacts for deleted resource")
		return nil
	}
	if obj.GetArtifact() != nil {
		if err := r.Storage.RemoveAllButCurrent(*obj.GetArtifact()); err != nil {
			r.Eventf(ctx, obj, events.EventSeverityError, "GarbageCollectionFailed",
				"Garbage collection of old artifacts failed: %s", err)
			return err
		}
		// TODO(hidde): we should only push this event if we actually garbage collected something
		r.Eventf(ctx, obj, events.EventSeverityInfo, "GarbageCollectionSucceeded",
			"Garbage collected old artifacts")
	}
	return nil
}

// lowestRequeuingResult returns the reconcileResult with the lowest requeue
// period.
// Weightage:
//  resultRequeue - immediate requeue (lowest)
//  resultSuccess - requeue at an interval
//  resultEmpty - no requeue
func lowestRequeuingResult(i, j reconcileResult) reconcileResult {
	switch {
	case i == resultEmpty:
		return j
	case j == resultEmpty:
		return i
	case i == resultRequeue:
		return i
	case j == resultRequeue:
		return j
	default:
		return j
	}
}

// markNegativeCondition marks a negative condition and reconciling condition on
// an object to indicate abnormality and ongoing reconciliation.
func markNegativeCondition(to conditions.Setter, t string, reason, messageFormat string, messageArgs ...interface{}) {
	conditions.MarkTrue(to, t, reason, messageFormat, messageArgs...)
	conditions.MarkReconciling(to, reason, messageFormat, messageArgs...)
}
