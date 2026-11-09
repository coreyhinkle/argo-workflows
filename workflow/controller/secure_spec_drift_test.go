package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-workflows/v4/config"
	wfv1 "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/argoproj/argo-workflows/v4/util/logging"
)

const secureDriftTemplate = `
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: secure-drift
  namespace: default
spec:
  entrypoint: main
  templates:
  - name: main
    steps:
    - - name: print
        template: print-message
  - name: print-message
    script:
      image: busybox
      command: [sh]
      source: |
        echo "about to sleep"
        sleep 60
        echo "done sleeping"
`

const secureDriftWorkflow = `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: secure-drift
  namespace: default
spec:
  workflowTemplateRef:
    name: secure-drift
`

const secureDaemonTemplate = `
apiVersion: argoproj.io/v1alpha1
kind: WorkflowTemplate
metadata:
  name: secure-daemon
  namespace: default
spec:
  entrypoint: main
  templates:
  - name: main
    steps:
    - - name: server
        template: server
    - - name: client
        template: client
  - name: server
    daemon: true
    container:
      image: busybox
      command: [sleep, "999999"]
  - name: client
    container:
      image: busybox
      command: [echo, hello]
`

const secureDaemonWorkflow = `
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: secure-daemon
  namespace: default
spec:
  workflowTemplateRef:
    name: secure-daemon
`

const (
	secureDriftMessage   = "WorkflowSpec may not change during execution when the controller is set `templateReferencing: Secure`"
	secureDriftPodNode   = "secure-drift[0].print"
	secureDaemonNode     = "secure-daemon[0].server"
	secureDriftEditedArg = "edited"
)

// runUntilPodRunning operates wf until podNode's pod is running and its incomplete task result has been seen.
func runUntilPodRunning(ctx context.Context, t *testing.T, controller *WorkflowController, wf *wfv1.Workflow, podNode string, with ...with) *wfOperationCtx {
	t.Helper()
	woc := r4Operate(t, ctx, controller, wf)
	require.Equal(t, wfv1.WorkflowRunning, woc.wf.Status.Phase)
	r4MoveNewPodsPending(ctx, t, woc)
	r4SetPodsPhase(t, ctx, woc, apiv1.PodRunning, r4PodForNode(podNode), append(with, r4IncompleteTaskResult(ctx, t))...)
	woc = r4Operate(t, ctx, controller, woc.wf)
	node, err := woc.wf.GetNodeByName(podNode)
	require.NoError(t, err)
	require.Equal(t, wfv1.NodeRunning, node.Phase)
	return woc
}

// newSecureController starts a controller in Secure mode with wftmpl stored.
func newSecureController(ctx context.Context, t *testing.T, wf *wfv1.Workflow, wftmpl *wfv1.WorkflowTemplate) *WorkflowController {
	t.Helper()
	cancel, controller := newController(ctx, wf, wftmpl)
	t.Cleanup(cancel)
	controller.Config.WorkflowRestrictions = &config.WorkflowRestrictions{TemplateReferencing: config.TemplateReferencingSecure}
	return controller
}

// editTemplate updates the stored WorkflowTemplate and waits for the informer to see it.
func editTemplate(ctx context.Context, t *testing.T, controller *WorkflowController, wftmpl *wfv1.WorkflowTemplate, edit func(*wfv1.WorkflowTemplate)) {
	t.Helper()
	tmpls := controller.wfclientset.ArgoprojV1alpha1().WorkflowTemplates(wftmpl.Namespace)
	stored, err := tmpls.Get(ctx, wftmpl.Name, metav1.GetOptions{})
	require.NoError(t, err)
	edit(stored)
	updated, err := tmpls.Update(ctx, stored, metav1.UpdateOptions{})
	require.NoError(t, err)
	waitForInformer(ctx, controller.wftmplInformer.Informer(), updated, func(obj any) bool {
		return obj.(*wfv1.WorkflowTemplate).ResourceVersion == updated.ResourceVersion
	})
}

// startSecure runs secure-drift in Secure mode until its pod's task result is in progress, then edits the template if edit is set.
func startSecure(t *testing.T, edit bool) (context.Context, *WorkflowController, *wfOperationCtx) {
	t.Helper()
	ctx := logging.TestContext(t.Context())
	wf := wfv1.MustUnmarshalWorkflow(secureDriftWorkflow)
	wftmpl := wfv1.MustUnmarshalWorkflowTemplate(secureDriftTemplate)
	controller := newSecureController(ctx, t, wf, wftmpl)
	woc := runUntilPodRunning(ctx, t, controller, wf, secureDriftPodNode)
	require.True(t, woc.wf.Status.TaskResultsInProgress(), "the pod's task result is in progress")
	if edit {
		editTemplate(ctx, t, controller, wftmpl, func(stored *wfv1.WorkflowTemplate) {
			stored.Spec.Templates[1].Script.Source += "echo \"test\"\n"
		})
	}
	return ctx, controller, woc
}

// operateUntilCompleted reconciles from the stored workflow up to five times.
func operateUntilCompleted(ctx context.Context, t *testing.T, controller *WorkflowController, woc *wfOperationCtx) *wfOperationCtx {
	t.Helper()
	for range 5 {
		woc = r4Operate(t, ctx, controller, woc.wf)
		if woc.wf.Status.Phase.Completed() {
			break
		}
	}
	t.Logf("workflow phase=%q message=%q taskResultsInProgress=%v", woc.wf.Status.Phase, woc.wf.Status.Message, woc.wf.Status.TaskResultsInProgress())
	return woc
}

func assertNodePhase(t *testing.T, woc *wfOperationCtx, nodeName string, phase wfv1.NodePhase) {
	t.Helper()
	node, err := woc.wf.GetNodeByName(nodeName)
	require.NoError(t, err)
	assert.Equal(t, phase, node.Phase, "node %s: %s", nodeName, node.Message)
}

// Control: the same run, with the WorkflowTemplate left alone, succeeds.
func TestSecureSpecUnchangedSucceeds(t *testing.T) {
	ctx, controller, woc := startSecure(t, false)
	woc = r4Operate(t, ctx, controller, woc.wf)
	r4SetPodsPhase(t, ctx, woc, apiv1.PodSucceeded, r4PodForNode(secureDriftPodNode), withExitCode(0))
	r4CompleteTaskResult(ctx, t, woc, secureDriftPodNode)
	woc = operateUntilCompleted(ctx, t, controller, woc)
	assert.Equal(t, wfv1.WorkflowSucceeded, woc.wf.Status.Phase)
}

// A Secure workflow whose template changes while its pod runs must end in Error once the pod reports, not hang.
func TestSecureSpecDriftWhileTaskResultInProgress(t *testing.T) {
	ctx, controller, woc := startSecure(t, true)
	woc = r4Operate(t, ctx, controller, woc.wf)
	assert.False(t, woc.wf.Status.Phase.Completed(), "the error waits for the running pod's task result")

	r4SetPodsPhase(t, ctx, woc, apiv1.PodSucceeded, r4PodForNode(secureDriftPodNode), withExitCode(0))
	r4CompleteTaskResult(ctx, t, woc, secureDriftPodNode)
	woc = operateUntilCompleted(ctx, t, controller, woc)

	assert.Equal(t, wfv1.WorkflowError, woc.wf.Status.Phase)
	assert.Equal(t, secureDriftMessage, woc.wf.Status.Message)
	assertNodePhase(t, woc, secureDriftPodNode, wfv1.NodeSucceeded)
}

// The same when the pod never reports: only pod reconciliation can settle its task result.
func TestSecureSpecDriftWhenPodNeverReports(t *testing.T) {
	t.Setenv("RECENTLY_STARTED_POD_DURATION", "0")
	t.Setenv("RECENTLY_DELETED_POD_DURATION", "0")
	t.Setenv("TASK_RESULT_TIMEOUT_DURATION", "0")

	t.Run("PodDeleted", func(t *testing.T) {
		ctx, controller, woc := startSecure(t, true)
		woc = r4Operate(t, ctx, controller, woc.wf)
		deletePods(ctx, woc)
		woc = operateUntilCompleted(ctx, t, controller, woc)

		assert.Equal(t, wfv1.WorkflowError, woc.wf.Status.Phase)
		assert.Equal(t, secureDriftMessage, woc.wf.Status.Message)
		assertNodePhase(t, woc, secureDriftPodNode, wfv1.NodeError)
	})

	t.Run("PodEvicted", func(t *testing.T) {
		ctx, controller, woc := startSecure(t, true)
		woc = r4Operate(t, ctx, controller, woc.wf)
		r4SetPodsPhase(t, ctx, woc, apiv1.PodFailed, r4PodForNode(secureDriftPodNode), func(pod *apiv1.Pod, _ *wfOperationCtx) {
			pod.Status.Reason = "Evicted"
			pod.Status.ContainerStatuses = nil
		})
		woc = operateUntilCompleted(ctx, t, controller, woc)

		assert.Equal(t, wfv1.WorkflowError, woc.wf.Status.Phase)
		assert.Equal(t, secureDriftMessage, woc.wf.Status.Message)
		assertNodePhase(t, woc, secureDriftPodNode, wfv1.NodeFailed)
	})
}

// A daemon under a steps boundary never ends on its own, so the drift error must stop it rather than wait for it.
// The controller doesn't track a daemon node's task result, so only the running daemon holds the error back.
func TestSecureSpecDriftStopsNestedDaemon(t *testing.T) {
	ctx := logging.TestContext(t.Context())
	wf := wfv1.MustUnmarshalWorkflow(secureDaemonWorkflow)
	wftmpl := wfv1.MustUnmarshalWorkflowTemplate(secureDaemonTemplate)
	controller := newSecureController(ctx, t, wf, wftmpl)
	woc := runUntilPodRunning(ctx, t, controller, wf, secureDaemonNode, r4WithReady)
	server, err := woc.wf.GetNodeByName(secureDaemonNode)
	require.NoError(t, err)
	require.True(t, server.IsDaemoned(), "the server is running as a daemon")
	require.NotEmpty(t, server.BoundaryID, "the daemon belongs to the steps boundary, not the workflow root")

	editTemplate(ctx, t, controller, wftmpl, func(stored *wfv1.WorkflowTemplate) {
		stored.Spec.Templates[2].Container.Args = []string{secureDriftEditedArg}
	})
	woc = operateUntilCompleted(ctx, t, controller, woc)

	assert.Equal(t, wfv1.WorkflowError, woc.wf.Status.Phase)
	assert.Equal(t, secureDriftMessage, woc.wf.Status.Message)
	server, err = woc.wf.GetNodeByName(secureDaemonNode)
	require.NoError(t, err)
	assert.False(t, server.IsDaemoned(), "the drift error stops the daemon")
}
