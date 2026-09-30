package app

import (
	"github.com/nathanxiangang-web/panta/internal/acquisition"
)

// These assertions keep production composition connected to the accepted stage
// services while the process-level worker remains a later Gate.
var (
	_ acquisition.ProviderStage            = (*acquisition.ExecutionStepService)(nil)
	_ acquisition.ProviderOutcomeCommitter = (*acquisition.ProviderOutcomeService)(nil)
	_ acquisition.VisibilityStage          = (*acquisition.RefreshStep)(nil)
	_ acquisition.CanonicalStage           = (*acquisition.CanonicalConfirmation)(nil)
)

// NewAcquisitionStageDispatcher is an explicit, reusable one-claim production
// composition. The caller supplies an already-claimed Job; this function does not
// create a worker, call ClaimNext, or configure a polling cadence.
func NewAcquisitionStageDispatcher(
	jobs acquisition.JobReader,
	manifests acquisition.ManifestReader,
	provider *acquisition.ExecutionStepService,
	outcomes *acquisition.ProviderOutcomeService,
	visibility *acquisition.RefreshStep,
	canonical *acquisition.CanonicalConfirmation,
) (*acquisition.StageDispatcher, error) {
	if provider == nil || outcomes == nil || visibility == nil || canonical == nil {
		return nil, acquisition.ErrInvalidStageDispatch
	}
	return acquisition.NewStageDispatcher(jobs, manifests, provider, outcomes, visibility, canonical)
}
