package acquisition

import (
	"context"
	"errors"

	"github.com/nathanxiangang-web/panta/internal/jobs"
)

// ErrLeaseRecoveryDebt marks an expired acquisition pair that requires explicit
// operator diagnosis; no half-transition is allowed for that pair.
var ErrLeaseRecoveryDebt = errors.New("acquisition lease recovery debt")

// MaxExpiredLeaseRecoveryBatch bounds one explicit recovery invocation.
const MaxExpiredLeaseRecoveryBatch = 100

// ExpiredLeaseRecoveryStore is the one-shot recovery capability. It does not
// schedule work or call a provider.
type ExpiredLeaseRecoveryStore interface {
	MarkExpiredAcquisitionRecoveryRequired(context.Context, jobs.RecoveryRequest) (int64, error)
}
