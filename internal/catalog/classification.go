package catalog

import (
	"context"
	"errors"
)

var (
	ErrInvalidClassification         = errors.New("invalid Copy classification request")
	ErrClassificationCopyNotFound    = errors.New("classification Copy not found")
	ErrClassificationVariantNotFound = errors.New("classification Variant not found")
	ErrCopyAlreadyClassified         = errors.New("Copy is already classified to another Variant")
	ErrClassificationPersistence     = errors.New("Copy classification persistence failure")
)

// BindCopyRequest explicitly selects one existing Copy and one existing target
// Variant. Classification never derives or creates either identity.
type BindCopyRequest struct {
	CopyID    CopyID
	VariantID VariantID
}

// BindCopyResult returns the durable Copy after binding. Changed is true only
// for the monotonic NULL -> target Variant transition; same-target replay is a
// successful unchanged result.
type BindCopyResult struct {
	Copy    Copy
	Changed bool
}

// CopyBinder is the atomic persistence boundary for monotonic classification.
// Implementations must fence competing targets and preserve all Copy fields
// except VariantID and normal update metadata.
type CopyBinder interface {
	BindCopyToVariant(context.Context, CopyID, VariantID) (BindCopyResult, error)
}

// ClassificationService owns the explicit application command while depending
// only on the Panta-owned atomic binding port.
type ClassificationService struct {
	binder CopyBinder
}

func NewClassificationService(binder CopyBinder) (*ClassificationService, error) {
	if binder == nil {
		return nil, ErrInvalidClassification
	}
	return &ClassificationService{binder: binder}, nil
}

func (service *ClassificationService) Bind(ctx context.Context, request BindCopyRequest) (BindCopyResult, error) {
	if request.CopyID == "" || request.VariantID == "" {
		return BindCopyResult{}, ErrInvalidClassification
	}
	return service.binder.BindCopyToVariant(ctx, request.CopyID, request.VariantID)
}
