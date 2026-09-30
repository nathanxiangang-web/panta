// Package runtime owns the explicit process-level acquisition dependency graph.
// It is the only layer importing both application services and PostgreSQL.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nathanxiangang-web/panta/internal/acquisition"
	"github.com/nathanxiangang-web/panta/internal/app"
	"github.com/nathanxiangang-web/panta/internal/catalog"
	"github.com/nathanxiangang-web/panta/internal/integrations/indexcore"
	"github.com/nathanxiangang-web/panta/internal/platform/config"
	"github.com/nathanxiangang-web/panta/internal/projector"
	p115 "github.com/nathanxiangang-web/panta/internal/providers/115"
	"github.com/nathanxiangang-web/panta/internal/providers/contracts"
	"github.com/nathanxiangang-web/panta/internal/providers/session"
	"github.com/nathanxiangang-web/panta/internal/storage"
	"github.com/nathanxiangang-web/panta/internal/store/postgres"
)

var ErrInvalidAcquisitionRuntime = errors.New("invalid acquisition runtime configuration")

// RuntimeSession declares one exact, externally provisioned provider session.
// CredentialRef is an opaque database reference, never credential material.
type RuntimeSession struct {
	ProviderID    contracts.ProviderID
	ConnectionID  storage.ConnectionID
	CredentialRef contracts.CredentialRef
}

// DownloaderFactory is the narrow construction seam for controlled HTTP-backed
// provider tests. Production uses the pinned 115 adapter. Factories must not
// retain or log the supplied credential bytes.
type DownloaderFactory func(context.Context, contracts.ProviderID, []byte) (contracts.DownloaderProvider, error)

// RuntimeDependencies come from a caller-owned trust boundary. In particular,
// neither environment variables nor a static test resolver are used as a
// production credential backend by this composition.
type RuntimeDependencies struct {
	Secrets           contracts.SecretResolver
	Sessions          []RuntimeSession
	HintBaseURL       string
	HintToken         string
	ReadHTTPClient    *http.Client
	HintHTTPClient    *http.Client
	DownloaderFactory DownloaderFactory
	WorkerOptions     []app.WorkerOption
}

// Runtime owns the Panta product pool; injected transports and secret resolvers
// remain caller-owned. Close is idempotent and must follow Run shutdown.
type Runtime struct {
	application *app.Application
	pool        *pgxpool.Pool
	closeOnce   sync.Once
}

func (runtime *Runtime) Run(ctx context.Context) error { return runtime.application.Run(ctx) }
func (runtime *Runtime) Close() {
	if runtime != nil {
		runtime.closeOnce.Do(func() { runtime.pool.Close() })
	}
}

// NewRuntime composes the accepted stores and stage services once. It neither
// migrates a database nor probes 115/IndexCore; only the Panta database is read
// during startup. The returned application owns its pool and closes it on Close.
// A failed composition closes the pool before returning.
func NewRuntime(ctx context.Context, cfg config.Config, deps RuntimeDependencies) (_ *Runtime, err error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate configuration: %w", err)
	}
	if !cfg.AcquisitionWorker.Enabled || strings.TrimSpace(cfg.DatabaseURL) == "" ||
		strings.TrimSpace(cfg.IndexCoreBaseURL) == "" || deps.Secrets == nil ||
		len(deps.Sessions) == 0 || strings.TrimSpace(deps.HintBaseURL) == "" || deps.HintToken == "" {
		return nil, ErrInvalidAcquisitionRuntime
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	parsedReadURL, parseErr := url.Parse(cfg.IndexCoreBaseURL)
	if parseErr != nil || parsedReadURL.User != nil {
		return nil, ErrInvalidAcquisitionRuntime
	}
	if _, testResolver := deps.Secrets.(*session.StaticSecretResolver); testResolver {
		return nil, ErrInvalidAcquisitionRuntime
	}
	readOptions := []indexcore.ClientOption{}
	if deps.ReadHTTPClient != nil {
		readOptions = append(readOptions, indexcore.WithHTTPClient(deps.ReadHTTPClient))
	}
	readOptions = append(readOptions, indexcore.WithTimeout(10*time.Second))
	readClient, err := indexcore.NewClient(cfg.IndexCoreBaseURL, readOptions...)
	if err != nil {
		return nil, fmt.Errorf("configure IndexCore read client: %w", err)
	}
	var hintHTTPClient *http.Client
	if deps.HintHTTPClient != nil {
		clone := *deps.HintHTTPClient
		hintHTTPClient = &clone
	}
	hint, err := app.NewHintPort(app.HintConfig{BaseURL: deps.HintBaseURL, Token: deps.HintToken, HTTPClient: hintHTTPClient})
	if err != nil {
		return nil, fmt.Errorf("configure trusted Hint: %w", err)
	}
	pool, err := postgres.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		// pgx transport errors may echo a credential-bearing DSN. Startup only
		// exposes a closed category; detailed DB diagnosis stays out of logs.
		return nil, fmt.Errorf("%w: Panta database unavailable", ErrInvalidAcquisitionRuntime)
	}
	defer func() {
		if err != nil {
			pool.Close()
		}
	}()
	migrator, err := postgres.NewMigrator(pool)
	if err != nil {
		return nil, err
	}
	status, err := migrator.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("read Panta schema: %w", err)
	}
	if !status.Compatible || status.CurrentVersion != 12 || status.LatestVersion != 12 {
		return nil, postgres.ErrIncompatibleSchema
	}
	jobs, err := postgres.NewJobRepository(pool)
	if err != nil {
		return nil, err
	}
	manifests, err := postgres.NewAcquisitionManifestRepository(pool)
	if err != nil {
		return nil, err
	}
	storageStore, err := postgres.NewStorageRepository(pool)
	if err != nil {
		return nil, err
	}
	tasks, err := postgres.NewAcquisitionProviderTaskRepository(pool)
	if err != nil {
		return nil, err
	}
	outcomeStore, err := postgres.NewProviderOutcomeRepository(pool)
	if err != nil {
		return nil, err
	}
	refreshStore, err := postgres.NewRefreshRepository(pool)
	if err != nil {
		return nil, err
	}
	canonicalStore, err := postgres.NewCanonicalStageRepository(pool)
	if err != nil {
		return nil, err
	}
	projectionStore, err := postgres.NewProjectionStore(pool)
	if err != nil {
		return nil, err
	}
	catalogStore, err := postgres.NewCatalogRepository(pool)
	if err != nil {
		return nil, err
	}

	sessions := session.New()
	registered := make(map[storage.ConnectionID]RuntimeSession, len(deps.Sessions))
	factory := deps.DownloaderFactory
	if factory == nil {
		if cfg.AcquisitionWorker.TickTimeout < 30*time.Second {
			return nil, ErrInvalidAcquisitionRuntime
		}
		factory = new115Downloader
	}
	for _, binding := range deps.Sessions {
		if binding.ProviderID != p115.ProviderID || binding.ConnectionID == "" || binding.CredentialRef == "" ||
			strings.TrimSpace(string(binding.CredentialRef)) != string(binding.CredentialRef) {
			return nil, ErrInvalidAcquisitionRuntime
		}
		if _, exists := registered[binding.ConnectionID]; exists {
			return nil, ErrInvalidAcquisitionRuntime
		}
		connection, lookupErr := storageStore.GetConnection(ctx, binding.ConnectionID)
		if lookupErr != nil || connection.Status != storage.ConnectionStatusActive ||
			connection.ProviderType != string(binding.ProviderID) || connection.CredentialRef == nil ||
			*connection.CredentialRef != string(binding.CredentialRef) {
			return nil, fmt.Errorf("%w: provider connection binding", ErrInvalidAcquisitionRuntime)
		}
		secret, resolveErr := deps.Secrets.ResolveSecret(ctx, binding.CredentialRef)
		if resolveErr != nil || len(secret) == 0 {
			zeroSecret(secret)
			return nil, fmt.Errorf("%w: provider credential unavailable", ErrInvalidAcquisitionRuntime)
		}
		downloader, buildErr := factory(ctx, binding.ProviderID, secret)
		zeroSecret(secret)
		if buildErr != nil || downloader == nil {
			// A factory error may include secret material; never wrap it.
			return nil, fmt.Errorf("%w: provider session construction", ErrInvalidAcquisitionRuntime)
		}
		ref := string(binding.CredentialRef)
		if registerErr := sessions.Register(session.RegistrationRequest{ProviderID: binding.ProviderID,
			ConnectionID: string(binding.ConnectionID), CredentialRef: &ref, Downloader: downloader}); registerErr != nil {
			return nil, fmt.Errorf("%w: duplicate or mismatched provider session", ErrInvalidAcquisitionRuntime)
		}
		registered[binding.ConnectionID] = binding
	}
	activeSessions, err := postgres.ListActiveAcquisitionSessions(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("%w: active session inventory", ErrInvalidAcquisitionRuntime)
	}
	for _, active := range activeSessions {
		registeredSession, found := registered[storage.ConnectionID(active.ConnectionID)]
		if !active.Complete || !found || active.BindingStatus != "ACTIVE" || active.ConnectionStatus != "ACTIVE" ||
			active.ProviderType != string(registeredSession.ProviderID) || active.CredentialRef != string(registeredSession.CredentialRef) {
			return nil, fmt.Errorf("%w: uncovered active acquisition session", ErrInvalidAcquisitionRuntime)
		}
	}

	inputs, err := acquisition.NewExecutionInputResolver(manifests, storageStore)
	if err != nil {
		return nil, err
	}
	execution, err := acquisition.NewExecutionStepService(inputs, manifests, jobs, sessions, tasks)
	if err != nil {
		return nil, err
	}
	outcomes, err := acquisition.NewProviderOutcomeService(outcomeStore)
	if err != nil {
		return nil, err
	}
	visibility, err := acquisition.NewRefreshStep(manifests, jobs, storageStore, hint, refreshStore)
	if err != nil {
		return nil, err
	}
	projectors, err := projector.NewService(storageStore, readClient, projectionStore)
	if err != nil {
		return nil, err
	}
	classifier, err := catalog.NewClassificationService(catalogStore)
	if err != nil {
		return nil, err
	}
	canonical, err := app.NewCanonicalConfirmation(readClient, projectors, catalogStore, classifier,
		manifests, jobs, storageStore, canonicalStore)
	if err != nil {
		return nil, err
	}
	dispatcher, err := app.NewAcquisitionStageDispatcher(jobs, manifests, execution, outcomes, visibility, canonical)
	if err != nil {
		return nil, err
	}
	runner, err := app.NewAcquisitionRunOnce(jobs, jobs, dispatcher, app.RunOnceLimits{
		MinLeaseDuration:      cfg.AcquisitionWorker.TickTimeout + config.AcquisitionLeaseSafetyMargin,
		MaxLeaseDuration:      config.MaxAcquisitionLeaseDuration,
		MaxRecoveryLimit:      cfg.AcquisitionWorker.RecoveryLimit,
		MaxProjectorPageLimit: cfg.AcquisitionWorker.ProjectorPageLimit,
	})
	if err != nil {
		return nil, err
	}
	worker, err := app.NewAcquisitionWorker(runner, cfg.AcquisitionWorker, deps.WorkerOptions...)
	if err != nil {
		return nil, err
	}
	application, err := app.NewWithAcquisitionWorker(cfg, worker)
	if err != nil {
		return nil, err
	}
	return &Runtime{application: application, pool: pool}, nil
}

func new115Downloader(_ context.Context, providerID contracts.ProviderID, secret []byte) (contracts.DownloaderProvider, error) {
	if providerID != p115.ProviderID {
		return nil, ErrInvalidAcquisitionRuntime
	}
	return p115.NewAdapterFromCookie(secret, p115.Options{HTTPTimeout: 5 * time.Second, MaxPages: 3})
}

func zeroSecret(secret []byte) {
	for index := range secret {
		secret[index] = 0
	}
}
