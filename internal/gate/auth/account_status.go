package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// kvBucketAccounts is owned by spinifex IAM. The gate only attaches to it and never
// creates it, so a node without IAM cannot invent account records.
const kvBucketAccounts = "spinifex-accounts"

// accountActiveTTL bounds how long an ACTIVE answer is trusted when the accounts
// watcher is down. A running watcher evicts the entry on the change itself.
const accountActiveTTL = 5 * time.Second

const accountStatusActive = "ACTIVE"

// ErrAccountNotActive means the account is missing, suspended or terminating.
var ErrAccountNotActive = errors.New("account is not active")

// AccountGate decides whether an account may use the service. A credential provider
// that sources accounts from the IAM directory implements it; static config
// credentials belong to no account and are never asked.
type AccountGate interface {
	// CheckAccountActive returns nil for an ACTIVE account, ErrAccountNotActive for a
	// missing, suspended or terminating one, and any other error when status could not
	// be read. Callers must refuse on every error.
	CheckAccountActive(ctx context.Context, accountID string) error
}

var (
	_ AccountGate = (*NATSIAMProvider)(nil)
	_ AccountGate = (*ChainProvider)(nil)
)

// CheckAccountActive reads the account record, caching only ACTIVE answers.
func (p *NATSIAMProvider) CheckAccountActive(ctx context.Context, accountID string) error {
	if accountID == "" {
		return ErrAccountNotActive
	}

	p.mu.RLock()
	until, hit := p.accountActive[accountID]
	p.mu.RUnlock()
	if hit && time.Now().Before(until) {
		return nil
	}

	p.mu.Lock()
	if err := p.ensureAccountsBucket(ctx); err != nil {
		p.mu.Unlock()
		if errors.Is(err, jetstream.ErrBucketNotFound) || errors.Is(err, jetstream.ErrStreamNotFound) {
			return ErrAccountNotActive
		}
		return fmt.Errorf("account status unavailable: %w", err)
	}
	bucket := p.accountsBucket
	gen := p.cacheGen
	p.mu.Unlock()

	entry, err := bucket.Get(ctx, accountID)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return ErrAccountNotActive
		}
		return fmt.Errorf("read account: %w", err)
	}
	var record struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(entry.Value(), &record); err != nil {
		return fmt.Errorf("decode account: %w", err)
	}
	if record.Status != accountStatusActive {
		return ErrAccountNotActive
	}

	p.mu.Lock()
	// A change that landed during the read has already invalidated this answer.
	if p.cacheGen == gen {
		p.accountActive[accountID] = time.Now().Add(accountActiveTTL)
	}
	p.mu.Unlock()
	return nil
}

// ensureAccountsBucket attaches to the accounts bucket and starts its watcher. The
// caller must hold p.mu.
func (p *NATSIAMProvider) ensureAccountsBucket(ctx context.Context) error {
	if p.accountsReady {
		return nil
	}
	if p.js == nil {
		return errors.New("JetStream context not available")
	}
	bucket, err := p.js.KeyValue(ctx, kvBucketAccounts)
	if err != nil {
		return fmt.Errorf("open accounts bucket: %w", err)
	}
	p.accountsBucket = bucket
	p.accountsReady = true
	p.startWatcher(p.lifetimeCtx(), bucket, kvBucketAccounts, p.invalidateAccountLocked)
	slog.Info("Accounts bucket now available — account status is enforced")
	return nil
}

// invalidateAccountLocked evicts one account's cached ACTIVE answer. The caller must
// hold p.mu.
func (p *NATSIAMProvider) invalidateAccountLocked(accountID string) {
	delete(p.accountActive, accountID)
	p.cacheGen++
}

// CheckAccountActive asks the IAM-backed provider. Config credentials never reach
// here: the gate skips the check for them.
func (p *ChainProvider) CheckAccountActive(ctx context.Context, accountID string) error {
	gate, ok := p.iam.(AccountGate)
	if !ok {
		return nil
	}
	return gate.CheckAccountActive(ctx, accountID)
}
