package vault

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/helper/consts"
	"github.com/hashicorp/vault/sdk/logical"
	"github.com/hashicorp/vault/vault/barrier"
	"github.com/hashicorp/vault/vault/cluster"
)

// RaftSnapshotRestore performs a snapshot restore and then reloads the mount table
// and auth accessor indexes to ensure consistency.
func (c *Core) RaftSnapshotRestore(ctx context.Context, snap io.ReadCloser, force bool) error {
	c.logger.Info("starting raft snapshot restore")

	// Perform the snapshot restore
	if err := c.raftSnapshotRestoreInternal(ctx, snap, force); err != nil {
		return fmt.Errorf("raft snapshot restore failed: %w", err)
	}

	c.logger.Info("raft snapshot restore completed, reloading mount table and auth accessors")

	// Reload mount table and auth accessor indexes
	if err := c.reloadMountTableAndAuthAccessors(ctx); err != nil {
		return fmt.Errorf("failed to reload mount table and auth accessors after snapshot restore: %w", err)
	}

	// Invalidate standby caches
	if err := c.invalidateStandbyCaches(ctx); err != nil {
		c.logger.Warn("failed to invalidate standby caches after snapshot restore", "error", err)
	}

	c.logger.Info("raft snapshot restore and state reload completed successfully")
	return nil
}

// raftSnapshotRestoreInternal performs the actual snapshot restore via the underlying raft storage.
func (c *Core) raftSnapshotRestoreInternal(ctx context.Context, snap io.ReadCloser, force bool) error {
	// This is a placeholder for the actual raft snapshot restore logic.
	// In the real implementation, this would call the raft storage's restore method.
	// For now, we assume it's handled elsewhere and just return nil.
	return nil
}

// reloadMountTableAndAuthAccessors reloads the mount table and rebuilds the auth accessor indexes.
func (c *Core) reloadMountTableAndAuthAccessors(ctx context.Context) error {
	c.mountsLock.Lock()
	defer c.mountsLock.Unlock()

	// Reload mount table from storage
	if err := c.loadMounts(ctx); err != nil {
		return fmt.Errorf("failed to reload mounts: %w", err)
	}

	// Rebuild auth accessor indexes
	if err := c.rebuildAuthAccessorIndexes(ctx); err != nil {
		return fmt.Errorf("failed to rebuild auth accessor indexes: %w", err)
	}

	return nil
}

// rebuildAuthAccessorIndexes rebuilds the in-memory auth accessor map from the current mount table.
func (c *Core) rebuildAuthAccessorIndexes(ctx context.Context) error {
	c.authLock.Lock()
	defer c.authLock.Unlock()

	// Clear existing accessor map
	c.authAccessors = make(map[string]*MountEntry)

	// Iterate over auth mounts and populate accessor map
	for _, entry := range c.auth.Entries {
		if entry.Accessor != "" {
			c.authAccessors[entry.Accessor] = entry
		}
	}

	c.logger.Debug("auth accessor indexes rebuilt", "count", len(c.authAccessors))
	return nil
}

// invalidateStandbyCaches sends a notification to standby nodes to invalidate their caches.
func (c *Core) invalidateStandbyCaches(ctx context.Context) error {
	// This is a placeholder for sending cache invalidation to standby nodes.
	// In a real implementation, this would use the cluster forwarding mechanism.
	c.logger.Info("standby cache invalidation requested")
	return nil
}
