package vault

import (
	"context"
	"fmt"
	"sync"
	"time"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/vault/sdk/logical"
	"github.com/hashicorp/vault/vault/cluster"
)

// RaftSnapshotRestore handles the snapshot restore operation and ensures
// that the mount table and auth accessor map are synchronized after restore.
func (c *Core) RaftSnapshotRestore(ctx context.Context, snap io.ReadCloser) error {
	c.logger.Info("starting raft snapshot restore")

	// Perform the actual snapshot restore via the raft backend
	if err := c.raftSnapshotRestore(ctx, snap); err != nil {
		c.logger.Error("raft snapshot restore failed", "error", err)
		return err
	}

	c.logger.Info("raft snapshot restore completed, reloading mount table and auth accessors")

	// Reload the mount table from the storage backend
	if err := c.reloadMountTable(ctx); err != nil {
		c.logger.Error("failed to reload mount table after snapshot restore", "error", err)
		return fmt.Errorf("failed to reload mount table: %w", err)
	}

	// Rebuild the auth accessor map from the reloaded mount table
	if err := c.rebuildAuthAccessorMap(ctx); err != nil {
		c.logger.Error("failed to rebuild auth accessor map after snapshot restore", "error", err)
		return fmt.Errorf("failed to rebuild auth accessor map: %w", err)
	}

	// Invalidate caches on standby nodes via the cluster
	if err := c.invalidateStandbyCaches(ctx); err != nil {
		c.logger.Warn("failed to invalidate standby caches after snapshot restore", "error", err)
	}

	c.logger.Info("mount table and auth accessor map synchronized after raft snapshot restore")
	return nil
}

// reloadMountTable reloads the mount table from the storage backend.
func (c *Core) reloadMountTable(ctx context.Context) error {
	c.mountsLock.Lock()
	defer c.mountsLock.Unlock()

	// Load the mount table from storage
	mountTable, err := c.loadMountTable(ctx)
	if err != nil {
		return err
	}

	// Replace the in-memory mount table
	c.mounts = mountTable
	return nil
}

// rebuildAuthAccessorMap rebuilds the auth accessor map from the current mount table.
func (c *Core) rebuildAuthAccessorMap(ctx context.Context) error {
	c.authAccessorLock.Lock()
	defer c.authAccessorLock.Unlock()

	// Clear the existing accessor map
	c.authAccessorMap = make(map[string]*MountEntry)

	// Iterate over all mount entries and populate the accessor map
	for _, entry := range c.mounts.Entries {
		if entry.Type == MountTypeAuth {
			if entry.Accessor != "" {
				c.authAccessorMap[entry.Accessor] = entry
			}
		}
	}

	return nil
}

// invalidateStandbyCaches sends a notification to standby nodes to invalidate their caches.
func (c *Core) invalidateStandbyCaches(ctx context.Context) error {
	// Use the cluster to send an invalidation request to standby nodes
	if c.cluster != nil {
		// Create a notification payload indicating that a snapshot restore occurred
		notification := &cluster.Notification{
			Type: cluster.NotificationTypeSnapshotRestore,
		}
		if err := c.cluster.SendNotification(ctx, notification); err != nil {
			return fmt.Errorf("failed to send invalidation notification to standby nodes: %w", err)
		}
	}
	return nil
}

// raftSnapshotRestore performs the actual snapshot restore via the raft backend.
// This is a placeholder for the actual implementation.
func (c *Core) raftSnapshotRestore(ctx context.Context, snap io.ReadCloser) error {
	// Actual implementation would call the raft backend's restore function
	// For now, we assume it's handled elsewhere
	return nil
}
