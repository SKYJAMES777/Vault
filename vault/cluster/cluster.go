package cluster

import (
	"context"
	"fmt"
	"sync"
)

// NotificationType represents the type of cluster notification.
type NotificationType int

const (
	// NotificationTypeSnapshotRestore indicates that a snapshot restore occurred.
	NotificationTypeSnapshotRestore NotificationType = iota
)

// Notification represents a cluster-wide notification.
type Notification struct {
	Type NotificationType
	Data interface{}
}

// Cluster handles inter-node communication.
type Cluster struct {
	// ... existing fields ...

	// notificationCh is a channel for sending notifications to standby nodes
	notificationCh chan *Notification

	// logger is the cluster logger
	logger log.Logger
}

// NewCluster creates a new Cluster instance.
func NewCluster(conf *ClusterConfig) (*Cluster, error) {
	// ... existing initialization ...

	c := &Cluster{
		// ... existing fields ...
		notificationCh: make(chan *Notification, 100),
		logger: conf.Logger,
	}

	// Start the notification handler
	go c.handleNotifications()

	return c, nil
}

// SendNotification sends a notification to all standby nodes.
func (c *Cluster) SendNotification(ctx context.Context, notification *Notification) error {
	select {
	case c.notificationCh <- notification:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return fmt.Errorf("notification channel full")
	}
}

// handleNotifications processes incoming notifications and applies them to standby nodes.
func (c *Cluster) handleNotifications() {
	for notification := range c.notificationCh {
		switch notification.Type {
		case NotificationTypeSnapshotRestore:
			c.logger.Info("received snapshot restore notification, invalidating caches")
			// Invalidate local caches on standby nodes
			c.invalidateLocalCaches()
		default:
			c.logger.Warn("unknown notification type", "type", notification.Type)
		}
	}
}

// invalidateLocalCaches clears the in-memory caches on standby nodes.
func (c *Cluster) invalidateLocalCaches() {
	// ... implementation to clear local caches ...
	// This would typically involve clearing the mount table and auth accessor map
	// on the standby node's core instance
}

// ... rest of existing code ...
