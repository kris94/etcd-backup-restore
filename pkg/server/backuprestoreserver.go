// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/errors"
	"github.com/gardener/etcd-backup-restore/pkg/metrics"
	"github.com/gardener/etcd-backup-restore/pkg/objectstore"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gardener/etcd-backup-restore/pkg/defragmentor"
	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"
	"github.com/gardener/etcd-backup-restore/pkg/initializer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/copier"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/restorer"
	"github.com/gardener/etcd-backup-restore/pkg/snapshot/snapshotter"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	cron "github.com/robfig/cron/v3"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/pkg/types"
)

// BackupRestoreServer holds the details for backup-restore server.
type BackupRestoreServer struct {
	config                  *BackupRestoreComponentConfig
	logger                  *logrus.Entry
	defragmentationSchedule cron.Schedule
}

// NewBackupRestoreServer return new backup restore server.
func NewBackupRestoreServer(logger *logrus.Logger, config *BackupRestoreComponentConfig) (*BackupRestoreServer, error) {
	parsedDefragSchedule, err := cron.ParseStandard(config.DefragmentationSchedule)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		return nil, err
	}
	return &BackupRestoreServer{
		logger:                  logger.WithField("actor", "backup-restore-server"),
		config:                  config,
		defragmentationSchedule: parsedDefragSchedule,
	}, nil
}

// Run starts the backup restore server.
func (b *BackupRestoreServer) Run(ctx context.Context) error {
	clusterURLsMap, err := types.NewURLsMap(b.config.RestorationConfig.InitialCluster)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	peerURLs, err := types.NewURLs(b.config.RestorationConfig.InitialAdvertisePeerURLs)
	if err != nil {
		// Ideally this case should not occur, since this check is done at the config validaitions.
		b.logger.Fatalf("failed creating url map for restore cluster: %v", err)
	}

	options := &restorer.RestoreOptions{
		Config:      b.config.RestorationConfig,
		ClusterURLs: clusterURLsMap,
		PeerURLs:    peerURLs,
	}

	if b.config.SnapstoreConfig == nil || len(b.config.SnapstoreConfig.Provider) == 0 {
		b.logger.Warnf("No snapstore storage provider configured. Will not start backup schedule.")
		b.runServerWithoutSnapshotter(ctx, options)
		return nil
	}
	return b.runServerWithSnapshotter(ctx, options)
}

// startHTTPServer creates and starts the HTTP handler
// with status 503 (Service Unavailable)
func (b *BackupRestoreServer) startHTTPServer(initializer initializer.Initializer, ssr *snapshotter.Snapshotter) *HTTPHandler {
	// Start http handler with Error state and wait till snapshotter is up
	// and running before setting the status to OK.
	handler := &HTTPHandler{
		Port:              b.config.ServerConfig.Port,
		Initializer:       initializer,
		Snapshotter:       ssr,
		Logger:            b.logger,
		StopCh:            make(chan struct{}),
		EnableProfiling:   b.config.ServerConfig.EnableProfiling,
		ReqCh:             make(chan struct{}),
		AckCh:             make(chan struct{}),
		EnableTLS:         (b.config.ServerConfig.TLSCertFile != "" && b.config.ServerConfig.TLSKeyFile != ""),
		ServerTLSCertFile: b.config.ServerConfig.TLSCertFile,
		ServerTLSKeyFile:  b.config.ServerConfig.TLSKeyFile,
	}
	handler.SetStatus(http.StatusServiceUnavailable)
	b.logger.Info("Registering the http request handlers...")
	handler.RegisterHandler()
	b.logger.Info("Starting the http server...")
	go handler.Start()

	return handler
}

// runServerWithoutSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is not configured
func (b *BackupRestoreServer) runServerWithoutSnapshotter(ctx context.Context, restoreOpts *restorer.RestoreOptions) {
	etcdInitializer := initializer.NewInitializer(restoreOpts, nil, b.logger.Logger)

	// If no storage provider is given, snapshotter will be nil, in which
	// case the status is set to OK as soon as etcd probe is successful
	handler := b.startHTTPServer(etcdInitializer, nil)
	defer handler.Stop()

	// start defragmentation without trigerring full snapshot
	// after each successful data defragmentation
	go defragmentor.DefragDataPeriodically(ctx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, nil, b.logger)

	b.runEtcdProbeLoopWithoutSnapshotter(ctx, handler)
}

// runServerWithSnapshotter runs the etcd-backup-restore
// for the case where snapshotter is configured correctly
func (b *BackupRestoreServer) runServerWithSnapshotter(ctx context.Context, restoreOpts *restorer.RestoreOptions) error {
	ackCh := make(chan struct{})

	etcdInitializer := initializer.NewInitializer(restoreOpts, b.config.SnapstoreConfig, b.logger.Logger)

	b.logger.Infof("Creating snapstore from provider: %s", b.config.SnapstoreConfig.Provider)
	ss, err := snapstore.GetSnapstore(b.config.SnapstoreConfig)
	if err != nil {
		return fmt.Errorf("failed to create snapstore from configured storage provider: %v", err)
	}

	b.logger.Infof("Creating snapshotter...")
	ssr, err := snapshotter.NewSnapshotter(b.logger, b.config.SnapshotterConfig, ss, b.config.EtcdConnectionConfig, b.config.CompressionConfig)
	if err != nil {
		return err
	}

	handler := b.startHTTPServer(etcdInitializer, ssr)
	defer handler.Stop()

	ssrStopCh := make(chan struct{})
	go handleSsrStopRequest(ctx, handler, ssr, ackCh, ssrStopCh)
	go handleAckState(handler, ackCh)

	timer := time.NewTimer(30 * time.Second)
	os := objectstore.NewObjectStore(ss, b.logger)
	go b.handleOperationEvents(timer, os, handler)

	go defragmentor.DefragDataPeriodically(ctx, b.config.EtcdConnectionConfig, b.defragmentationSchedule, ssr.TriggerFullSnapshot, b.logger)

	b.runEtcdProbeLoopWithSnapshotter(ctx, os, handler, ssr, ssrStopCh, ackCh)
	return nil
}

// runEtcdProbeLoopWithSnapshotter runs the etcd probe loop
// for the case where snapshotter is configured correctly
func (b *BackupRestoreServer) runEtcdProbeLoopWithSnapshotter(ctx context.Context, os objectstore.ObjectStore, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ssrStopCh chan struct{}, ackCh chan struct{}) {
	var (
		err                       error
		initialDeltaSnapshotTaken bool
	)

	for {
		if b.config.CopyBackups {
			if err := b.handleCopyOption(); err != nil {
				b.logger.Errorf("Copying backups failed: %v", err)
				handler.SetStatus(http.StatusServiceUnavailable)
				continue
			}
			handler.SetStatus(http.StatusOK)
			b.logger.Infof("Sleeping for 30 seconds...")
			time.Sleep(30 * time.Second)
			continue
		}

		b.logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			b.logger.Info("Shutting down...")
			return
		default:
			err = b.probeEtcd(ctx)
		}
		if err != nil {
			b.logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}

		b.logger.Infof("Getting copy operation...")
		obj, copyOp, err := b.getCopyOperation(os)
		if err != nil {
			b.logger.Errorf("Failed to get copy operation: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}
		if copyOp != nil {
			if copyOp.Source {
				b.logger.Infof("Copy operation with owner %s and status %s initiated at %s as source", copyOp.Owner, copyOp.Status, copyOp.Initiated)
				if copyOp.Status == objectstore.OperationStatusInitial {
					handler.SetStatus(http.StatusServiceUnavailable)

					// Take the final full snapshot
					b.logger.Infof("Taking final full snapshot...")
					if _, err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
						b.logger.Errorf("Failed to take final full snapshot: %v", err)
						continue
					}

					// Set copy operation status to Ready
					b.logger.Infof("Setting copy operation status to Ready...")
					copyOp.Status = objectstore.OperationStatusReady
					if err := b.setCopyOperation(os, obj, copyOp); err != nil {
						b.logger.Errorf("Failed to set copy operation status to Ready: %v", err)
						continue
					}
				}
				b.logger.Infof("Shutting down...")
				return
			} else {
				b.logger.Infof("Copy operation with owner %s and status %s initiated at %s as destination", copyOp.Owner, copyOp.Status, copyOp.Initiated)
				b.logger.Infof("Sleeping for 30 seconds...")
				time.Sleep(30 * time.Second)
				continue
			}
		}

		// The decision to either take an initial delta snapshot or
		// or a full snapshot directly is based on whether there has
		// been a previous full snapshot (if not, we assume the etcd
		// to be a fresh etcd) or it has been more than 24 hours since
		// the last full snapshot was taken.
		// If this is not the case, we take a delta snapshot by first
		// collecting all the delta events since the previous snapshot
		// and take a delta snapshot of these (there may be multiple
		// delta snapshots based on the amount of events collected and
		// the delta snapshot memory limit), after which a full snapshot
		// is taken and the regular snapshot schedule comes into effect.

		// TODO: write code to find out if prev full snapshot is older than it is
		// supposed to be, according to the given cron schedule, instead of the
		// hard-coded "24 hours" full snapshot interval

		// Temporary fix for missing alternate full snapshots for Gardener shoots
		// with hibernation schedule set: change value from 24 ot 23.5 to
		// accommodate for slight pod spin-up delays on shoot wake-up
		const recentFullSnapshotPeriodInHours = 23.5
		initialDeltaSnapshotTaken = false
		if ssr.PrevFullSnapshot != nil && time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours {
			ssrStopped, err := ssr.CollectEventsSincePrevSnapshot(ssrStopCh)
			if ssrStopped {
				b.logger.Info("Snapshotter stopped.")
				ackCh <- emptyStruct
				handler.SetStatus(http.StatusServiceUnavailable)
				b.logger.Info("Shutting down...")
				return
			}
			if err == nil {
				if _, err := ssr.TakeDeltaSnapshot(); err != nil {
					b.logger.Warnf("Failed to take first delta snapshot: snapshotter failed with error: %v", err)
					continue
				}
				initialDeltaSnapshotTaken = true
			} else {
				b.logger.Warnf("Failed to collect events for first delta snapshot(s): %v", err)
			}
		}
		if !initialDeltaSnapshotTaken {
			// need to take a full snapshot here
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindDelta}).Set(0)
			metrics.SnapshotRequired.With(prometheus.Labels{metrics.LabelKind: snapstore.SnapshotKindFull}).Set(1)
			if _, err := ssr.TakeFullSnapshotAndResetTimer(); err != nil {
				b.logger.Errorf("Failed to take substitute first full snapshot: %v", err)
				continue
			}
		}

		// set server's healthz endpoint status to OK so that
		// etcd is marked as ready to serve traffic
		handler.SetStatus(http.StatusOK)

		ssr.SsrStateMutex.Lock()
		ssr.SsrState = snapshotter.SnapshotterActive
		ssr.SsrStateMutex.Unlock()
		gcStopCh := make(chan struct{})
		go ssr.RunGarbageCollector(gcStopCh)
		b.logger.Infof("Starting snapshotter...")
		startWithFullSnapshot := ssr.PrevFullSnapshot == nil || !(time.Since(ssr.PrevFullSnapshot.CreatedOn).Hours() <= recentFullSnapshotPeriodInHours)
		if err := ssr.Run(ssrStopCh, startWithFullSnapshot); err != nil {
			if etcdErr, ok := err.(*errors.EtcdError); ok == true {
				b.logger.Errorf("Snapshotter failed with etcd error: %v", etcdErr)
			} else {
				b.logger.Fatalf("Snapshotter failed with error: %v", err)
			}
		}
		b.logger.Infof("Snapshotter stopped.")
		ackCh <- emptyStruct
		handler.SetStatus(http.StatusServiceUnavailable)
		close(gcStopCh)
	}
}

// runEtcdProbeLoopWithoutSnapshotter runs the etcd probe loop
// for the case where snapshotter is not configured
func (b *BackupRestoreServer) runEtcdProbeLoopWithoutSnapshotter(ctx context.Context, handler *HTTPHandler) {
	var err error
	for {
		b.logger.Infof("Probing etcd...")
		select {
		case <-ctx.Done():
			b.logger.Info("Shutting down...")
			return
		default:
			err = b.probeEtcd(ctx)
		}
		if err != nil {
			b.logger.Errorf("Failed to probe etcd: %v", err)
			handler.SetStatus(http.StatusServiceUnavailable)
			continue
		}

		handler.SetStatus(http.StatusOK)
		<-ctx.Done()
		handler.SetStatus(http.StatusServiceUnavailable)
		b.logger.Infof("Received stop signal. Terminating !!")
		return
	}
}

// probeEtcd will make the snapshotter probe for etcd endpoint to be available
// before it starts taking regular snapshots.
func (b *BackupRestoreServer) probeEtcd(ctx context.Context) error {
	client, err := etcdutil.GetTLSClientForEtcd(b.config.EtcdConnectionConfig)
	if err != nil {
		return &errors.EtcdError{
			Message: fmt.Sprintf("failed to create etcd client: %v", err),
		}
	}

	ctx, cancel := context.WithTimeout(ctx, b.config.EtcdConnectionConfig.ConnectionTimeout.Duration)
	defer cancel()
	if _, err := client.Get(ctx, "foo"); err != nil {
		b.logger.Errorf("Failed to connect to client: %v", err)
		return err
	}
	return nil
}

func handleAckState(handler *HTTPHandler, ackCh chan struct{}) {
	for {
		<-ackCh
		if atomic.CompareAndSwapUint32(&handler.AckState, HandlerAckWaiting, HandlerAckDone) {
			handler.AckCh <- emptyStruct
		}
	}
}

// handleSsrStopRequest responds to handlers request and stop interrupt.
func handleSsrStopRequest(ctx context.Context, handler *HTTPHandler, ssr *snapshotter.Snapshotter, ackCh, ssrStopCh chan struct{}) {
	for {
		var ok bool
		select {
		case _, ok = <-handler.ReqCh:
		case _, ok = <-ctx.Done():
		}

		ssr.SsrStateMutex.Lock()
		if ssr.SsrState == snapshotter.SnapshotterActive {
			ssr.SsrStateMutex.Unlock()
			ssrStopCh <- emptyStruct
		} else {
			ssr.SsrState = snapshotter.SnapshotterInactive
			ssr.SsrStateMutex.Unlock()
			ackCh <- emptyStruct
		}
		if !ok {
			return
		}
	}
}

func (b *BackupRestoreServer) handleOperationEvents(timer *time.Timer, os objectstore.ObjectStore, handler *HTTPHandler) {
	for {
		select {
		case <-timer.C:
			b.logger.Infof("Getting copy operation...")
			if _, copyOp, _ := b.getCopyOperation(os); copyOp != nil {
				b.logger.Infof("Copy operation found, stopping snapshotter...")
				atomic.StoreUint32(&handler.AckState, HandlerAckWaiting)
				handler.Logger.Info("Changing handler state...")
				handler.ReqCh <- emptyStruct
				handler.Logger.Info("Waiting for acknowledgment...")
				<-handler.AckCh
			}
			timer.Reset(30 * time.Second)
		}
	}
}

func (b *BackupRestoreServer) getCopyOperation(os objectstore.ObjectStore) (*objectstore.Object, *objectstore.CopyOperation, error) {
	b.logger.Infof("Listing objects...")
	objects, err := os.List()
	if err != nil {
		return nil, nil, err
	}

	if len(objects) == 0 {
		return nil, nil, nil
	}
	if len(objects) > 1 {
		return nil, nil, fmt.Errorf("multiple objects found")
	}
	if objects[0].Kind != objectstore.ObjectKindCopyOperation {
		return nil, nil, fmt.Errorf("found object of kind different from CopyOperation")
	}

	b.logger.Infof("Reading object %v...", objects[0])
	copyOp := &objectstore.CopyOperation{}
	if err := os.Read(objects[0], copyOp); err != nil {
		return nil, nil, err
	}
	return objects[0], copyOp, nil
}

func (b *BackupRestoreServer) setCopyOperation(os objectstore.ObjectStore, obj *objectstore.Object, copyOp *objectstore.CopyOperation) error {
	b.logger.Infof("Writing object %v with contents %v...", obj, copyOp)
	if err := os.Write(obj, copyOp); err != nil {
		return err
	}
	return nil
}

func (b *BackupRestoreServer) handleCopyOption() error {
	sourceSnapStore, destSnapStore, err := copier.GetSourceAndDestinationStores(b.config.CopierConfig, b.config.SnapstoreConfig)
	if err != nil {
		return fmt.Errorf("Could not get source and destintion snapstores: %v", err)
	}
	os := objectstore.NewObjectStore(sourceSnapStore, b.logger)
	obj, copyOp, err := b.getCopyOperation(os)
	if err != nil {
		return fmt.Errorf("Failed to retrieve copy operation: %v", err)
	}

	if copyOp == nil {
		obj, copyOp, err = b.initializeCopyOperation(os, objectstore.OperationStatusInitial)
		if err != nil {
			return fmt.Errorf("Failed to initialize copy operation: %v", err)
		}
		b.logger.Info("Successfully initialized copy operation")
	}

	if copyOp.Status == objectstore.OperationStatusDone {
		b.logger.Info("Operation is already done, nothing to do")
		return nil
	}

	if copyOp.Status == objectstore.OperationStatusInitial {
		b.logger.Info("Waiting for copy operation to become Ready ...")
		timer := time.NewTimer(30)
		obj, copyOp, err = b.waitForReadyCopyOp(timer, os)
		if err != nil {
			return fmt.Errorf("Failed waiting for copy operation to become ready: %v", err)
		}
		b.logger.Info("Copy operation became ready")
	}

	b.logger.Info("Starting to copy backups ...")
	backupCopier := copier.NewCopier(sourceSnapStore, destSnapStore, b.logger)
	if err := backupCopier.Run(); err != nil {
		return fmt.Errorf("Failed to copy backups: %v", err)
	}
	b.logger.Info("Successfully copied backups")

	if obj, copyOp, err = b.updateCopyOperationStatus(os, obj, copyOp, objectstore.OperationStatusDone); err != nil {
		return fmt.Errorf("Failed to set copy operation status to done: %v", err)
	}
	b.logger.Info("Successfully set copy operation to done")

	return nil
}

func (b *BackupRestoreServer) initializeCopyOperation(os objectstore.ObjectStore, status objectstore.OperationStatus) (*objectstore.Object, *objectstore.CopyOperation, error) {
	now := time.Now().UTC()
	copyOp := &objectstore.CopyOperation{
		Source:    true,
		Owner:     "foo",
		Initiated: now,
		Status:    status,
	}
	obj := &objectstore.Object{
		Kind:      objectstore.ObjectKindCopyOperation,
		Name:      "test",
		CreatedOn: now,
	}
	return obj, copyOp, b.setCopyOperation(os, obj, copyOp)
}

func (b *BackupRestoreServer) updateCopyOperationStatus(os objectstore.ObjectStore, obj *objectstore.Object, copyOp *objectstore.CopyOperation, status objectstore.OperationStatus) (*objectstore.Object, *objectstore.CopyOperation, error) {
	copyOp.Status = status
	return obj, copyOp, b.setCopyOperation(os, obj, copyOp)
}

func (b *BackupRestoreServer) waitForReadyCopyOp(timer *time.Timer, os objectstore.ObjectStore) (*objectstore.Object, *objectstore.CopyOperation, error) {
	for {
		select {
		case <-timer.C:
			b.logger.Infof("Getting copy operation...")
			if obj, copyOp, error := b.getCopyOperation(os); copyOp != nil {
				b.logger.Infof("copy operation is %+v", copyOp)
				if copyOp.Status == objectstore.OperationStatusReady {
					return obj, copyOp, error
				}
			}
			timer.Reset(30 * time.Second)
		}
	}
}
