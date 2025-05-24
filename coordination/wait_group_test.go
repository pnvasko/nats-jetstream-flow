package coordination

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

const (
	testScope = "test_scope"
	longWait  = 5 * time.Second
	shortWait = 50 * time.Millisecond
)

func TestWaitGroup(t *testing.T) {
	setEnvironment(t, "test.WaitGroup")

	tc, err := getJSHelpers(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tc.cancel()

	t.Run("NewWaitGroup", func(t *testing.T) {
		t.Run("NewWaitGroupDefaults", func(t *testing.T) {
			dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger)
			require.NoError(t, err)
			require.NotNil(t, dwg)
			require.Equal(t, defaultWaitGroupScope, dwg.Scope())
			require.Equal(t, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, defaultWaitGroupScope), dwg.Bucket())
		})

		t.Run("NewWaitGroupOptions", func(t *testing.T) {
			scopeI := uniqueScope(testScope)
			var optsSet []StoreOption[*WaitGroup]
			optsSet = append(optsSet, SetValue(func(wg *WaitGroup) *string { return &wg.scope }, scopeI))

			dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, optsSet...)
			require.NoError(t, err)
			require.NotNil(t, dwg)
			require.Equal(t, scopeI, dwg.Scope())
			require.Equal(t, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scopeI), dwg.Bucket())

			scopeII := uniqueScope(testScope)
			var opts []StoreOption[*WaitGroup]
			opts = append(opts, WithScope[*WaitGroup](scopeII))
			dwg, err = NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
			require.NoError(t, err)
			require.NotNil(t, dwg)
			require.Equal(t, scopeII, dwg.Scope())

			var optsI []StoreOption[*WaitGroup]
			optsI = append(optsI, WithScope[*WaitGroup]("test2"))
			optsI = append(optsI, WithCleanupTTL[*WaitGroup](20*time.Millisecond))
			dwg, err = NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, optsI...)
			require.Error(t, err)

		})
	})

	t.Run("WaitGroupBasic", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "add-done-wait-basic"

		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)

		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.NoError(t, err)
		}()
		require.Equal(t, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scope), dwg.Bucket())

		kv, err := tc.js.KeyValue(tc.ctx, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scope))
		_ = kv
		require.NoError(t, err)

		val, exists := getKVValue(t, tc.ctx, kv, groupId)
		if exists { // It's okay if it doesn't exist or is 0
			require.Equal(t, 0, val)
		}

		// 2. Add 1
		err = dwg.Add(tc.ctx, groupId, 1)
		require.NoError(t, err)
		val, exists = getKVValue(t, tc.ctx, kv, groupId)
		require.True(t, exists, "Key should exist after Add")
		require.Equal(t, 1, val, "Value should be 1 after Add(1)")

		// Start Wait in background - should block
		waitChan := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(tc.ctx, longWait)
			defer cancel()
			waitChan <- dwg.Wait(waitCtx, groupId)
		}()
		// Ensure Wait doesn't return immediately
		select {
		case err := <-waitChan:
			require.Fail(t, "Wait should have blocked, but returned immediately with error: %v", err)
		case <-time.After(shortWait):
			// Expected behavior: Wait is blocking
			t.Logf("Wait should have blocked, but returned immediately without error")
		}

		err = dwg.Done(tc.ctx, groupId)
		require.NoError(t, err)
		val, exists = getKVValue(t, tc.ctx, kv, groupId)
		require.True(t, exists, "Key should still exist after Done")
		require.Equal(t, 0, val, "Value should be 0 after Done")

		select {
		case err := <-waitChan:
			require.NoError(t, err, "Wait should unblock without error")
		case <-time.After(shortWait): // Give watcher time to receive update
			require.Fail(t, "Wait did not unblock after Done")
		}

		waitCtxImmediate, cancelImmediate := context.WithTimeout(tc.ctx, shortWait)
		defer cancelImmediate()
		err = dwg.Wait(waitCtxImmediate, groupId)
		require.NoError(t, err, "Second Wait call should return immediately")
	})

	t.Run("WaitGroupAddDoneMultiple", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "add-done-wait-multi"
		count := 5

		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)
		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.NoError(t, err)
		}()

		kv, err := tc.js.KeyValue(tc.ctx, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scope))
		require.NoError(t, err)

		err = dwg.Add(tc.ctx, groupId, count)
		require.NoError(t, err)
		val, _ := getKVValue(t, tc.ctx, kv, groupId)
		require.Equal(t, count, val)

		// Start Wait
		waitChan := make(chan error, 1)
		go func() {
			waitCtx, cancel := context.WithTimeout(tc.ctx, longWait)
			defer cancel()
			waitChan <- dwg.Wait(waitCtx, groupId)
		}()

		time.Sleep(shortWait)
		select {
		case <-waitChan:
			require.Fail(t, "Wait unblocked too early")
		default: // Good
			t.Logf("Good")
		}

		// Call Done count times
		for i := 0; i < count; i++ {
			err = dwg.Done(tc.ctx, groupId)
			require.NoError(t, err, "Done failed on iteration %d", i)
			// Optional: check intermediate state (can slow down test)
			val, _ = getKVValue(t, tc.ctx, kv, groupId)
			require.Equal(t, count-(i+1), val)
		}

		// Wait should unblock
		select {
		case err := <-waitChan:
			require.NoError(t, err, "Wait should unblock without error after all Dones")
		case <-time.After(longWait): // Increased timeout as multiple updates propagate
			require.Fail(t, "Wait did not unblock")
		}

		val, _ = getKVValue(t, tc.ctx, kv, groupId)
		require.Equal(t, 0, val, "Final value should be 0")
	})

	t.Run("WaitGroupAddDoneGoroutine", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "add-done-wait-goroutine"
		numWaiters := 3
		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)
		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.NoError(t, err)
		}()

		err = dwg.Add(tc.ctx, groupId, 1)
		require.NoError(t, err)

		var wg sync.WaitGroup // Standard waitgroup for test coordination
		errChan := make(chan error, numWaiters)
		wg.Add(numWaiters)

		for i := 0; i < numWaiters; i++ {
			go func(id int) {
				defer wg.Done()
				waitCtx, cancel := context.WithTimeout(tc.ctx, longWait)
				defer cancel()
				err := dwg.Wait(waitCtx, groupId)
				if err != nil {
					errChan <- fmt.Errorf("waiter %d failed: %w", id, err)
				} else {
					errChan <- nil // Signal success
				}
			}(i)
		}
		// Ensure waiters are blocking
		time.Sleep(shortWait * 2) // Give time for watchers to establish
		// Trigger unblock
		t.Log("Calling Done()")
		err = dwg.Done(tc.ctx, groupId)
		require.NoError(t, err)

		// Wait for all waiters to finish
		wg.Wait()
		close(errChan)

		// Check results
		for err := range errChan {
			require.NoError(t, err, "One of the waiters failed")
		}
		t.Log("All waiters finished successfully")
	})

	t.Run("WaitGroupConcurrent", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "add-done-wait-concurrent"
		numOps := 50 // Number of Add/Done pairs

		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)
		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.NoError(t, err)
		}()

		var testWg sync.WaitGroup // Standard waitgroup for test coordination

		testWg.Add(numOps) // Add goroutines
		for i := 0; i < numOps; i++ {
			go func() {
				defer testWg.Done()
				addCtx, cancel := context.WithTimeout(tc.ctx, longWait)
				defer cancel()
				if err := dwg.Add(addCtx, groupId, 1); err != nil {
					t.Errorf("Concurrent Add failed: %v", err) // Use Errorf for concurrent tests
				}
			}()
		}

		testWg.Add(numOps) // Done goroutines (can run concurrently with Add)

		for i := 0; i < numOps; i++ {
			go func() {
				defer testWg.Done()
				// Add a small random delay to increase interleaving chances
				time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
				doneCtx, cancel := context.WithTimeout(tc.ctx, longWait)
				defer cancel()
				if err := dwg.Done(doneCtx, groupId); err != nil {
					// It's possible Done might temporarily fail if Adds haven't completed
					// But it should eventually succeed if retried (which Done does internally)
					// So log error but don't fail test immediately unless persistent
					t.Logf("Concurrent Done potentially failed (may retry): %v", err)
					// Re-check final state instead of failing here
				}
			}()
		}
		// Wait for Add/Done operations to seem complete
		testWg.Wait()
		t.Log("Initial Add/Done goroutines finished")

		// Now Wait - it might take a moment for the final Done update to propagate
		waitCtx, cancel := context.WithTimeout(tc.ctx, longWait*3) // Longer timeout needed
		defer cancel()
		err = dwg.Wait(waitCtx, groupId)
		require.NoError(t, err, "Wait should eventually succeed after concurrent Add/Done")

		// Final check of the value in KV store
		kv, _ := tc.js.KeyValue(tc.ctx, fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scope))
		val, exists := getKVValue(t, tc.ctx, kv, groupId)
		require.True(t, exists, "Key should exist after operations")
		require.Equal(t, 0, val, "Final count must be 0 after equal Add and Done calls")
	})

	t.Run("WaitGroupClose", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "wait-close"

		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)
		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.NoError(t, err)
		}()

		kv, err := tc.js.KeyValue(tc.ctx, dwg.Bucket())
		require.NoError(t, err)

		// Add something so the key exists
		err = dwg.Add(tc.ctx, groupId, 1)
		require.NoError(t, err)
		_, exists := getKVValue(t, tc.ctx, kv, groupId)
		require.True(t, exists, "Key should exist before Close")

		// Close the waitgroup
		err = dwg.Close(tc.ctx, groupId)
		require.NoError(t, err)

		// Verify key is purged (Get should return ErrKeyNotFound)
		_, err = kv.Get(tc.ctx, groupId)
		require.Error(t, err, "Get should fail after Close")
		require.True(t, errors.Is(err, jetstream.ErrKeyNotFound), "Error should be ErrKeyNotFound after Close")

		// Try Add/Done/Wait after Close - should likely fail or behave as if starting from 0
		// Add might re-create the key
		err = dwg.Add(tc.ctx, groupId, 1)
		require.NoError(t, err, "Add after Close should potentially re-create the key")
		val, exists := getKVValue(t, tc.ctx, kv, groupId)
		require.True(t, exists, "Key should exist after Add following Close")
		require.Equal(t, 1, val)

		// Close again (should be idempotent regarding key)
		err = dwg.Close(tc.ctx, groupId)
		require.NoError(t, err)
		_, err = kv.Get(tc.ctx, groupId)
		require.True(t, errors.Is(err, jetstream.ErrKeyNotFound), "Key should be gone again after second Close")
	})

	t.Run("WaitGroupDelete", func(t *testing.T) {
		scope := uniqueScope(testScope)
		groupId := "wait-delete"

		var opts []StoreOption[*WaitGroup]
		opts = append(opts, WithScope[*WaitGroup](scope))

		dwg, err := NewWaitGroup(tc.ctx, tc.js, tc.tracer, tc.logger, opts...)
		require.NoError(t, err)
		require.NotNil(t, dwg)
		defer func() {
			err := dwg.Delete(tc.ctx) // Ensure cleanup
			require.Error(t, err)
		}()

		bucketName := fmt.Sprintf("%s_%s", defaultWaitGroupBucketPrefix, scope)
		require.Equal(t, bucketName, dwg.Bucket())

		_, err = tc.js.KeyValue(tc.ctx, bucketName)
		require.NoError(t, err)
		// Delete the waitgroup (and its bucket)
		err = dwg.Delete(tc.ctx)
		require.NoError(t, err)
		// Verify bucket is deleted
		_, err = tc.js.KeyValue(tc.ctx, bucketName)
		require.Error(t, err, "Bucket should not exist after Delete")
		require.True(t, errors.Is(err, jetstream.ErrBucketNotFound), "Error should be ErrBucketNotFound")

		// Try operations after delete - should fail because bucket is gone
		err = dwg.Add(tc.ctx, groupId, 1)
		require.Error(t, err, "Add should fail after Delete")
		// The exact error might vary, but likely involves bucket not found during getState
		// require.Contains(t, err.Error(), "bucket")
		// require.Contains(t, err.Error(), "not found")

		err = dwg.Wait(tc.ctx, groupId)
		require.Error(t, err, "Wait should fail after Delete")

	})
}

func getKVValue(t *testing.T, ctx context.Context, kv jetstream.KeyValue, key string) (int, bool) {
	t.Helper()
	entry, err := kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 0, false // Key doesn't exist
	}
	require.NoError(t, err)
	val, err := strconv.Atoi(string(entry.Value()))
	require.NoError(t, err)
	return val, true
}

func uniqueScope(base string) string {
	return fmt.Sprintf("%s_%d", base, time.Now().UnixNano())
}
