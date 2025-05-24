package coordination

import (
	"fmt"
	"reflect"
	"time"
)

type storeOptionScopable interface {
	setScope(string)
	setBucketPrefix(string)
	setBucketName(string)
	setRetryWait(time.Duration)
	setCleanupTTL(time.Duration)
	setMaxRetryAttempts(int)
}
type StoreOption[T any] func(T) error

func WithScope[T any](scope string) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setScope(string) }); ok {
			settable.setScope(scope)
			return nil
		}
		return fmt.Errorf("type %T does not support scope setting", s)
	}
}

func WithBucketPrefix[T any](prefix string) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setBucketPrefix(string) }); ok {
			settable.setBucketPrefix(prefix)
			return nil
		}
		return fmt.Errorf("type %T does not support bucketPrefix setting", s)
	}
}

func WithBucketName[T any](bucketName string) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setBucketName(string) }); ok {
			settable.setBucketName(bucketName)
			return nil
		}
		return fmt.Errorf("type %T does not support bucketName setting", s)
	}
}

func WithRetryWait[T any](ttl time.Duration) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setRetryWait(time.Duration) }); ok {
			settable.setRetryWait(ttl)
			return nil
		}
		return fmt.Errorf("type %T does not support cleanupTTL setting", s)
	}
}
func WithCleanupTTL[T any](ttl time.Duration) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setCleanupTTL(time.Duration) }); ok {
			settable.setCleanupTTL(ttl)
			return nil
		}
		return fmt.Errorf("type %T does not support cleanupTTL setting", s)
	}
}

func WithMaxRetryAttempts[T any](n int) StoreOption[T] {
	return func(s T) error {
		if settable, ok := any(s).(interface{ setMaxRetryAttempts(int) }); ok {
			settable.setMaxRetryAttempts(n)
			return nil
		}
		return fmt.Errorf("type %T does not support cleanupTTL setting", s)
	}
}

// func SetValue[T any, R any](setter func(T) *R, value R) StoreOption[T] {
func SetValue[T ~*U, U any, R any](setter func(T) *R, value R) StoreOption[T] {
	return func(target T) error {
		ptr := setter(target)
		if ptr == nil {
			return fmt.Errorf("field is nil for type %T", target)
		}
		*ptr = value
		return nil
	}
}

func withScopeReflectV0[T any](scope string) StoreOption[T] {
	return func(s T) error {
		v := reflect.ValueOf(s)
		if v.Kind() != reflect.Ptr {
			return fmt.Errorf("must be a pointer")
		}

		field := v.Elem().FieldByName("scope")
		if !field.IsValid() || field.Kind() != reflect.String {
			return fmt.Errorf("type %T has no string scope field", s)
		}

		field.SetString(scope)
		return nil
	}
}
