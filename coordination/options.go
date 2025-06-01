package coordination

import (
	"fmt"
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

func WithScope[T interface{ setScope(string) }](scope string) StoreOption[T] {
	return func(s T) error {
		s.setScope(scope)
		return nil
	}
}

func WithBucketPrefix[T interface{ setBucketPrefix(string) }](prefix string) StoreOption[T] {
	return func(s T) error {
		s.setBucketPrefix(prefix)
		return nil
	}
}

func WithBucketName[T interface{ setBucketName(string) }](bucketName string) StoreOption[T] {
	return func(s T) error {
		s.setBucketName(bucketName)
		return nil
	}
}

func WithRetryWait[T interface{ setRetryWait(time.Duration) }](ttl time.Duration) StoreOption[T] {
	return func(s T) error {
		s.setRetryWait(ttl)
		return nil
	}
}

func WithCleanupTTL[T interface{ setCleanupTTL(time.Duration) }](ttl time.Duration) StoreOption[T] {
	return func(s T) error {
		s.setCleanupTTL(ttl)
		return nil
	}
}

func WithMaxRetryAttempts[T interface{ setMaxRetryAttempts(int) }](n int) StoreOption[T] {
	return func(s T) error {
		s.setMaxRetryAttempts(n)
		return nil
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

//func WithScope[T interface{ setScope(string) }](scope string) StoreOption[T] {
//	return func(s T) error {
//		if settable, ok := any(s).(interface{ setScope(string) }); ok {
//			settable.setScope(scope)
//			return nil
//		}
//		return fmt.Errorf("type %T does not support scope setting", s)
//	}
//}

//func withScopeReflectV0[T any](scope string) StoreOption[T] {
//	return func(s T) error {
//		v := reflect.ValueOf(s)
//		if v.Kind() != reflect.Ptr {
//			return fmt.Errorf("must be a pointer")
//		}
//
//		field := v.Elem().FieldByName("scope")
//		if !field.IsValid() || field.Kind() != reflect.String {
//			return fmt.Errorf("type %T has no string scope field", s)
//		}
//
//		field.SetString(scope)
//		return nil
//	}
//}
