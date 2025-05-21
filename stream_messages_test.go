package nats_jetstream_flow

import (
	"context"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/pnvasko/nats-jetstream-flow/flow"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Helper to create a basic message for tests
func newMessageForTest(uuid string, payload []byte) *Message {
	msg := &Message{
		uuid:     uuid,
		metadata: make(map[string]string),
		payload:  payload,
		ack:      make(chan struct{}),
		noAck:    make(chan struct{}),
	}
	return msg
}

func TestNewMessage(t *testing.T) {
	uuid := "test-uuid-123"
	payload := []byte("test payload")
	msg, err := NewMessage(uuid, payload)

	require.NoError(t, err)
	require.NotNil(t, msg)

	assert.Equal(t, uuid, msg.Uuid())
	assert.Empty(t, msg.Metadata())

	// Verify payload was marshaled correctly
	assert.Equal(t, payload, msg.Payload())

	// Verify channels were created
	assert.NotNil(t, msg.ack)
	assert.NotNil(t, msg.noAck)

	// Verify initial state
	assert.Equal(t, NoAckSent, msg.ackSentType)
}

func TestMessageEquals(t *testing.T) {
	payload1 := []byte("payload one")
	payload2 := []byte("payload two")

	msg1, _ := NewMessage("uuid1", payload1)
	msg1.SetMessageMetadata("key1", "value1")
	msg1.SetMessageMetadata("key2", "value2")

	msg2, _ := NewMessage("uuid1", payload1)
	msg2.SetMessageMetadata("key1", "value1")
	msg2.SetMessageMetadata("key2", "value2")

	msg3, _ := NewMessage("uuid2", payload1) // Different UUID

	msg4, _ := NewMessage("uuid1", payload2) // Different payload

	msg5, _ := NewMessage("uuid1", payload1) // Different metadata size
	msg5.SetMessageMetadata("key1", "value1")

	msg6, _ := NewMessage("uuid1", payload1) // Different metadata key
	msg6.SetMessageMetadata("key3", "value1")
	msg6.SetMessageMetadata("key2", "value2")

	msg7, _ := NewMessage("uuid1", payload1) // Different metadata value
	msg7.SetMessageMetadata("key1", "value-changed")
	msg7.SetMessageMetadata("key2", "value2")

	// Test equality
	equal, err := msg1.Equals(msg2)
	require.NoError(t, err)
	assert.True(t, equal, "msg1 should equal msg2")

	// Test inequality due to different fields
	equal, err = msg1.Equals(msg3)
	require.NoError(t, err)
	assert.False(t, equal, "msg1 should not equal msg3 (uuid)")

	equal, err = msg1.Equals(msg4)
	require.NoError(t, err)
	assert.False(t, equal, "msg1 should not equal msg4 (payload)")

	equal, err = msg1.Equals(msg5)
	require.NoError(t, err)
	assert.False(t, equal, "msg1 should not equal msg5 (metadata size)")

	equal, err = msg1.Equals(msg6)
	require.NoError(t, err)
	assert.False(t, equal, "msg1 should not equal msg6 (metadata key)")

	equal, err = msg1.Equals(msg7)
	require.NoError(t, err)
	assert.False(t, equal, "msg1 should not equal msg7 (metadata value)")

	// Test with messages having no metadata
	msgNoMeta1, _ := NewMessage("uuidA", payload1)
	msgNoMeta2, _ := NewMessage("uuidA", payload1)
	equal, err = msgNoMeta1.Equals(msgNoMeta2)
	require.NoError(t, err)
	assert.True(t, equal, "messages with no metadata should be equal if uuid and payload match")

	// payload1 := []byte("payload one")
	//msg1, _ := NewMessage("uuid1", payload1)
	//msg1.SetMessageMetadata("key1", "value1")
	//msg1.SetMessageMetadata("key2", "value2")
	// --- Test with a payload of a different proto type (simulates unmarshalling error due to type mismatch) ---
	// todo
	//msgDifferentTypePayload, _ := NewMessage("uuidY", []byte("dummy")) // Payload data doesn't matter for type mismatch
	//stringValue := &wrapperspb.StringValue{Value: "not bytes value"}   // Use wrapperspb
	//stringAny, err := anypb.New(stringValue)                           // Marshal StringValue into Any
	//require.NoError(t, err)
	//msgDifferentTypePayload.Payload() = stringAny // Replace payload with Any of different type

	// Now compare msg1 (valid BytesValue) with msgDifferentTypePayload (StringValue in Any)
	//equal, err = msg1.Equals(msgDifferentTypePayload)
	//assert.Error(t, err, "Equals should return error when comparing BytesValue to Any claiming StringValue")
	//assert.False(t, equal, "Equals should return false when comparing BytesValue to Any claiming StringValue")
	//
	//// Test the check in the other direction
	//equal, err = msgDifferentTypePayload.Equals(msg1)
	//assert.Error(t, err, "Equals should return error when comparing Any claiming StringValue to BytesValue (reverse)")
	//assert.False(t, equal)

	// --- Test with a message where the payload is truly malformed protobuf ---
	// Craft an Any claiming to be BytesValue, but provide invalid protobuf data

	//msgMalformedBytesPayload, _ := NewMessage("uuidZ", []byte("dummy"))
	//msgMalformedBytesPayload.Payload() = &anypb.Any{
	//	TypeUrl: "type.googleapis.com/google.protobuf.BytesValue", // Claims to be BytesValue
	//	Value:   []byte{0xFF, 0xFF, 0xFF, 0xFF},                   // Invalid protobuf bytes for BytesValue (varint would be too large)
	//}
	//
	//equal, err = msg1.Equals(msgMalformedBytesPayload)
	//assert.Error(t, err, "Equals should return error when comparing BytesValue to Any with malformed data")
	//assert.False(t, equal, "Equals should return false when comparing BytesValue to Any with malformed data")
	//
	//equal, err = msgMalformedBytesPayload.Equals(msg1)
	//assert.Error(t, err, "Equals should return error when comparing Any with malformed data to BytesValue (reverse)")
	//assert.False(t, equal)
	// todo.
}

func TestMessageAck(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	// Initial state
	assert.Equal(t, NoAckSent, msg.ackSentType)

	// First Ack
	success := msg.Ack()
	assert.True(t, success, "First Ack should succeed")
	assert.Equal(t, Ack, msg.ackSentType)

	// Check Ack channel is closed
	select {
	case <-msg.Acked():
		// Success
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Acked channel should be closed after Ack()")
	}

	// Idempotency: Second Ack
	success = msg.Ack()
	assert.True(t, success, "Second Ack should also succeed (idempotent)")
	assert.Equal(t, Ack, msg.ackSentType) // State should remain Ack

	// Cannot Ack after Nack
	msg2 := newMessageForTest("uuid", []byte("payload"))
	msg2.Nack()
	success = msg2.Ack()
	assert.False(t, success, "Ack should fail after Nack")
	assert.Equal(t, Nack, msg2.ackSentType) // State should remain Nack
}

func TestMessageNack(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	// Initial state
	assert.Equal(t, NoAckSent, msg.ackSentType)

	// First Nack
	success := msg.Nack()
	assert.True(t, success, "First Nack should succeed")
	assert.Equal(t, Nack, msg.ackSentType)

	// Check noAck channel is closed
	select {
	case <-msg.Nacked():
		// Success
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Nacked channel should be closed after Nack()")
	}

	// Idempotency: Second Nack
	success = msg.Nack()
	assert.True(t, success, "Second Nack should also succeed (idempotent)")
	assert.Equal(t, Nack, msg.ackSentType) // State should remain Nack

	// Cannot Nack after Ack
	msg2 := newMessageForTest("uuid", []byte("payload"))
	msg2.Ack()
	success = msg2.Nack()
	assert.False(t, success, "Nack should fail after Ack")
	assert.Equal(t, Ack, msg2.ackSentType) // State should remain Ack
}

func TestMessageAckNackMutualExclusion(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	// Use channels to synchronize goroutines and check results
	ackFinished := make(chan bool)
	nackFinished := make(chan bool)

	go func() {
		ackFinished <- msg.Ack()
	}()

	go func() {
		nackFinished <- msg.Nack()
	}()

	ackResult := <-ackFinished
	nackResult := <-nackFinished

	// Exactly one of them should have succeeded
	assert.True(t, ackResult != nackResult, "Exactly one of Ack() or Nack() should return true")

	// Verify the final state matches the successful operation
	if ackResult {
		assert.Equal(t, Ack, msg.ackSentType)
		select {
		case <-msg.Acked():
		default:
			t.Fatal("Acked channel should be closed if Ack succeeded")
		}
		select {
		case <-msg.Nacked():
			t.Fatal("Nacked channel should not be closed if Ack succeeded")
		default:
		}
	} else { // nackResult must be true
		assert.Equal(t, Nack, msg.ackSentType)
		select {
		case <-msg.Nacked():
		default:
			t.Fatal("Nacked channel should be closed if Nack succeeded")
		}
		select {
		case <-msg.Acked():
			t.Fatal("Acked channel should not be closed if Nack succeeded")
		default:
		}
	}
}

func TestMessageAckedNackedChannels(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	ackCh := msg.Acked()
	nackCh := msg.Nacked()

	assert.NotNil(t, ackCh)
	assert.NotNil(t, nackCh)

	// Channels should initially not be closed
	select {
	case <-ackCh:
		t.Fatal("Acked channel should not be closed initially")
	case <-nackCh:
		t.Fatal("Nacked channel should not be closed initially")
	case <-time.After(10 * time.Millisecond):
		// Expected
	}

	// Close one and check
	msg.Ack()
	select {
	case <-ackCh:
		// Expected
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Acked channel should be closed after Ack")
	}
	select {
	case <-nackCh:
		t.Fatal("Nacked channel should not be closed after Ack")
	case <-time.After(10 * time.Millisecond):
		// Expected
	}

	// Test second message
	msg2 := newMessageForTest("uuid", []byte("payload"))
	ackCh2 := msg2.Acked()
	nackCh2 := msg2.Nacked()

	msg2.Nack()
	select {
	case <-nackCh2:
		// Expected
	case <-time.After(10 * time.Millisecond):
		t.Fatal("Nacked channel should be closed after Nack")
	}
	select {
	case <-ackCh2:
		t.Fatal("Acked channel should not be closed after Nack")
	case <-time.After(10 * time.Millisecond):
		// Expected
	}
}

func TestMessageContext(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	// Initial context should be Background
	assert.Equal(t, context.Background(), msg.Context())

	// Set a custom context
	ctx := context.WithValue(context.Background(), "key", "value")
	msg.SetContext(ctx)
	assert.Equal(t, ctx, msg.Context())

	// Setting nil context? Should probably return Background then.
	// The current implementation stores the nil context and returns Background if it's nil.
	msg.SetContext(nil)
	assert.Equal(t, context.Background(), msg.Context())
}

func TestMessageCopy(t *testing.T) {
	payload := []byte("original payload")
	original, _ := NewMessage("original-uuid", payload)
	original.SetContext(context.WithValue(context.Background(), "orig_key", "orig_value"))
	original.SetMessageMetadata("meta1", "val1")
	original.SetMessageMetadata("meta2", "val2")

	copied, err := original.Copy()
	require.NoError(t, err)
	require.NotNil(t, copied)

	// Check fields are copied correctly (value copy)
	assert.Equal(t, original.Uuid(), copied.Uuid())
	assert.Equal(t, original.Payload(), copied.Payload())

	assert.Equal(t, original.Metadata(), copied.Metadata())

	// Check metadata is a deep copy (modifying copy shouldn't affect original)
	copied.SetMessageMetadata("meta1", "changed")
	copied.SetMessageMetadata("new_meta", "added")
	assert.Equal(t, "val1", original.GetMessageMetadata("meta1"))
	assert.Empty(t, original.GetMessageMetadata("new_meta"))
	assert.Equal(t, "changed", copied.GetMessageMetadata("meta1"))
	assert.Equal(t, "added", copied.GetMessageMetadata("new_meta"))

	// Check channels are NEW (not the same pointer)
	assert.NotEqual(t, original.ack, copied.ack)
	assert.NotEqual(t, original.noAck, copied.noAck)

	// Check contexts are different (Context is value-like, but conceptually it's a copy)
	// The *value* of the context is copied, but modifying the copied context won't affect the original.
	// Setting a new value on the original's context won't show up in the copy.
	assert.Equal(t, original.Context(), copied.Context()) // Initially different because copied gets Background()
	copied.SetContext(context.WithValue(context.Background(), "copy_key", "copy_value"))
	assert.NotEqual(t, original.Context(), copied.Context())          // Initially different because copied gets Background()
	assert.Nil(t, original.Context().Value("copy_key"))               // Verify original wasn't affected
	assert.Equal(t, "copy_value", copied.Context().Value("copy_key")) // Verify copy got the new value

	// Check initial state of copied channels
	select {
	case <-copied.Acked():
		t.Fatal("Copied Acked channel should not be closed")
	case <-copied.Nacked():
		t.Fatal("Copied Nacked channel should not be closed")
	default:
	}
}

func TestMessageStreamMessage(t *testing.T) {
	payload := []byte("test data")
	msg, _ := NewMessage("stream-uuid", payload)
	msg.SetMessageMetadata("foo", "bar")
	msg.SetMessageMetadata("baz", "qux")

	streamMsg, err := msg.StreamMessage()
	require.NoError(t, err)
	require.NotNil(t, streamMsg)

	assert.Equal(t, msg.Uuid(), streamMsg.Uuid)

	anyValue := &anypb.Any{}
	bytesValue := &wrappers.BytesValue{
		Value: msg.Payload(),
	}
	err = anypb.MarshalFrom(anyValue, bytesValue, proto.MarshalOptions{})
	require.NoError(t, err)

	// Check payload reference/copy - it should be the same *Any* object reference
	assert.Equal(t, anyValue, streamMsg.Payload)

	// Check metadata copy (should be a deep copy of the map)
	assert.Equal(t, msg.Metadata(), streamMsg.Metadata) // Maps are reference types
	assert.Equal(t, len(msg.Metadata()), len(streamMsg.Metadata))
	for k, v := range msg.Metadata() {
		val, ok := streamMsg.Metadata[k]
		assert.True(t, ok)
		assert.Equal(t, v, val)
	}

	// Verify the bug fix: Ensure metadata is copied to the new map, not back onto the original
	msg.SetMessageMetadata("foo", "changed") // Modify original metadata AFTER StreamMessage call
	assert.Equal(t, "bar", streamMsg.Metadata["foo"], "streamMsg metadata should retain original value after original is modified")

	streamMsg.Metadata["new_key"] = "new_value" // Modify copied metadata
	_, ok := msg.Metadata()["new_key"]
	assert.False(t, ok, "Modifying streamMsg metadata should not affect original msg")

}

func TestMessageGetSetMessageMetadata(t *testing.T) {
	msg := newMessageForTest("uuid", []byte("payload"))

	// Get non-existent key
	assert.Equal(t, "", msg.GetMessageMetadata("nonexistent"))

	// Set and get existing key
	msg.SetMessageMetadata("key1", "value1")
	assert.Equal(t, "value1", msg.GetMessageMetadata("key1"))

	// Overwrite existing key
	msg.SetMessageMetadata("key1", "newValue")
	assert.Equal(t, "newValue", msg.GetMessageMetadata("key1"))

	// Set another key
	msg.SetMessageMetadata("key2", "value2")
	assert.Equal(t, "newValue", msg.GetMessageMetadata("key1"))
	assert.Equal(t, "value2", msg.GetMessageMetadata("key2"))

	subject, err := msg.Subject()
	require.Error(t, err)
	msg.SetSubject("test.subject")
	subject, err = msg.Subject()
	require.NoError(t, err)
	assert.Equal(t, "test.subject", subject)
}

func TestMessageMarshalUnmarshal(t *testing.T) {
	testSubject := "test.subject"
	testUuid := "test.uuid"
	testPayload := "payload"
	testMetadataKey := "test.metadata.key"
	testMetadataValue := "test.metadata.value"

	msg := newMessageForTest(testUuid, []byte(testPayload))
	msg.SetSubject(testSubject)
	msg.SetMessageMetadata(testMetadataKey, testMetadataValue)
	var _ flow.Message = msg

	subject, err := msg.Subject()
	require.NoError(t, err)
	assert.Equal(t, testSubject, subject)
	assert.Equal(t, testUuid, msg.Uuid())

	assert.Equal(t, testMetadataValue, msg.GetMessageMetadata(testMetadataKey))

	data, err := msg.Marshal()
	require.NoError(t, err)

	msg1 := &Message{}
	err = msg1.Unmarshal(data)
	require.NoError(t, err)
	assert.Equal(t, msg.Uuid(), msg1.Uuid())

	subject1, err := msg.Subject()
	require.NoError(t, err)
	assert.Equal(t, testSubject, subject1)
	assert.Equal(t, testMetadataValue, msg1.GetMessageMetadata(testMetadataKey))
	assert.Equal(t, msg.Payload(), msg1.Payload())
}
