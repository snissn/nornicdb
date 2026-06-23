package bolt

import (
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestDecodePackStreamValue_LocalDateTimeStructure(t *testing.T) {
	want := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	data := encodePackStreamLocalDateTimeInto(nil, want.Unix(), int64(want.Nanosecond()))

	got, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Fatalf("decode localdatetime failed: %v", err)
	}

	requireNormalizedDateTime(t, got, want)
}

func TestBoltIntegration_LocalDateTimeParamRoundTrip_ServerStack(t *testing.T) {
	want := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	t.Run("single param create", func(t *testing.T) {
		roundTripLocalDateTimeScenario(
			t,
			buildRunMessageWithLocalDateTimeParam(
				"CREATE (:T {uuid:'single', created_at:$dt})",
				"dt",
				want.Unix(),
				int64(want.Nanosecond()),
			),
			"MATCH (n:T {uuid:'single'}) RETURN n.created_at AS ca",
			want,
		)
	})

	t.Run("unwind bulk row", func(t *testing.T) {
		roundTripLocalDateTimeScenario(
			t,
			buildRunMessageWithLocalDateTimeRows(
				"UNWIND $rows AS row MERGE (n:T {uuid:row.uuid}) SET n.created_at = row.created_at",
				"bulk",
				want.Unix(),
				int64(want.Nanosecond()),
			),
			"MATCH (n:T {uuid:'bulk'}) RETURN n.created_at AS ca",
			want,
		)
	})
}

func roundTripLocalDateTimeScenario(t *testing.T, writeMessage []byte, readQuery string, want time.Time) {
	t.Helper()

	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "bolt_localdatetime_roundtrip")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	runBoltStatementNoRecords(t, conn, BuildRunMessage("MATCH (n:T) DETACH DELETE n", nil, nil))
	runBoltStatementNoRecords(t, conn, writeMessage)

	rows := runBoltQueryAndCollectRecords(t, conn, readQuery)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("expected one row with one field, got %#v", rows)
	}

	requireNormalizedDateTime(t, rows[0][0], want)
}

func runBoltStatementNoRecords(t *testing.T, conn net.Conn, message []byte) {
	t.Helper()

	requireNoError(t, SendMessage(conn, message))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
}

func encodePackStreamLocalDateTimeInto(dst []byte, sec, nanos int64) []byte {
	dst = append(dst, 0xB2, 0x64) // struct(2), LocalDateTime
	dst = encodePackStreamIntInto(dst, sec)
	dst = encodePackStreamIntInto(dst, nanos)
	return dst
}

func buildRunMessageWithLocalDateTimeParam(query, paramName string, sec, nanos int64) []byte {
	buf := []byte{0xB1, MsgRun}
	buf = append(buf, encodePackStreamString(query)...)
	buf = append(buf, 0xA1)
	buf = append(buf, encodePackStreamString(paramName)...)
	buf = encodePackStreamLocalDateTimeInto(buf, sec, nanos)
	buf = append(buf, 0xA0)
	return buf
}

func buildRunMessageWithLocalDateTimeRows(query, uuid string, sec, nanos int64) []byte {
	buf := []byte{0xB1, MsgRun}
	buf = append(buf, encodePackStreamString(query)...)
	buf = append(buf, 0xA1)
	buf = append(buf, encodePackStreamString("rows")...)
	buf = append(buf, 0x91)
	buf = append(buf, 0xA2)
	buf = append(buf, encodePackStreamString("uuid")...)
	buf = append(buf, encodePackStreamString(uuid)...)
	buf = append(buf, encodePackStreamString("created_at")...)
	buf = encodePackStreamLocalDateTimeInto(buf, sec, nanos)
	buf = append(buf, 0xA0)
	return buf
}

func requireNormalizedDateTime(t *testing.T, got any, want time.Time) {
	t.Helper()

	value, ok := got.(time.Time)
	if !ok {
		t.Fatalf("expected normalized time.Time, got %T (%#v)", got, got)
	}
	assertNormalizedDateTime(t, value, want)
}

func assertNormalizedDateTime(t *testing.T, got time.Time, want time.Time) {
	t.Helper()

	_, offset := got.Zone()
	if offset != 0 {
		t.Fatalf("expected zero-offset normalized time, got offset %d for %q", offset, got.Location())
	}
	if !got.Equal(want) {
		t.Fatalf("expected normalized datetime %s, got %s", want.Format(time.RFC3339Nano), got.Format(time.RFC3339Nano))
	}
}
