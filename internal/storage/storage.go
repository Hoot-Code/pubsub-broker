// Package storage implements the segment-based log storage engine.
//
// Each topic-partition maps to a directory of Segments. Each Segment owns:
//   - .log   — append-only encoded message records
//   - .index — sparse offset→filePosition lookup table
//
// Segment roll-over occurs when the .log exceeds SegmentMaxBytes.
// A new active Segment is then created with baseOffset = nextOffset.
//
// Record formats:
//
//	Version 1 (legacy): [ offset(8) | ts(8) | keyLen(2) | payloadLen(4) | crc(4) | key | payload ]
//	Version 2 (current): segment begins with a 5-byte header [magic(4)|version(1)],
//	  each record: [ offset(8) | ts(8) | keyLen(2) | payloadLen(4) | crc(4) | codec(1) | key | payload ]
//
// The sub-files of this package are:
//
//   - record.go       — record encoding constants and encode/decode functions
//   - segment.go      — Segment struct and all segment-level methods
//   - partition_log.go — PartitionLog struct and all partition-log methods
//   - sendfile.go /sendfile_common.go / sendfile_fallback.go — OS zero-copy
package storage
