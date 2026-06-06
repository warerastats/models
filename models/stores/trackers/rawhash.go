package trackers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// hashRaw returns a hex-encoded SHA-256 of the raw upstream payload. It is
// used by the tracker upserts to skip rewriting the (large) raw blob when the
// upstream object is byte-for-byte unchanged since the last sighting. An empty
// payload hashes to "" so empty/placeholder docs never count as "unchanged".
func hashRaw(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// rawUnchanged reports whether the document at id already stores rawHash and it
// equals hash, meaning a full re-write would be a no-op and can be skipped.
// A missing document, a mismatched hash, or an empty hash all return false so
// the caller proceeds with the full upsert.
func rawUnchanged(ctx context.Context, coll *mongo.Collection, id bson.ObjectID, hash string) (bool, error) {
	if hash == "" {
		return false, nil
	}
	var existing struct {
		RawHash string `bson:"rawHash"`
	}
	err := coll.FindOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		options.FindOne().SetProjection(bson.D{{Key: "rawHash", Value: 1}}),
	).Decode(&existing)
	switch {
	case err == nil:
		return existing.RawHash == hash, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		return false, nil
	default:
		return false, err
	}
}
