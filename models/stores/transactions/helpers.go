package transactions

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// earliestObjectIDTime returns the oldest document's creation time and whether any exist.
func earliestObjectIDTime(ctx context.Context, coll *mongo.Collection) (time.Time, bool, error) {
	var doc struct {
		ID bson.ObjectID `bson:"_id"`
	}
	err := coll.FindOne(ctx,
		bson.D{},
		options.FindOne().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetProjection(bson.D{{Key: "_id", Value: 1}}),
	).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return doc.ID.Timestamp(), true, nil
}
