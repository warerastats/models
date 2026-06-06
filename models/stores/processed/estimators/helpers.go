package estimators

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// existingEventIDs returns the set of event _ids in (since, until], keyed on the
// embedded ObjectID timestamp so it uses the primary key index.
func existingEventIDs(ctx context.Context, coll *mongo.Collection, since, until time.Time) (map[bson.ObjectID]bool, error) {
	cursor, err := coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{
			{Key: "$gt", Value: bson.NewObjectIDFromTimestamp(since)},
			{Key: "$lte", Value: bson.NewObjectIDFromTimestamp(until)},
		}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	out := map[bson.ObjectID]bool{}
	for cursor.Next(ctx) {
		var r struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&r)
		if err != nil {
			return nil, err
		}
		out[r.ID] = true
	}
	return out, cursor.Err()
}
