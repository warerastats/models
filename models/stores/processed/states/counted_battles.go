package states

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CountedBattle marks a finalized battle whose participation counters have
// already been folded in, so per-battle distinct counters stay exactly-once.
type CountedBattle struct {
	BattleID  bson.ObjectID `bson:"_id"`
	CountedAt time.Time     `bson:"countedAt"`
}

type CountedBattleStore struct {
	coll *mongo.Collection
}

func NewCountedBattleStore(ctx context.Context, db *mongo.Database) *CountedBattleStore {
	store := &CountedBattleStore{coll: db.Collection("participation_counted_battles")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountedBattleStore) ensureIndex(context.Context) {}

// ExistingAmong returns the subset of ids already marked as counted.
func (s *CountedBattleStore) ExistingAmong(ctx context.Context, ids []bson.ObjectID) (map[bson.ObjectID]bool, error) {
	out := map[bson.ObjectID]bool{}
	if len(ids) == 0 {
		return out, nil
	}
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

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

// Mark records the given battles as counted (idempotent upsert).
func (s *CountedBattleStore) Mark(ctx context.Context, ids []bson.ObjectID) error {
	if len(ids) == 0 {
		return nil
	}
	now := time.Now().UTC()
	ops := make([]mongo.WriteModel, len(ids))
	for i := range ids {
		ops[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: ids[i]}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: bson.D{{Key: "countedAt", Value: now}}}}).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}
