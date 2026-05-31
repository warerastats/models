package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Battle struct {
	ID                bson.ObjectID   `bson:"id"`
	AttackerRegionID  bson.ObjectID   `bson:"attackerRegionId"`
	AttackerCountryID bson.ObjectID   `bson:"attackerCountryId"`
	DefenderRegionID  bson.ObjectID   `bson:"defenderRegionId"`
	DefenderCountryID bson.ObjectID   `bson:"defenderCountryId"`
	LatestObject      json.RawMessage `bson:"raw"`
}

type BattleStore struct {
	coll *mongo.Collection
}

func NewBattleStore(ctx context.Context, db *mongo.Database) *BattleStore {
	store := &BattleStore{
		coll: db.Collection("battles"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on battles.id",
			"error", err,
		)
		return
	}
}
