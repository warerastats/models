package events

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type BattleOrderChange struct {
	ID       bson.ObjectID `bson:"_id,omitempty"`
	BattleID bson.ObjectID `bson:"battleId"`
	Side     string        `bson:"side"`
	Kind     string        `bson:"kind"`
	Action   string        `bson:"action"`
	EntityID bson.ObjectID `bson:"entityId"`
	At       time.Time     `bson:"at"`
}

type BattleOrderChangeStore struct {
	coll *mongo.Collection
}

func NewBattleOrderChangeStore(ctx context.Context, db *mongo.Database) *BattleOrderChangeStore {
	store := &BattleOrderChangeStore{
		coll: db.Collection("events_battle_order_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *BattleOrderChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "battleId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_battle_order_change.battleId",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "entityId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_battle_order_change.entityId",
			"error", err,
		)
	}
}

func (s *BattleOrderChangeStore) Set(ctx context.Context, change BattleOrderChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}
