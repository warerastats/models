package trackers

import (
	"context"
	"log/slog"
	"time"

	"github.com/warerastats/models/models/enums"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Order struct {
	// Basically the only place where we generate our own ID
	ID            bson.ObjectID  `bson:"id"`
	BattleID      bson.ObjectID  `bson:"battleId"`
	Side          enums.Side     `bson:"side"`
	ActiveSince   time.Time      `bson:"activeSince"`
	InactiveSince time.Time      `bson:"inactiveSince"`
	CountryID     *bson.ObjectID `bson:"countryId,omitempty"`
	MuID          *bson.ObjectID `bson:"muId,omitempty"`
}

type OrderStore struct {
	coll *mongo.Collection
}

func NewOrderStore(ctx context.Context, db *mongo.Database) *OrderStore {
	store := &OrderStore{
		coll: db.Collection("orders"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *OrderStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "battleId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on orders.battleId",
			"error", err,
		)
		return
	}
}
