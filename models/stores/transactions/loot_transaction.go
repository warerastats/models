package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type LootTransaction struct {
	ID     bson.ObjectID `bson:"id"`
	UserID bson.ObjectID `bson:"userId"`
	ItemID bson.ObjectID `bson:"itemId"`
}

type LootTransactionStore struct {
	coll *mongo.Collection
}

func NewLootTransactionStore(ctx context.Context, db *mongo.Database) *LootTransactionStore {
	store := &LootTransactionStore{
		coll: db.Collection("loot_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *LootTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on loot_transactions.userId & loot_transactions._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on loot_transactions.itemId & loot_transactions._id",
			"error", err,
		)
		return
	}
}
