package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type CraftTransaction struct {
	ID         bson.ObjectID `bson:"id"`
	UserID     bson.ObjectID `bson:"userId"`
	ItemID     bson.ObjectID `bson:"itemId"`
	ScrapsCost int           `bson:"scraps"`
}

type CraftTransactionStore struct {
	coll *mongo.Collection
}

func NewCraftTransactionStore(ctx context.Context, db *mongo.Database) *CraftTransactionStore {
	store := &CraftTransactionStore{
		coll: db.Collection("craft_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CraftTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on craft_transactions.userId & craft_transactions._id",
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
			"Failed creating index on craft_transactions.itemId & craft_transactions._id",
			"error", err,
		)
		return
	}
}
