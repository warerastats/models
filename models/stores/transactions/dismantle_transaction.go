package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type DismantleTransaction struct {
	ID             bson.ObjectID `bson:"id"`
	UserID         bson.ObjectID `bson:"userId"`
	ItemID         bson.ObjectID `bson:"itemId"`
	ScrapsReceived int           `bson:"scraps"`
}

type DismantleTransactionStore struct {
	coll *mongo.Collection
}

func NewDismantleTransactionStore(ctx context.Context, db *mongo.Database) *DismantleTransactionStore {
	store := &DismantleTransactionStore{
		coll: db.Collection("dismantle_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *DismantleTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on dismantle_transactions.userId & dismantle_transactions._id",
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
			"Failed creating index on dismantle_transactions.itemId & dismantle_transactions._id",
			"error", err,
		)
		return
	}
}
