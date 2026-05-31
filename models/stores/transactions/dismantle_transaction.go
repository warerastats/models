package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
			"Failed creating index on dismantle_transactions.userId",
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
			"Failed creating index on dismantle_transactions.itemId",
			"error", err,
		)
		return
	}
}

func (s *DismantleTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	userID bson.ObjectID,
	itemID bson.ObjectID,
	scrapsReceived int,
) error {
	tx := DismantleTransaction{
		ID:             id,
		UserID:         userID,
		ItemID:         itemID,
		ScrapsReceived: scrapsReceived,
	}

	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}
