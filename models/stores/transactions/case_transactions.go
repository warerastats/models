package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type CaseTransaction struct {
	ID     bson.ObjectID `bson:"id,omitempty"`
	Case   string        `bson:"case"`
	UserID string        `bson:"userId"`
	ItemID bson.ObjectID `bson:"itemId"`
}

type CaseTransactionStore struct {
	coll *mongo.Collection
}

func NewCaseTransactionStore(ctx context.Context, db *mongo.Database) *CaseTransactionStore {
	store := &CaseTransactionStore{
		coll: db.Collection("case_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CaseTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on case_transactions.userId & case_transactions._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on case_transactions.itemId & case_transactions._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemId", Value: 1},
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on case_transactions.itemId & case_transactions.userId & case_transactions._id",
			"error", err,
		)
		return
	}
}
