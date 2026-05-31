package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CaseTransaction struct {
	ID     bson.ObjectID `bson:"id"`
	UserID bson.ObjectID `bson:"userId"`
	ItemID bson.ObjectID `bson:"itemId"`
	Case   string        `bson:"case"`
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
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on case_transactions.userId",
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
			"Failed creating index on case_transactions.itemId",
			"error", err,
		)
		return
	}
}

func (s *CaseTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	userID bson.ObjectID,
	itemID bson.ObjectID,
	caseCode string,
) error {
	tx := CaseTransaction{
		ID:     id,
		UserID: userID,
		ItemID: itemID,
		Case:   caseCode,
	}

	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}
