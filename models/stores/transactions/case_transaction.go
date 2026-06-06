package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CaseTransaction struct {
	ID     bson.ObjectID `bson:"_id"`
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
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// BulkCreate inserts a batch of transactions in a single unordered bulk write.
// Each op is an idempotent upsert keyed on _id with $setOnInsert, so
// re-ingesting an already-stored transaction is a no-op.
func (s *CaseTransactionStore) BulkCreate(ctx context.Context, txs []CaseTransaction) error {
	if len(txs) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(txs))
	for i := range txs {
		ops[i] = mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: txs[i].ID}}).
			SetUpdate(bson.D{{Key: "$setOnInsert", Value: txs[i]}}).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}

// ByUser returns case openings for the given user, newest first. Pass a non-nil
// before (an _id) to page: only documents with _id < before are returned. _id
// encodes creation time, so it doubles as the time-ordered pagination cursor.
func (s *CaseTransactionStore) ByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]CaseTransaction, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := bson.D{{Key: "userId", Value: userID}}
	if before != nil {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$lt", Value: *before}}})
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().
			SetSort(bson.D{{Key: "_id", Value: -1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []CaseTransaction
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
