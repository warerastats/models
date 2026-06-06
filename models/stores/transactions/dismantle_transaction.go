package transactions

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type DismantleTransaction struct {
	ID             bson.ObjectID `bson:"_id"`
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
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating compound index on dismantle_transactions.{userId,_id}",
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
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

// BulkCreate inserts a batch of transactions in a single unordered bulk write.
// Each op is an idempotent upsert keyed on _id with $setOnInsert, so
// re-ingesting an already-stored transaction is a no-op.
func (s *DismantleTransactionStore) BulkCreate(ctx context.Context, txs []DismantleTransaction) error {
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

func (s *DismantleTransactionStore) GetByUserSince(
	ctx context.Context,
	userID bson.ObjectID,
	since time.Time,
) ([]DismantleTransaction, error) {
	cutoff := bson.NewObjectIDFromTimestamp(since)
	filter := bson.D{
		{Key: "userId", Value: userID},
		{Key: "_id", Value: bson.D{{Key: "$gte", Value: cutoff}}},
	}
	cursor, err := s.coll.Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "_id", Value: -1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []DismantleTransaction
	for cursor.Next(ctx) {
		var tx DismantleTransaction
		err = cursor.Decode(&tx)
		if err != nil {
			return nil, err
		}

		out = append(out, tx)
	}

	err = cursor.Err()
	if err != nil {
		return nil, err
	}

	return out, nil
}
