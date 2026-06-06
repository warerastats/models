package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CraftTransaction struct {
	ID         bson.ObjectID `bson:"_id"`
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
			"Failed creating index on craft_transactions.userId",
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
			"Failed creating index on craft_transactions.itemId",
			"error", err,
		)
		return
	}
}

func (s *CraftTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	userID bson.ObjectID,
	itemID bson.ObjectID,
	scrapsCost int,
) error {
	tx := CraftTransaction{
		ID:         id,
		UserID:     userID,
		ItemID:     itemID,
		ScrapsCost: scrapsCost,
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
func (s *CraftTransactionStore) BulkCreate(ctx context.Context, txs []CraftTransaction) error {
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
