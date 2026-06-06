package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MarketTransaction struct {
	ID       bson.ObjectID `bson:"_id"`
	SellerID bson.ObjectID `bson:"sellerId"`
	BuyerID  bson.ObjectID `bson:"buyerId"`
	ItemID   bson.ObjectID `bson:"itemId"`
	Money    float64       `bson:"money"`
}

type MarketTransactionStore struct {
	coll *mongo.Collection
}

func NewMarketTransactionStore(ctx context.Context, db *mongo.Database) *MarketTransactionStore {
	store := &MarketTransactionStore{
		coll: db.Collection("market_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MarketTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "sellerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on market_transactions.sellerId",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "buyerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on market_transactions.buyerId",
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
			"Failed creating index on market_transactions.itemId",
			"error", err,
		)
		return
	}
}

func (s *MarketTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	sellerID bson.ObjectID,
	buyerID bson.ObjectID,
	itemID bson.ObjectID,
	money float64,
) error {
	tx := MarketTransaction{
		ID:       id,
		SellerID: sellerID,
		BuyerID:  buyerID,
		ItemID:   itemID,
		Money:    money,
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
func (s *MarketTransactionStore) BulkCreate(ctx context.Context, txs []MarketTransaction) error {
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

// ByUser returns market transactions where the user is the buyer or seller,
// newest first. Pass a non-nil before (an _id) to page: only documents with
// _id < before are returned. _id encodes creation time, so it doubles as the
// time-ordered pagination cursor.
func (s *MarketTransactionStore) ByUser(ctx context.Context, userID bson.ObjectID, before *bson.ObjectID, limit int) ([]MarketTransaction, error) {
	if limit <= 0 {
		limit = 20
	}
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: "sellerId", Value: userID}},
		bson.D{{Key: "buyerId", Value: userID}},
	}}}
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

	var out []MarketTransaction
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
