package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MarketTransaction struct {
	ID       bson.ObjectID `bson:"id"`
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
			"Failed creating index on market_transactions.sellerId & market_transactions._id",
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
			"Failed creating index on market_transactions.buyerId & market_transactions._id",
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
			"Failed creating index on market_transactions.itemId & market_transactions._id",
			"error", err,
		)
		return
	}
}
