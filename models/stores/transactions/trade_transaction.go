package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type TradeTransaction struct {
	ID              bson.ObjectID  `bson:"id"`
	SellerID        bson.ObjectID  `bson:"sellerId"`
	BuyerID         bson.ObjectID  `bson:"buyerId"`
	SellerMuID      *bson.ObjectID `bson:"sellerMuId,omitempty"`
	BuyerMuID       *bson.ObjectID `bson:"buyerMuId,omitempty"`
	SellerCountryID *bson.ObjectID `bson:"sellerCountryId,omitempty"`
	BuyerCountryID  *bson.ObjectID `bson:"buyerSellerId,omitempty"`
	ItemOfferID     *bson.ObjectID `bson:"itemOfferId,omitempty"`
	ItemCode        string         `bson:"itemCode"`
	Money           float64        `bson:"money"`
	Quantity        int            `bson:"quantity"`
	// In ms
	TimeTillSale int64 `bson:"tts"`
}

type TradeTransactionStore struct {
	coll *mongo.Collection
}

func NewTradeTransactionStore(ctx context.Context, db *mongo.Database) *TradeTransactionStore {
	store := &TradeTransactionStore{
		coll: db.Collection("trade_transactions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *TradeTransactionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "sellerId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on trade_transactions.sellerId & trade_transactions._id",
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
			"Failed creating index on trade_transactions.buyerId & trade_transactions._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on trade_transactions.itemCode & trade_transactions._id",
			"error", err,
		)
		return
	}
}
