package transactions

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
			"Failed creating index on trade_transactions.sellerId",
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
			"Failed creating index on trade_transactions.buyerId",
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
			"Failed creating index on trade_transactions.itemCode",
			"error", err,
		)
		return
	}
}

func (s *TradeTransactionStore) Create(
	ctx context.Context,
	id bson.ObjectID,
	sellerID bson.ObjectID,
	buyerID bson.ObjectID,
	sellerMuID *bson.ObjectID,
	buyerMuID *bson.ObjectID,
	sellerCountryID *bson.ObjectID,
	buyerCountryID *bson.ObjectID,
	itemOfferID *bson.ObjectID,
	itemCode string,
	money float64,
	quantity int,
	timeTillSale int64,
) error {
	tx := TradeTransaction{
		ID:              id,
		SellerID:        sellerID,
		BuyerID:         buyerID,
		SellerMuID:      sellerMuID,
		BuyerMuID:       buyerMuID,
		SellerCountryID: sellerCountryID,
		BuyerCountryID:  buyerCountryID,
		ItemOfferID:     itemOfferID,
		ItemCode:        itemCode,
		Money:           money,
		Quantity:        quantity,
		TimeTillSale:    timeTillSale,
	}

	_, err := s.coll.UpdateOne(
		ctx,
		bson.D{{Key: "id", Value: id}},
		bson.D{{Key: "$setOnInsert", Value: tx}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}
