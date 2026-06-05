package trackers

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type MarketOffer struct {
	ID        bson.ObjectID `bson:"_id"`
	UserID    bson.ObjectID `bson:"userId"`
	ItemCode  string        `bson:"itemCode"`
	Quantity  int           `bson:"quantity"`
	Fulfilled int           `bson:"fulfilled"`
	Cancelled bool          `bson:"cancelled"`
	Price     float64       `bson:"price"`
	Since     time.Time     `bson:"since"`
}

type MarketOfferStore struct {
	coll *mongo.Collection
}

func NewMarketOfferStore(ctx context.Context, db *mongo.Database) *MarketOfferStore {
	store := &MarketOfferStore{
		coll: db.Collection("MarketOffers"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MarketOfferStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on MarketOffers._id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on MarketOffers.userId",
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
			"Failed creating index on MarketOffers.itemCode",
			"error", err,
		)
		return
	}
}
