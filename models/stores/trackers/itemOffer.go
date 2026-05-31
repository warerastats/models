package trackers

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ItemOffer struct {
	ID        bson.ObjectID `bson:"id"`
	UserID    bson.ObjectID `bson:"userId"`
	ItemCode  string        `bson:"itemCode"`
	Quantity  int           `bson:"quantity"`
	Fulfilled int           `bson:"fulfilled"`
	Cancelled bool          `bson:"cancelled"`
	Price     float64       `bson:"price"`
	Since     time.Time     `bson:"since"`
}

type ItemOfferStore struct {
	coll *mongo.Collection
}

func NewItemOfferStore(ctx context.Context, db *mongo.Database) *ItemOfferStore {
	store := &ItemOfferStore{
		coll: db.Collection("itemOffers"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *ItemOfferStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on itemOffers.id",
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
			"Failed creating index on itemOffers.userId",
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
			"Failed creating index on itemOffers.itemCode",
			"error", err,
		)
		return
	}
}
