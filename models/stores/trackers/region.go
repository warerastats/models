package trackers

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Region struct {
	ID                bson.ObjectID   `bson:"id"`
	Name              string          `bson:"name"`
	CountryID         bson.ObjectID   `bson:"countryId"`
	InitialCountryID  bson.ObjectID   `bson:"initialCountryId"`
	NeighborRegionIDs []bson.ObjectID `bson:"neighbors"`
	IsCapital         bool            `bson:"isCapital"`
	IsLinkedToCapital bool            `bson:"isLinkedToCapital"`
	Resistance        int             `bson:"resistance"`
	MaxResistance     int             `bson:"maxResistance"`
}

type RegionStore struct {
	coll *mongo.Collection
}

func NewRegionStore(ctx context.Context, db *mongo.Database) *RegionStore {
	store := &RegionStore{
		coll: db.Collection("regions"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *RegionStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on regions.id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on regions.countryId",
			"error", err,
		)
		return
	}
}
