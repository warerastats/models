package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Region struct {
	ID                bson.ObjectID   `bson:"_id"`
	Name              string          `bson:"name"`
	CountryID         bson.ObjectID   `bson:"countryId"`
	InitialCountryID  bson.ObjectID   `bson:"initialCountryId"`
	NeighborRegionIDs []bson.ObjectID `bson:"neighbors"`
	IsCapital         bool            `bson:"isCapital"`
	IsLinkedToCapital bool            `bson:"isLinkedToCapital"`
	Resistance        float64         `bson:"resistance"`
	MaxResistance     float64         `bson:"maxResistance"`
	LatestObject      json.RawMessage `bson:"raw"`
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
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on regions._id",
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

func (s *RegionStore) Get(ctx context.Context, id bson.ObjectID) (*Region, error) {
	var region Region
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

func (s *RegionStore) UpsertRegion(ctx context.Context, id bson.ObjectID, data Region) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}
