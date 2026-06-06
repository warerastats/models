package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type RegionOwnerChange struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	RegionID  bson.ObjectID `bson:"regionId"`
	CountryID bson.ObjectID `bson:"countryId"`
}

type RegionOwnerChangeStore struct {
	coll *mongo.Collection
}

func NewRegionOwnerChangeStore(ctx context.Context, db *mongo.Database) *RegionOwnerChangeStore {
	store := &RegionOwnerChangeStore{
		coll: db.Collection("events_region_owner_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *RegionOwnerChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "regionId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_region_owner_change.regionId",
			"error", err,
		)
		return
	}
}

func (s *RegionOwnerChangeStore) Set(ctx context.Context, change RegionOwnerChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *RegionOwnerChangeStore) Get(ctx context.Context, regionID bson.ObjectID) (*RegionOwnerChange, error) {
	var change RegionOwnerChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "regionId", Value: regionID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
