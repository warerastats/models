package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type RegionStrategicResource struct {
	ID       bson.ObjectID `bson:"_id,omitempty"`
	RegionID bson.ObjectID `bson:"regionId"`
	Resource *string       `bson:"resource,omitempty"`
}

type RegionStrategicResourceStore struct {
	coll *mongo.Collection
}

func NewRegionStrategicResourceStore(ctx context.Context, db *mongo.Database) *RegionStrategicResourceStore {
	store := &RegionStrategicResourceStore{
		coll: db.Collection("events_region_strategic_resource"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *RegionStrategicResourceStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "regionId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_region_strategic_resource.regionId",
			"error", err,
		)
		return
	}
}

func (s *RegionStrategicResourceStore) Set(ctx context.Context, change RegionStrategicResource) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *RegionStrategicResourceStore) Get(ctx context.Context, regionID bson.ObjectID) (*RegionStrategicResource, error) {
	var change RegionStrategicResource
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "regionId", Value: regionID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
