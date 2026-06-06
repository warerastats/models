package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CompanyRegionChange struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	CompanyID bson.ObjectID `bson:"companyId"`
	RegionID  bson.ObjectID `bson:"regionId"`
}

type CompanyRegionChangeStore struct {
	coll *mongo.Collection
}

func NewCompanyRegionChangeStore(ctx context.Context, db *mongo.Database) *CompanyRegionChangeStore {
	store := &CompanyRegionChangeStore{
		coll: db.Collection("events_company_region_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CompanyRegionChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "companyId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_company_region_change.companyId",
			"error", err,
		)
		return
	}
}

func (s *CompanyRegionChangeStore) Set(ctx context.Context, change CompanyRegionChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *CompanyRegionChangeStore) Get(ctx context.Context, companyID bson.ObjectID) (*CompanyRegionChange, error) {
	var change CompanyRegionChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "companyId", Value: companyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
