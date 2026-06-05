package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Company struct {
	ID           bson.ObjectID   `bson:"_id"`
	UserID       bson.ObjectID   `bson:"userId"`
	RegionID     bson.ObjectID   `bson:"regionId"`
	ItemCode     string          `bson:"itemCode"`
	Name         string          `bson:"name"`
	LatestObject json.RawMessage `bson:"raw"`
}

type CompanyStore struct {
	coll *mongo.Collection
}

func NewCompanyStore(ctx context.Context, db *mongo.Database) *CompanyStore {
	store := &CompanyStore{
		coll: db.Collection("companies"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CompanyStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on companies._id",
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
			"Failed creating index on companies.userId",
			"error", err,
		)
		return
	}
}

func (s *CompanyStore) Get(ctx context.Context, id bson.ObjectID) (*Company, error) {
	var company Company
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&company)
	if err != nil {
		return nil, err
	}
	return &company, nil
}

func (s *CompanyStore) Upsert(ctx context.Context, id bson.ObjectID, data Company) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}
