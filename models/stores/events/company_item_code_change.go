package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CompanyItemCodeChange struct {
	ID        bson.ObjectID `bson:"_id,omitempty"`
	CompanyID bson.ObjectID `bson:"companyId"`
	ItemCode  string        `bson:"itemCode"`
}

type CompanyItemCodeChangeStore struct {
	coll *mongo.Collection
}

func NewCompanyItemCodeChangeStore(ctx context.Context, db *mongo.Database) *CompanyItemCodeChangeStore {
	store := &CompanyItemCodeChangeStore{
		coll: db.Collection("events_company_item_code_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CompanyItemCodeChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "companyId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_company_item_code_change.companyId",
			"error", err,
		)
		return
	}
}

func (s *CompanyItemCodeChangeStore) Set(ctx context.Context, change CompanyItemCodeChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *CompanyItemCodeChangeStore) Get(ctx context.Context, companyID bson.ObjectID) (*CompanyItemCodeChange, error) {
	var change CompanyItemCodeChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "companyId", Value: companyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
