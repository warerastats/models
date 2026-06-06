package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserCompanyChange struct {
	ID     bson.ObjectID `bson:"_id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	// nullable
	CompanyID *bson.ObjectID `bson:"companyId,omitempty"`
}

type UserCompanyChangeStore struct {
	coll *mongo.Collection
}

func NewUserCompanyChangeStore(ctx context.Context, db *mongo.Database) *UserCompanyChangeStore {
	store := &UserCompanyChangeStore{
		coll: db.Collection("events_user_company_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserCompanyChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_company_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserCompanyChangeStore) Set(ctx context.Context, change UserCompanyChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserCompanyChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserCompanyChange, error) {
	var change UserCompanyChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
