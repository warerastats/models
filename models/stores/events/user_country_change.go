package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserCountryChange struct {
	ID     bson.ObjectID `bson:"_id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	// nullable
	CountryID *bson.ObjectID `bson:"countryId,omitempty"`
}

type UserCountryChangeStore struct {
	coll *mongo.Collection
}

func NewUserCountryChangeStore(ctx context.Context, db *mongo.Database) *UserCountryChangeStore {
	store := &UserCountryChangeStore{
		coll: db.Collection("events_user_country_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserCountryChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_country_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserCountryChangeStore) Set(ctx context.Context, change UserCountryChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserCountryChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserCountryChange, error) {
	var change UserCountryChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
