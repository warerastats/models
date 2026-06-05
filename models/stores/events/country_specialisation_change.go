package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CountrySpecialisationChange struct {
	ID                     bson.ObjectID `bson:"_id,omitempty"`
	CountryID              bson.ObjectID `bson:"countryId"`
	SpecialisationItemCode *string       `bson:"itemCode,omitempty"`
}

type CountrySpecialisationChangeStore struct {
	coll *mongo.Collection
}

func NewCountrySpecialisationChangeStore(ctx context.Context, db *mongo.Database) *CountrySpecialisationChangeStore {
	store := &CountrySpecialisationChangeStore{
		coll: db.Collection("events_country_specialisation_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CountrySpecialisationChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_country_specialisation_change.countryId",
			"error", err,
		)
		return
	}
}

func (s *CountrySpecialisationChangeStore) Set(ctx context.Context, change CountrySpecialisationChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *CountrySpecialisationChangeStore) Get(ctx context.Context, countryID bson.ObjectID) (*CountrySpecialisationChange, error) {
	var change CountrySpecialisationChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "countryId", Value: countryID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
