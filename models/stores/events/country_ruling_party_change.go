package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type CountryRulingPartyChange struct {
	ID        bson.ObjectID  `bson:"_id,omitempty"`
	CountryID bson.ObjectID  `bson:"countryId"`
	PartyID   *bson.ObjectID `bson:"partyId,omitempty"`
}

type CountryRulingPartyChangeStore struct {
	coll *mongo.Collection
}

func NewCountryRulingPartyChangeStore(ctx context.Context, db *mongo.Database) *CountryRulingPartyChangeStore {
	store := &CountryRulingPartyChangeStore{
		coll: db.Collection("events_country_ruling_party_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryRulingPartyChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_country_ruling_party_change.countryId",
			"error", err,
		)
		return
	}
}

func (s *CountryRulingPartyChangeStore) Set(ctx context.Context, change CountryRulingPartyChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *CountryRulingPartyChangeStore) Get(ctx context.Context, countryID bson.ObjectID) (*CountryRulingPartyChange, error) {
	var change CountryRulingPartyChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "countryId", Value: countryID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
