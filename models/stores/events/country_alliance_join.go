package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CountryAllianceJoin records a country joining (or switching to) an alliance.
type CountryAllianceJoin struct {
	ID         bson.ObjectID `bson:"_id,omitempty"`
	CountryID  bson.ObjectID `bson:"countryId"`
	AllianceID bson.ObjectID `bson:"allianceId"`
}

// CountryAllianceJoinStore manages the events_country_alliance_join collection.
type CountryAllianceJoinStore struct {
	coll *mongo.Collection
}

// NewCountryAllianceJoinStore creates the store and ensures indexes.
func NewCountryAllianceJoinStore(ctx context.Context, db *mongo.Database) *CountryAllianceJoinStore {
	store := &CountryAllianceJoinStore{coll: db.Collection("events_country_alliance_join")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryAllianceJoinStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error("Failed creating index on events_country_alliance_join.{countryId,_id}", "error", err)
	}
}

// Set inserts a new alliance-join event.
func (s *CountryAllianceJoinStore) Set(ctx context.Context, change CountryAllianceJoin) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}
