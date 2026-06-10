package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// CountryAllianceLeave records a country leaving an alliance.
type CountryAllianceLeave struct {
	ID             bson.ObjectID  `bson:"_id,omitempty"`
	CountryID      bson.ObjectID  `bson:"countryId"`
	PrevAllianceID *bson.ObjectID `bson:"prevAllianceId,omitempty"`
}

// CountryAllianceLeaveStore manages the events_country_alliance_leave collection.
type CountryAllianceLeaveStore struct {
	coll *mongo.Collection
}

// NewCountryAllianceLeaveStore creates the store and ensures indexes.
func NewCountryAllianceLeaveStore(ctx context.Context, db *mongo.Database) *CountryAllianceLeaveStore {
	store := &CountryAllianceLeaveStore{coll: db.Collection("events_country_alliance_leave")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryAllianceLeaveStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error("Failed creating index on events_country_alliance_leave.{countryId,_id}", "error", err)
	}
}

// Set inserts a new alliance-leave event.
func (s *CountryAllianceLeaveStore) Set(ctx context.Context, change CountryAllianceLeave) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}
