package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type PartyNameChange struct {
	ID      bson.ObjectID `bson:"_id,omitempty"`
	PartyID bson.ObjectID `bson:"partyId"`
	Name    string        `bson:"name"`
}

type PartyNameChangeStore struct {
	coll *mongo.Collection
}

func NewPartyNameChangeStore(ctx context.Context, db *mongo.Database) *PartyNameChangeStore {
	store := &PartyNameChangeStore{
		coll: db.Collection("events_party_name_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyNameChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "partyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_party_name_change.partyId",
			"error", err,
		)
		return
	}
}

func (s *PartyNameChangeStore) Set(ctx context.Context, change PartyNameChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *PartyNameChangeStore) Get(ctx context.Context, partyID bson.ObjectID) (*PartyNameChange, error) {
	var change PartyNameChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "partyId", Value: partyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
