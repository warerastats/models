package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type PartyDescriptionChange struct {
	ID          bson.ObjectID `bson:"_id,omitempty"`
	PartyID     bson.ObjectID `bson:"partyId"`
	Description string        `bson:"description"`
}

type PartyDescriptionChangeStore struct {
	coll *mongo.Collection
}

func NewPartyDescriptionChangeStore(ctx context.Context, db *mongo.Database) *PartyDescriptionChangeStore {
	store := &PartyDescriptionChangeStore{
		coll: db.Collection("events_party_description_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyDescriptionChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "partyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_party_description_change.partyId",
			"error", err,
		)
		return
	}
}

func (s *PartyDescriptionChangeStore) Set(ctx context.Context, change PartyDescriptionChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *PartyDescriptionChangeStore) Get(ctx context.Context, partyID bson.ObjectID) (*PartyDescriptionChange, error) {
	var change PartyDescriptionChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "partyId", Value: partyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
