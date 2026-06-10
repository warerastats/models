package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type PartyEthicsChange struct {
	ID      bson.ObjectID `bson:"_id,omitempty"`
	PartyID bson.ObjectID `bson:"partyId"`
	Ethics  struct {
		Unethical     bool `bson:"unethical"`
		Militarism    int  `bson:"militarism"`
		Isolationism  int  `bson:"isolationism"`
		Imperialism   int  `bson:"imperialism"`
		Industrialism int  `bson:"industrialism"`
	} `bson:"ethics"`
}

type PartyEthicsChangeStore struct {
	coll *mongo.Collection
}

func NewPartyEthicsChangeStore(ctx context.Context, db *mongo.Database) *PartyEthicsChangeStore {
	store := &PartyEthicsChangeStore{
		coll: db.Collection("events_party_ethics_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyEthicsChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "partyId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_party_ethics_change.partyId",
			"error", err,
		)
		return
	}
}

func (s *PartyEthicsChangeStore) Set(ctx context.Context, change PartyEthicsChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *PartyEthicsChangeStore) Get(ctx context.Context, partyID bson.ObjectID) (*PartyEthicsChange, error) {
	var change PartyEthicsChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "partyId", Value: partyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
