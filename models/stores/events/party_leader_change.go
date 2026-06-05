package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type PartyLeaderChange struct {
	ID           bson.ObjectID `bson:"_id,omitempty"`
	PartyID      bson.ObjectID `bson:"partyId"`
	LeaderUserID bson.ObjectID `bson:"leaderUserId"`
}

type PartyLeaderChangeStore struct {
	coll *mongo.Collection
}

func NewPartyLeaderChangeStore(ctx context.Context, db *mongo.Database) *PartyLeaderChangeStore {
	store := &PartyLeaderChangeStore{
		coll: db.Collection("events_party_leader_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyLeaderChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "partyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_party_leader_change.partyId",
			"error", err,
		)
		return
	}
}

func (s *PartyLeaderChangeStore) Set(ctx context.Context, change PartyLeaderChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *PartyLeaderChangeStore) Get(ctx context.Context, partyID bson.ObjectID) (*PartyLeaderChange, error) {
	var change PartyLeaderChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "partyId", Value: partyID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
