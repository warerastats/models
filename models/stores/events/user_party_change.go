package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserPartyChange struct {
	ID     bson.ObjectID `bson:"_id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	// nullable
	PartyID *bson.ObjectID `bson:"partyId,omitempty"`
}

type UserPartyChangeStore struct {
	coll *mongo.Collection
}

func NewUserPartyChangeStore(ctx context.Context, db *mongo.Database) *UserPartyChangeStore {
	store := &UserPartyChangeStore{
		coll: db.Collection("events_user_party_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserPartyChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_party_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserPartyChangeStore) Set(ctx context.Context, change UserPartyChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserPartyChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserPartyChange, error) {
	var change UserPartyChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
