package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserNameChange struct {
	ID            bson.ObjectID `bson:"_id,omitempty"`
	UserID        bson.ObjectID `bson:"userId"`
	Username      string        `bson:"username"`
	UsernameLower string        `bson:"usernameLower"`
}

type UserNameChangeStore struct {
	coll *mongo.Collection
}

func NewUserNameChangeStore(ctx context.Context, db *mongo.Database) *UserNameChangeStore {
	store := &UserNameChangeStore{
		coll: db.Collection("events_user_name_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserNameChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_name_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserNameChangeStore) Set(ctx context.Context, change UserNameChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserNameChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserNameChange, error) {
	var change UserNameChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
