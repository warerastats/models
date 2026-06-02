package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type UserMUChange struct {
	ID     bson.ObjectID `bson:"_id,omitempty"`
	UserID bson.ObjectID `bson:"userId"`
	// nullable
	MUID *bson.ObjectID `bson:"MUId,omitempty"`
}

type UserMUChangeStore struct {
	coll *mongo.Collection
}

func NewUserMUChangeStore(ctx context.Context, db *mongo.Database) *UserMUChangeStore {
	store := &UserMUChangeStore{
		coll: db.Collection("events_user_mu_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *UserMUChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "userId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_user_mu_change.userId",
			"error", err,
		)
		return
	}
}

func (s *UserMUChangeStore) Set(ctx context.Context, change UserMUChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *UserMUChangeStore) Get(ctx context.Context, userID bson.ObjectID) (*UserMUChange, error) {
	var change UserMUChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "userId", Value: userID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
