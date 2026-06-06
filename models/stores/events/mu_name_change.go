package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MuNameChange struct {
	ID   bson.ObjectID `bson:"_id,omitempty"`
	MuID bson.ObjectID `bson:"muId"`
	Name string        `bson:"name"`
}

type MuNameChangeStore struct {
	coll *mongo.Collection
}

func NewMuNameChangeStore(ctx context.Context, db *mongo.Database) *MuNameChangeStore {
	store := &MuNameChangeStore{
		coll: db.Collection("events_mu_name_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MuNameChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "muId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_mu_name_change.muId",
			"error", err,
		)
		return
	}
}

func (s *MuNameChangeStore) Set(ctx context.Context, change MuNameChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *MuNameChangeStore) Get(ctx context.Context, muID bson.ObjectID) (*MuNameChange, error) {
	var change MuNameChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "muId", Value: muID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
