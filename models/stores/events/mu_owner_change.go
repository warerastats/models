package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MuOwnerChange struct {
	ID          bson.ObjectID `bson:"_id,omitempty"`
	MuID        bson.ObjectID `bson:"muId"`
	OwnerUserID bson.ObjectID `bson:"ownerUserId"`
}

type MuOwnerChangeStore struct {
	coll *mongo.Collection
}

func NewMuOwnerChangeStore(ctx context.Context, db *mongo.Database) *MuOwnerChangeStore {
	store := &MuOwnerChangeStore{
		coll: db.Collection("events_mu_owner_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MuOwnerChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "muId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_mu_owner_change.muId",
			"error", err,
		)
		return
	}
}

func (s *MuOwnerChangeStore) Set(ctx context.Context, change MuOwnerChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *MuOwnerChangeStore) Get(ctx context.Context, muID bson.ObjectID) (*MuOwnerChange, error) {
	var change MuOwnerChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "muId", Value: muID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
