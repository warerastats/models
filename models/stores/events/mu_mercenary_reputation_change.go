package events

import (
	"context"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MuMercenaryReputationChange struct {
	ID                  bson.ObjectID `bson:"_id,omitempty"`
	MuID                bson.ObjectID `bson:"muId"`
	MercenaryReputation float64       `bson:"mercRep"`
}

type MuMercenaryReputationChangeStore struct {
	coll *mongo.Collection
}

func NewMuMercenaryReputationChangeStore(ctx context.Context, db *mongo.Database) *MuMercenaryReputationChangeStore {
	store := &MuMercenaryReputationChangeStore{
		coll: db.Collection("events_mu_mercenary_reputation_change"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *MuMercenaryReputationChangeStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "muId", Value: 1},
			{Key: "_id", Value: -1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on events_mu_mercenary_reputation_change.muId",
			"error", err,
		)
		return
	}
}

func (s *MuMercenaryReputationChangeStore) Set(ctx context.Context, change MuMercenaryReputationChange) error {
	_, err := s.coll.InsertOne(ctx, change)
	return err
}

func (s *MuMercenaryReputationChangeStore) Get(ctx context.Context, muID bson.ObjectID) (*MuMercenaryReputationChange, error) {
	var change MuMercenaryReputationChange
	err := s.coll.FindOne(ctx,
		bson.D{{Key: "muId", Value: muID}},
		options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}),
	).Decode(&change)
	if err != nil {
		return nil, err
	}
	return &change, nil
}
