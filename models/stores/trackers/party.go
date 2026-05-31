package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Party struct {
	ID            bson.ObjectID   `bson:"id"`
	Name          string          `bson:"name"`
	Description   string          `bson:"description"`
	CountryID     bson.ObjectID   `bson:"countryId"`
	RegionID      bson.ObjectID   `bson:"regionId"`
	LeaderUserID  bson.ObjectID   `bson:"leaderId"`
	MemberUserIDs []bson.ObjectID `bson:"members"`
	AvatarUrl     string          `bson:"avatarUrl"`
	Ethics        struct {
		Militarism    int `bson:"militarism"`
		Isolationism  int `bson:"isolationism"`
		Imperialism   int `bson:"imperialism"`
		Industrialism int `bson:"industrialism"`
	} `bson:"ethics"`
	LatestObject json.RawMessage `bson:"raw"`
}

type PartyStore struct {
	coll *mongo.Collection
}

func NewPartyStore(ctx context.Context, db *mongo.Database) *PartyStore {
	store := &PartyStore{
		coll: db.Collection("parties"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *PartyStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on parties.id",
			"error", err,
		)
		return
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "countryId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on parties.countryId",
			"error", err,
		)
		return
	}
}
