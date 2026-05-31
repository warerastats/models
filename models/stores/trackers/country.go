package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Country struct {
	ID    bson.ObjectID `bson:"_id"`
	Name  string        `bson:"name"`
	Code  string        `bson:"code"`
	Money float64       `bson:"money"`
	Taxes struct {
		Income   float64 `bson:"income"`
		Market   float64 `bson:"market"`
		SelfWork float64 `bson:"selfWork"`
	} `bson:"taxes"`
	SpecialisationItemCode string          `bson:"specialisation"`
	RulingPartyID          bson.ObjectID   `bson:"rulingPartyId"`
	LatestObject           json.RawMessage `bson:"raw"`
}

type CountryStore struct {
	coll *mongo.Collection
}

func NewCountryStore(ctx context.Context, db *mongo.Database) *CountryStore {
	store := &CountryStore{
		coll: db.Collection("countries"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "_id", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on countries._id",
			"error", err,
		)
		return
	}
}
