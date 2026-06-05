package trackers

import (
	"context"
	"encoding/json"
	"log/slog"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
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
	SpecialisationItemCode string          `bson:"specialisation,omitempty"`
	RulingPartyID          *bson.ObjectID  `bson:"rulingPartyId,omitempty"`
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

func (s *CountryStore) Get(ctx context.Context, id bson.ObjectID) (*Country, error) {
	var country Country
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&country)
	if err != nil {
		return nil, err
	}
	return &country, nil
}

func (s *CountryStore) UpsertCountry(ctx context.Context, id bson.ObjectID, data Country) error {
	data.ID = id
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}
