package trackers

import (
	"context"
	"encoding/json"
	"log/slog"
	"regexp"
	"strings"

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
	SpecialisationItemCode *string         `bson:"specialisation,omitempty"`
	RulingPartyID          *bson.ObjectID  `bson:"rulingPartyId,omitempty"`
	AllianceID             *bson.ObjectID  `bson:"allianceId,omitempty"`
	LatestObject           json.RawMessage `bson:"raw"`
	RawHash                string          `bson:"rawHash,omitempty"`
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
			{Key: "rulingPartyId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on countries.rulingPartyId",
			"error", err,
		)
	}

	_, err = s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "allianceId", Value: 1},
		},
	})
	if err != nil {
		slog.Error(
			"Failed creating index on countries.allianceId",
			"error", err,
		)
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
	hash := hashRaw(data.LatestObject)
	data.RawHash = hash

	skip, err := rawUnchanged(ctx, s.coll, id, hash)
	if err != nil {
		return err
	} else if skip {
		return nil
	}

	_, err = s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		data,
		options.Replace().SetUpsert(true),
	)
	return err
}

// DistinctRulingPartyIDs returns the set of non-nil rulingPartyId values across
// all tracked countries. Used by the party refresh scheduler to guarantee every
// ruling party is re-fetched on a fixed cadence.
func (s *CountryStore) DistinctRulingPartyIDs(ctx context.Context) ([]bson.ObjectID, error) {
	res := s.coll.Distinct(ctx, "rulingPartyId", bson.D{
		{Key: "rulingPartyId", Value: bson.D{{Key: "$ne", Value: nil}}},
	})
	err := res.Err()
	if err != nil {
		return nil, err
	}

	var ids []bson.ObjectID
	err = res.Decode(&ids)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// Search returns up to limit countries whose name contains term
// (case-insensitive), ordered alphabetically. Countries are few, so an
// unindexed scan is acceptable here.
func (s *CountryStore) Search(ctx context.Context, term string, limit int) ([]Country, error) {
	if term == "" || limit <= 0 {
		return nil, nil
	}
	pattern := regexp.QuoteMeta(strings.TrimSpace(term))
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "name", Value: bson.D{
			{Key: "$regex", Value: pattern},
			{Key: "$options", Value: "i"},
		}}},
		options.Find().
			SetSort(bson.D{{Key: "name", Value: 1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Country
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
