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

type Region struct {
	ID                bson.ObjectID   `bson:"_id"`
	Name              string          `bson:"name"`
	NameLower         string          `bson:"nameLower"`
	CountryID         bson.ObjectID   `bson:"countryId"`
	InitialCountryID  bson.ObjectID   `bson:"initialCountryId"`
	NeighborRegionIDs []bson.ObjectID `bson:"neighbors"`
	IsCapital         bool            `bson:"isCapital"`
	IsLinkedToCapital bool            `bson:"isLinkedToCapital"`
	Resistance        float64         `bson:"resistance"`
	MaxResistance     float64         `bson:"maxResistance"`
	LatestObject      json.RawMessage `bson:"raw"`
	RawHash           string          `bson:"rawHash,omitempty"`
}

type RegionStore struct {
	coll *mongo.Collection
}

func NewRegionStore(ctx context.Context, db *mongo.Database) *RegionStore {
	store := &RegionStore{
		coll: db.Collection("regions"),
	}
	store.ensureIndex(ctx)
	store.migrate(ctx)
	return store
}

func (s *RegionStore) ensureIndex(ctx context.Context) {
	indexes := []mongo.IndexModel{
		{Keys: bson.D{{Key: "countryId", Value: 1}}},
		{Keys: bson.D{{Key: "nameLower", Value: 1}}},
	}
	_, err := s.coll.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		slog.Error(
			"Failed creating indexes on regions",
			"error", err,
		)
	}
}

// migrate backfills nameLower for documents written before the field existed.
func (s *RegionStore) migrate(ctx context.Context) {
	_, err := s.coll.UpdateMany(ctx,
		bson.D{{Key: "nameLower", Value: bson.D{{Key: "$exists", Value: false}}}},
		mongo.Pipeline{
			{{Key: "$set", Value: bson.D{
				{Key: "nameLower", Value: bson.D{{Key: "$toLower", Value: "$name"}}},
			}}},
		},
	)
	if err != nil {
		slog.Error("Failed backfilling regions.nameLower", "error", err)
	}
}

func (s *RegionStore) Get(ctx context.Context, id bson.ObjectID) (*Region, error) {
	var region Region
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&region)
	if err != nil {
		return nil, err
	}
	return &region, nil
}

func (s *RegionStore) UpsertRegion(ctx context.Context, id bson.ObjectID, data Region) error {
	data.ID = id
	data.NameLower = strings.ToLower(data.Name)
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

// Search returns up to limit regions whose nameLower starts with term
// (case-insensitive), ordered alphabetically. Prefix-anchored so the nameLower
// index is used.
func (s *RegionStore) Search(ctx context.Context, term string, limit int) ([]Region, error) {
	if term == "" || limit <= 0 {
		return nil, nil
	}
	pattern := "^" + regexp.QuoteMeta(strings.ToLower(term))
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "nameLower", Value: bson.D{{Key: "$regex", Value: pattern}}}},
		options.Find().
			SetSort(bson.D{{Key: "nameLower", Value: 1}}).
			SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Region
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
