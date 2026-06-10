package trackers

import (
	"context"
	"log/slog"
	"regexp"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Alliance is a tracker for in-game alliances discovered via country.allianceId.
type Alliance struct {
	ID   bson.ObjectID `bson:"_id"`
	Name string        `bson:"name"`
}

// AllianceStore manages the alliances collection.
type AllianceStore struct {
	coll *mongo.Collection
}

// NewAllianceStore creates the alliances store and ensures indexes.
func NewAllianceStore(ctx context.Context, db *mongo.Database) *AllianceStore {
	store := &AllianceStore{coll: db.Collection("alliances")}
	store.ensureIndex(ctx)
	return store
}

func (s *AllianceStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "name", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on alliances.name", "error", err)
	}
}

// Exists returns the subset of ids that have no tracker document yet.
func (s *AllianceStore) Exists(ctx context.Context, ids []bson.ObjectID) ([]bson.ObjectID, error) {
	cursor, err := s.coll.Find(ctx,
		bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}},
		options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	existing := make(map[bson.ObjectID]struct{}, len(ids))
	for cursor.Next(ctx) {
		var r struct {
			ID bson.ObjectID `bson:"_id"`
		}
		err = cursor.Decode(&r)
		if err != nil {
			return nil, err
		}
		existing[r.ID] = struct{}{}
	}
	if err = cursor.Err(); err != nil {
		return nil, err
	}

	var missing []bson.ObjectID
	for _, id := range ids {
		if _, ok := existing[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing, nil
}

// CreateEmpty inserts a placeholder alliance with an empty name.
func (s *AllianceStore) CreateEmpty(ctx context.Context, id bson.ObjectID) error {
	_, err := s.coll.InsertOne(ctx, Alliance{ID: id})
	return err
}

// Get returns a single alliance by id.
func (s *AllianceStore) Get(ctx context.Context, id bson.ObjectID) (*Alliance, error) {
	var a Alliance
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&a)
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// GetMany returns alliance documents for the given ids.
func (s *AllianceStore) GetMany(ctx context.Context, ids []bson.ObjectID) ([]Alliance, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cursor, err := s.coll.Find(ctx, bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: ids}}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Alliance
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// SetName updates the name of an alliance.
func (s *AllianceStore) SetName(ctx context.Context, id bson.ObjectID, name string) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "name", Value: name}}}},
	)
	return err
}

// Search returns up to limit alliances whose name contains term (case-insensitive).
func (s *AllianceStore) Search(ctx context.Context, term string, limit int) ([]Alliance, error) {
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

	var out []Alliance
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetAll returns every alliance tracker document.
func (s *AllianceStore) GetAll(ctx context.Context) ([]Alliance, error) {
	cursor, err := s.coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []Alliance
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
