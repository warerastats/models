package estimators

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// InflationPoint is the daily inflation index and its per-item input prices.
type InflationPoint struct {
	ID         string             `bson:"_id"`
	DayStart   time.Time          `bson:"dayStart"`
	IndexValue float64            `bson:"indexValue"`
	PctChange  float64            `bson:"pctChange24h"`
	PerItem    map[string]float64 `bson:"perItem"`
}

// InflationPointID is the deterministic per-day key for idempotent upserts.
func InflationPointID(dayStart time.Time) string {
	return dayStart.UTC().Format("2006-01-02")
}

type InflationStore struct {
	coll *mongo.Collection
}

func NewInflationStore(ctx context.Context, db *mongo.Database) *InflationStore {
	store := &InflationStore{coll: db.Collection("inflation_history")}
	store.ensureIndex(ctx)
	return store
}

func (s *InflationStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "dayStart", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on inflation_history.dayStart", "error", err)
	}
}

// Upsert writes a daily inflation point keyed on _id.
func (s *InflationStore) Upsert(ctx context.Context, p InflationPoint) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: p.ID}},
		p,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns the inflation point for a day, or false when none exists.
func (s *InflationStore) Get(ctx context.Context, dayStart time.Time) (*InflationPoint, bool, error) {
	var p InflationPoint
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: InflationPointID(dayStart)}}).Decode(&p)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &p, true, nil
}
