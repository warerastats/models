package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MarketState is a point-in-time snapshot of 24h wage and market aggregates.
type MarketState struct {
	ID              string    `bson:"_id"`
	At              time.Time `bson:"at"`
	AvgWage24h      float64   `bson:"avgWage24h"`
	WageVolume24h   int       `bson:"wageVolume24h"`
	MarketVolume24h float64   `bson:"marketVolume24h"`
	WageMin         float64   `bson:"wageMin"`
	WageMax         float64   `bson:"wageMax"`
	WageAvgWeighted float64   `bson:"wageAvgWeighted"`
}

// MarketStateID is the deterministic per-snapshot key for idempotent upserts.
func MarketStateID(at time.Time) string {
	return at.UTC().Format(time.RFC3339)
}

type MarketStateStore struct {
	coll *mongo.Collection
}

func NewMarketStateStore(ctx context.Context, db *mongo.Database) *MarketStateStore {
	store := &MarketStateStore{coll: db.Collection("market_state")}
	store.ensureIndex(ctx)
	return store
}

func (s *MarketStateStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "at", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on market_state.at", "error", err)
	}
}

// Upsert writes a market-state snapshot keyed on _id.
func (s *MarketStateStore) Upsert(ctx context.Context, st MarketState) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: st.ID}},
		st,
		options.Replace().SetUpsert(true),
	)
	return err
}
