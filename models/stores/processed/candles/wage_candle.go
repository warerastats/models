package candles

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// WageCandle is a closed OHLC window of the wage market (rate = money/productionPoints).
type WageCandle struct {
	ID          string    `bson:"_id"`
	BucketStart time.Time `bson:"bucketStart"`
	Open        float64   `bson:"open"`
	High        float64   `bson:"high"`
	Low         float64   `bson:"low"`
	Close       float64   `bson:"close"`
	Avg         float64   `bson:"avg"`
	Volume      int       `bson:"volume"`
	Money       float64   `bson:"money"`
	Count       int       `bson:"count"`
}

// WageCandleID is the deterministic key so re-running a window is idempotent.
func WageCandleID(bucketStart time.Time) string {
	return bucketStart.UTC().Format(time.RFC3339)
}

type WageCandleStore struct {
	coll *mongo.Collection
}

func NewWageCandleStore(ctx context.Context, db *mongo.Database) *WageCandleStore {
	store := &WageCandleStore{
		coll: db.Collection("wage_candles"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *WageCandleStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "bucketStart", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on wage_candles.bucketStart", "error", err)
	}
}

// BulkUpsert writes a batch of candles as idempotent upserts keyed on _id.
func (s *WageCandleStore) BulkUpsert(ctx context.Context, candles []WageCandle) error {
	if len(candles) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(candles))
	for i := range candles {
		ops[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: candles[i].ID}}).
			SetReplacement(candles[i]).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}
