package candles

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ItemCandle is a closed OHLC window for one fungible item code from trade_transactions.
type ItemCandle struct {
	ID          string    `bson:"_id"`
	ItemCode    string    `bson:"itemCode"`
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

// ItemCandleID is the deterministic composite key for idempotent upserts.
func ItemCandleID(itemCode string, bucketStart time.Time) string {
	return itemCode + "@" + bucketStart.UTC().Format(time.RFC3339)
}

type ItemCandleStore struct {
	coll *mongo.Collection
}

func NewItemCandleStore(ctx context.Context, db *mongo.Database) *ItemCandleStore {
	store := &ItemCandleStore{
		coll: db.Collection("item_candles"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *ItemCandleStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{
			{Key: "itemCode", Value: 1},
			{Key: "bucketStart", Value: 1},
		},
	})
	if err != nil {
		slog.Error("Failed creating compound index on item_candles.{itemCode,bucketStart}", "error", err)
	}
}

// BulkUpsert writes a batch of candles as idempotent upserts keyed on _id.
func (s *ItemCandleStore) BulkUpsert(ctx context.Context, candles []ItemCandle) error {
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
