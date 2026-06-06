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

// ItemAvg is a per-item volume-weighted price over a window.
type ItemAvg struct {
	ItemCode    string  `bson:"_id"`
	WeightedAvg float64 `bson:"weightedAvg"`
	Volume      int     `bson:"volume"`
}

// WeightedAvgByItem returns the volume-weighted average price per item code
// across candles whose bucketStart falls in [since, until).
func (s *ItemCandleStore) WeightedAvgByItem(ctx context.Context, since, until time.Time) ([]ItemAvg, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "bucketStart", Value: bson.D{{Key: "$gte", Value: since}, {Key: "$lt", Value: until}}},
		}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$itemCode"},
			{Key: "money", Value: bson.D{{Key: "$sum", Value: "$money"}}},
			{Key: "volume", Value: bson.D{{Key: "$sum", Value: "$volume"}}},
		}}},
		{{Key: "$project", Value: bson.D{
			{Key: "volume", Value: 1},
			{Key: "weightedAvg", Value: bson.D{{Key: "$cond", Value: bson.A{
				bson.D{{Key: "$gt", Value: bson.A{"$volume", 0}}},
				bson.D{{Key: "$divide", Value: bson.A{"$money", "$volume"}}},
				0,
			}}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []ItemAvg
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ItemStats is per-item OHLC/volume rollup over a window of candles.
type ItemStats struct {
	ItemCode string  `bson:"_id"`
	Open     float64 `bson:"open"`
	Close    float64 `bson:"close"`
	High     float64 `bson:"high"`
	Low      float64 `bson:"low"`
	Volume   int     `bson:"volume"`
	Money    float64 `bson:"money"`
}

// StatsByItem returns per-item OHLC/volume stats across candles whose
// bucketStart falls in [since, until), ordered open=first, close=last.
func (s *ItemCandleStore) StatsByItem(ctx context.Context, since, until time.Time) ([]ItemStats, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "bucketStart", Value: bson.D{{Key: "$gte", Value: since}, {Key: "$lt", Value: until}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "bucketStart", Value: 1}}}},
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$itemCode"},
			{Key: "open", Value: bson.D{{Key: "$first", Value: "$open"}}},
			{Key: "close", Value: bson.D{{Key: "$last", Value: "$close"}}},
			{Key: "high", Value: bson.D{{Key: "$max", Value: "$high"}}},
			{Key: "low", Value: bson.D{{Key: "$min", Value: "$low"}}},
			{Key: "volume", Value: bson.D{{Key: "$sum", Value: "$volume"}}},
			{Key: "money", Value: bson.D{{Key: "$sum", Value: "$money"}}},
		}}},
	}
	cursor, err := s.coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var out []ItemStats
	err = cursor.All(ctx, &out)
	if err != nil {
		return nil, err
	}
	return out, nil
}
