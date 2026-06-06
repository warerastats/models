package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// OrderbookLevel is one flattened price level (summed quantity at a price).
type OrderbookLevel struct {
	Price    float64 `bson:"price"`
	Quantity int     `bson:"quantity"`
}

// EffectivePrice is the average fill price to trade a given size against the book.
type EffectivePrice struct {
	Size     int     `bson:"size"`
	AvgPrice float64 `bson:"avgPrice"`
}

// ItemMarketReport is the per-fungible-item market report including orderbook.
type ItemMarketReport struct {
	ItemCode       string           `bson:"_id"`
	Volume24h      int              `bson:"volume24h"`
	AvgWeighted24h float64          `bson:"avgWeighted24h"`
	PctChange24h   float64          `bson:"pctChange24h"`
	Low24h         float64          `bson:"low24h"`
	High24h        float64          `bson:"high24h"`
	Bids           []OrderbookLevel `bson:"bids"`
	Asks           []OrderbookLevel `bson:"asks"`
	Spread         float64          `bson:"spread"`
	EffectiveBuy   []EffectivePrice `bson:"effectiveBuy"`
	EffectiveSell  []EffectivePrice `bson:"effectiveSell"`
	UpdatedAt      time.Time        `bson:"updatedAt"`
}

type ItemMarketReportStore struct {
	coll *mongo.Collection
}

func NewItemMarketReportStore(ctx context.Context, db *mongo.Database) *ItemMarketReportStore {
	store := &ItemMarketReportStore{coll: db.Collection("item_market_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *ItemMarketReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on item_market_reports.updatedAt", "error", err)
	}
}

// Upsert replaces an item's market report keyed on _id (itemCode).
func (s *ItemMarketReportStore) Upsert(ctx context.Context, r ItemMarketReport) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: r.ItemCode}},
		r,
		options.Replace().SetUpsert(true),
	)
	return err
}
