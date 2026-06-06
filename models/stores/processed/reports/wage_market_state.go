package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// WagePaidUser is one entry in a most/least-paid leaderboard.
type WagePaidUser struct {
	UserID    bson.ObjectID `bson:"userId"`
	TotalPaid float64       `bson:"totalPaid"`
}

// WageMarketState is a snapshot of 14d wage aggregates and 24h pay leaderboards.
type WageMarketState struct {
	ID             string         `bson:"_id"`
	At             time.Time      `bson:"at"`
	AvgWeighted14d float64        `bson:"avgWeighted14d"`
	Min14d         float64        `bson:"min14d"`
	Max14d         float64        `bson:"max14d"`
	TotalPaid14d   float64        `bson:"totalPaid14d"`
	Top10Least24h  []WagePaidUser `bson:"top10Least24h"`
	Top10Most24h   []WagePaidUser `bson:"top10Most24h"`
}

// WageMarketStateID is the deterministic per-snapshot key for idempotent upserts.
func WageMarketStateID(at time.Time) string {
	return at.UTC().Format(time.RFC3339)
}

type WageMarketStateStore struct {
	coll *mongo.Collection
}

func NewWageMarketStateStore(ctx context.Context, db *mongo.Database) *WageMarketStateStore {
	store := &WageMarketStateStore{coll: db.Collection("wage_market_state")}
	store.ensureIndex(ctx)
	return store
}

func (s *WageMarketStateStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "at", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on wage_market_state.at", "error", err)
	}
}

// Upsert writes a wage-market-state snapshot keyed on _id.
func (s *WageMarketStateStore) Upsert(ctx context.Context, st WageMarketState) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: st.ID}},
		st,
		options.Replace().SetUpsert(true),
	)
	return err
}
