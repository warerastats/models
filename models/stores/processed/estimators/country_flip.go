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

// CountryFlipEvent is a single realised take-profit (or loss) sell by a country.
type CountryFlipEvent struct {
	ID          bson.ObjectID `bson:"_id"`
	CountryID   bson.ObjectID `bson:"countryId"`
	ItemCode    string        `bson:"itemCode"`
	Quantity    int           `bson:"quantity"`
	BuyCost     float64       `bson:"buyCost"`
	SellRevenue float64       `bson:"sellRevenue"`
	Profit      float64       `bson:"profit"`
	At          time.Time     `bson:"at"`
}

type CountryFlipEventStore struct {
	coll *mongo.Collection
}

func NewCountryFlipEventStore(ctx context.Context, db *mongo.Database) *CountryFlipEventStore {
	store := &CountryFlipEventStore{coll: db.Collection("country_flip_events")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryFlipEventStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "countryId", Value: 1}, {Key: "at", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on country_flip_events.{countryId,at}", "error", err)
	}
}

// Upsert writes a flip event idempotently keyed on _id (the sell trade id).
func (s *CountryFlipEventStore) Upsert(ctx context.Context, e CountryFlipEvent) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: e.ID}},
		e,
		options.Replace().SetUpsert(true),
	)
	return err
}

// ExistingIDs returns the set of flip-event _ids whose at falls in (since, until].
func (s *CountryFlipEventStore) ExistingIDs(ctx context.Context, since, until time.Time) (map[bson.ObjectID]bool, error) {
	return existingEventIDs(ctx, s.coll, since, until)
}

// CountryFlipState aggregates a country's flip performance for line graphs.
type CountryFlipState struct {
	CountryID   bson.ObjectID `bson:"_id"`
	TotalTrades int           `bson:"totalTrades"`
	Profitable  int           `bson:"profitable"`
	TotalProfit float64       `bson:"totalProfit"`
	UpdatedAt   time.Time     `bson:"updatedAt"`
}

type CountryFlipStateStore struct {
	coll *mongo.Collection
}

func NewCountryFlipStateStore(ctx context.Context, db *mongo.Database) *CountryFlipStateStore {
	store := &CountryFlipStateStore{coll: db.Collection("country_flip_states")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryFlipStateStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on country_flip_states.updatedAt", "error", err)
	}
}

// Upsert replaces a country's flip state keyed on _id.
func (s *CountryFlipStateStore) Upsert(ctx context.Context, st CountryFlipState) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: st.CountryID}},
		st,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns a country's flip state, or false when none exists.
func (s *CountryFlipStateStore) Get(ctx context.Context, countryID bson.ObjectID) (*CountryFlipState, bool, error) {
	var st CountryFlipState
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: countryID}}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &st, true, nil
}
