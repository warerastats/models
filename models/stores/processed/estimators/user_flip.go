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

// UserFlipEvent is a realised user flip: a buy matched to sells within 12 hours.
type UserFlipEvent struct {
	ID          bson.ObjectID `bson:"_id"`
	UserID      bson.ObjectID `bson:"userId"`
	ItemCode    string        `bson:"itemCode"`
	Quantity    int           `bson:"quantity"`
	BuyCost     float64       `bson:"buyCost"`
	SellRevenue float64       `bson:"sellRevenue"`
	Profit      float64       `bson:"profit"`
	HeldMs      int64         `bson:"heldMs"`
	At          time.Time     `bson:"at"`
}

type UserFlipEventStore struct {
	coll *mongo.Collection
}

func NewUserFlipEventStore(ctx context.Context, db *mongo.Database) *UserFlipEventStore {
	store := &UserFlipEventStore{coll: db.Collection("user_flip_events")}
	store.ensureIndex(ctx)
	return store
}

func (s *UserFlipEventStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "userId", Value: 1}, {Key: "at", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on user_flip_events.{userId,at}", "error", err)
	}
}

// Upsert writes a flip event idempotently keyed on _id (the sell trade id).
func (s *UserFlipEventStore) Upsert(ctx context.Context, e UserFlipEvent) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: e.ID}},
		e,
		options.Replace().SetUpsert(true),
	)
	return err
}

// ExistingIDs returns the set of flip-event _ids whose at falls in (since, until].
func (s *UserFlipEventStore) ExistingIDs(ctx context.Context, since, until time.Time) (map[bson.ObjectID]bool, error) {
	return existingEventIDs(ctx, s.coll, since, until)
}

// UserFlipLot is one open buy lot awaiting a matching sell within the window.
type UserFlipLot struct {
	TradeID   bson.ObjectID `bson:"tradeId"`
	Quantity  int           `bson:"quantity"`
	UnitPrice float64       `bson:"unitPrice"`
	BoughtAt  time.Time     `bson:"boughtAt"`
}

// UserFlipState holds a user's open buy lots and aggregate flip performance.
type UserFlipState struct {
	UserID      bson.ObjectID            `bson:"_id"`
	OpenLots    map[string][]UserFlipLot `bson:"openLots"`
	TotalFlips  int                      `bson:"totalFlips"`
	TotalProfit float64                  `bson:"totalProfit"`
	UpdatedAt   time.Time                `bson:"updatedAt"`
}

type UserFlipStateStore struct {
	coll *mongo.Collection
}

func NewUserFlipStateStore(ctx context.Context, db *mongo.Database) *UserFlipStateStore {
	store := &UserFlipStateStore{coll: db.Collection("user_flip_states")}
	store.ensureIndex(ctx)
	return store
}

func (s *UserFlipStateStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on user_flip_states.updatedAt", "error", err)
	}
}

// Upsert replaces a user's flip state keyed on _id.
func (s *UserFlipStateStore) Upsert(ctx context.Context, st UserFlipState) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: st.UserID}},
		st,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns a user's flip state, or false when none exists.
func (s *UserFlipStateStore) Get(ctx context.Context, userID bson.ObjectID) (*UserFlipState, bool, error) {
	var st UserFlipState
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: userID}}).Decode(&st)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &st, true, nil
}
