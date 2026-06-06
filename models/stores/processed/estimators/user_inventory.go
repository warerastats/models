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

// UserInventory is a user's estimated current equipment holdings by item code.
type UserInventory struct {
	UserID    bson.ObjectID  `bson:"_id"`
	Items     map[string]int `bson:"items"`
	UpdatedAt time.Time      `bson:"updatedAt"`
}

type UserInventoryStore struct {
	coll *mongo.Collection
}

func NewUserInventoryStore(ctx context.Context, db *mongo.Database) *UserInventoryStore {
	store := &UserInventoryStore{coll: db.Collection("user_inventory_estimates")}
	store.ensureIndex(ctx)
	return store
}

func (s *UserInventoryStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on user_inventory_estimates.updatedAt", "error", err)
	}
}

// Upsert replaces a user's inventory estimate keyed on _id.
func (s *UserInventoryStore) Upsert(ctx context.Context, inv UserInventory) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: inv.UserID}},
		inv,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns a user's inventory estimate, or false when none exists.
func (s *UserInventoryStore) Get(ctx context.Context, userID bson.ObjectID) (*UserInventory, bool, error) {
	var inv UserInventory
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: userID}}).Decode(&inv)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &inv, true, nil
}
