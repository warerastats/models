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

// InventoryLot is one FIFO purchase lot of a fungible item held by a country.
type InventoryLot struct {
	Quantity  int       `bson:"quantity"`
	UnitPrice float64   `bson:"unitPrice"`
	BoughtAt  time.Time `bson:"boughtAt"`
}

// CountryInventory holds a country's open FIFO lots per fungible item code.
type CountryInventory struct {
	CountryID bson.ObjectID             `bson:"_id"`
	Lots      map[string][]InventoryLot `bson:"lots"`
	UpdatedAt time.Time                 `bson:"updatedAt"`
}

type CountryInventoryStore struct {
	coll *mongo.Collection
}

func NewCountryInventoryStore(ctx context.Context, db *mongo.Database) *CountryInventoryStore {
	store := &CountryInventoryStore{coll: db.Collection("country_inventory_estimates")}
	store.ensureIndex(ctx)
	return store
}

func (s *CountryInventoryStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on country_inventory_estimates.updatedAt", "error", err)
	}
}

// Upsert replaces a country's inventory estimate keyed on _id.
func (s *CountryInventoryStore) Upsert(ctx context.Context, inv CountryInventory) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: inv.CountryID}},
		inv,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns a country's inventory estimate, or false when none exists.
func (s *CountryInventoryStore) Get(ctx context.Context, countryID bson.ObjectID) (*CountryInventory, bool, error) {
	var inv CountryInventory
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: countryID}}).Decode(&inv)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &inv, true, nil
}
