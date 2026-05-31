package states

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ScraperState struct {
	ID              bson.ObjectID `bson:"id,omitempty"`
	LastTransaction time.Time     `bson:"lastTransaction"`

	coll *mongo.Collection
}

type ScraperStateStore struct {
	coll *mongo.Collection
}

func NewScraperStateStore(ctx context.Context, db *mongo.Database) *ScraperStateStore {
	store := &ScraperStateStore{
		coll: db.Collection("scraper_state"),
	}
	store.init(ctx)
	return store
}

func (s *ScraperStateStore) init(ctx context.Context) {
	count, err := s.coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		slog.Error("Failed checking scraper_state existence", "error", err)
		return
	}
	if count == 0 {
		_, err := s.coll.InsertOne(ctx, ScraperState{
			LastTransaction: time.Time{},
		})
		if err != nil {
			slog.Error("Failed inserting initial scraper_state", "error", err)
		}
	}
}

func (s *ScraperStateStore) Get(ctx context.Context) *ScraperState {
	var state ScraperState
	err := s.coll.FindOne(ctx, bson.D{}).Decode(&state)
	if err != nil {
		slog.Error("Failed getting scraper_state", "error", err)
		return nil
	}
	state.coll = s.coll
	return &state
}

func (s *ScraperState) Set(ctx context.Context) {
	_, err := s.coll.ReplaceOne(ctx, bson.D{{Key: "_id", Value: s.ID}}, s)
	if err != nil {
		slog.Error("Failed setting scraper_state", "error", err)
	}
}
