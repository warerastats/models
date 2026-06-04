package states

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type ScraperState struct {
	ID               bson.ObjectID `bson:"_id,omitempty"`
	LastTransaction  time.Time     `bson:"lastTransaction"`
	TrackBattlesFrom time.Time     `bson:"trackBattlesFrom,omitempty"`

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
			LastTransaction:  time.Time{},
			TrackBattlesFrom: time.Now().UTC(),
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

	if state.TrackBattlesFrom.IsZero() {
		now := time.Now().UTC()
		_, err := s.coll.UpdateOne(ctx,
			bson.D{{Key: "_id", Value: state.ID}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "trackBattlesFrom", Value: now}}}},
		)
		if err != nil {
			slog.Error("Failed initialising scraper_state.trackBattlesFrom", "error", err)
		} else {
			state.TrackBattlesFrom = now
		}
	}

	return &state
}

func (s *ScraperState) SetLastTransaction(ctx context.Context, t time.Time) {
	s.LastTransaction = t
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: s.ID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "lastTransaction", Value: t}}}},
	)
	if err != nil {
		slog.Error("Failed setting scraper_state.lastTransaction", "error", err)
	}
}

func (s *ScraperState) SetTrackBattlesFrom(ctx context.Context, t time.Time) {
	s.TrackBattlesFrom = t
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: s.ID}},
		bson.D{{Key: "$set", Value: bson.D{{Key: "trackBattlesFrom", Value: t}}}},
	)
	if err != nil {
		slog.Error("Failed setting scraper_state.trackBattlesFrom", "error", err)
	}
}
