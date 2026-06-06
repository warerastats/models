package states

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// JobState is a per-job processing watermark for incremental, resumable passes.
type JobState struct {
	Name      string         `bson:"_id"`
	Boundary  time.Time      `bson:"boundary,omitempty"`
	LastID    *bson.ObjectID `bson:"lastId,omitempty"`
	UpdatedAt time.Time      `bson:"updatedAt"`
}

type JobStateStore struct {
	coll *mongo.Collection
}

func NewJobStateStore(ctx context.Context, db *mongo.Database) *JobStateStore {
	store := &JobStateStore{
		coll: db.Collection("processor_job_state"),
	}
	store.ensureIndex(ctx)
	return store
}

func (s *JobStateStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "boundary", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on processor_job_state.boundary", "error", err)
	}
}

// Get returns the watermark for name, creating a zero-value document on first use.
func (s *JobStateStore) Get(ctx context.Context, name string) (*JobState, error) {
	var state JobState
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: name}}).Decode(&state)
	switch {
	case err == nil:
		return &state, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		state = JobState{Name: name}
		_, insErr := s.coll.InsertOne(ctx, state)
		if insErr != nil && !mongo.IsDuplicateKeyError(insErr) {
			return nil, insErr
		}
		return &state, nil
	default:
		return nil, err
	}
}

// SetWatermark advances the watermark for name; lastID may be nil for window-based jobs.
func (s *JobStateStore) SetWatermark(ctx context.Context, name string, boundary time.Time, lastID *bson.ObjectID) error {
	set := bson.D{
		{Key: "boundary", Value: boundary},
		{Key: "updatedAt", Value: time.Now().UTC()},
	}
	if lastID != nil {
		set = append(set, bson.E{Key: "lastId", Value: lastID})
	}
	_, err := s.coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: name}},
		bson.D{{Key: "$set", Value: set}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}
