package reports

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DismantleReport is an hourly histogram of destroyed-equipment states (0-100).
type DismantleReport struct {
	ID           string         `bson:"_id"`
	HourStart    time.Time      `bson:"hourStart"`
	Count        int            `bson:"count"`
	StateBuckets map[string]int `bson:"stateBuckets"`
	UpdatedAt    time.Time      `bson:"updatedAt"`
}

// DismantleReportID is the deterministic per-hour key for idempotent upserts.
func DismantleReportID(hourStart time.Time) string {
	return hourStart.UTC().Format(time.RFC3339)
}

type DismantleReportStore struct {
	coll *mongo.Collection
}

func NewDismantleReportStore(ctx context.Context, db *mongo.Database) *DismantleReportStore {
	store := &DismantleReportStore{coll: db.Collection("dismantle_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *DismantleReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "hourStart", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on dismantle_reports.hourStart", "error", err)
	}
}

// Upsert writes an hourly dismantle report keyed on _id.
func (s *DismantleReportStore) Upsert(ctx context.Context, r DismantleReport) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: r.ID}},
		r,
		options.Replace().SetUpsert(true),
	)
	return err
}

// Get returns the hourly dismantle report for an id, or false when none exists.
func (s *DismantleReportStore) Get(ctx context.Context, id string) (*DismantleReport, bool, error) {
	var r DismantleReport
	err := s.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&r)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &r, true, nil
}
