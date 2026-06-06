package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// CaseItemStat is the per-dropped-item summary inside a case report.
type CaseItemStat struct {
	ItemCode       string         `bson:"itemCode"`
	AvgWeighted14d float64        `bson:"avgWeighted14d"`
	TotalDrops     int            `bson:"totalDrops"`
	PerSkillRoll   map[string]int `bson:"perSkillRoll"`
}

// CasesReport is the per-case drop and value report.
type CasesReport struct {
	Case             string         `bson:"_id"`
	TotalOpened      int            `bson:"totalOpened"`
	UniqueItemCodes  []string       `bson:"uniqueItemCodes"`
	ExpectedValue14d float64        `bson:"expectedValue14d"`
	PerItem          []CaseItemStat `bson:"perItem"`
	UpdatedAt        time.Time      `bson:"updatedAt"`
}

type CasesReportStore struct {
	coll *mongo.Collection
}

func NewCasesReportStore(ctx context.Context, db *mongo.Database) *CasesReportStore {
	store := &CasesReportStore{coll: db.Collection("cases_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *CasesReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "updatedAt", Value: 1}},
	})
	if err != nil {
		slog.Error("Failed creating index on cases_reports.updatedAt", "error", err)
	}
}

// Upsert replaces a case report keyed on _id (the case code).
func (s *CasesReportStore) Upsert(ctx context.Context, r CasesReport) error {
	_, err := s.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: r.Case}},
		r,
		options.Replace().SetUpsert(true),
	)
	return err
}
