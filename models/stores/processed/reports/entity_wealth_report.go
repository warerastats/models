package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// EntityWealthReport is a per-24h country/MU/party damage and wealth roll-up.
type EntityWealthReport struct {
	ID          string        `bson:"_id"`
	EntityType  string        `bson:"entityType"`
	EntityID    bson.ObjectID `bson:"entityId"`
	DayStart    time.Time     `bson:"dayStart"`
	MemberCount int           `bson:"memberCount"`
	TotalDamage int64         `bson:"totalDamage"`
	TotalWealth float64       `bson:"totalWealth"`
	WagesPaid   float64       `bson:"wagesPaid"`
	WagesEarned float64       `bson:"wagesEarned"`
}

// EntityWealthReportID is the deterministic per-entity-per-day key.
func EntityWealthReportID(entityType string, entityID bson.ObjectID, dayStart time.Time) string {
	return entityType + "@" + entityID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

type EntityWealthReportStore struct {
	coll *mongo.Collection
}

func NewEntityWealthReportStore(ctx context.Context, db *mongo.Database) *EntityWealthReportStore {
	store := &EntityWealthReportStore{coll: db.Collection("entity_wealth_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *EntityWealthReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "entityType", Value: 1}, {Key: "entityId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on entity_wealth_reports.{entityType,entityId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-entity daily wealth reports keyed on _id.
func (s *EntityWealthReportStore) Upsert(ctx context.Context, rows []EntityWealthReport) error {
	if len(rows) == 0 {
		return nil
	}
	ops := make([]mongo.WriteModel, len(rows))
	for i := range rows {
		ops[i] = mongo.NewReplaceOneModel().
			SetFilter(bson.D{{Key: "_id", Value: rows[i].ID}}).
			SetReplacement(rows[i]).
			SetUpsert(true)
	}
	_, err := s.coll.BulkWrite(ctx, ops, options.BulkWrite().SetOrdered(false))
	return err
}
