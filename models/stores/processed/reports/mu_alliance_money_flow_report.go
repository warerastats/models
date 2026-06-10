package reports

import (
	"context"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MuAllianceMoneyFlowCounterpart is one per-counterpart-alliance breakdown row for an MU.
type MuAllianceMoneyFlowCounterpart struct {
	AllianceID   bson.ObjectID `bson:"allianceId"`
	InEquipment  float64       `bson:"inEquipment"`
	OutEquipment float64       `bson:"outEquipment"`
	InItems      float64       `bson:"inItems"`
	OutItems     float64       `bson:"outItems"`
	InWages      float64       `bson:"inWages"`
	OutWages     float64       `bson:"outWages"`
}

// MuAllianceMoneyFlowReport is a per-MU daily money-flow roll-up grouped by alliance.
type MuAllianceMoneyFlowReport struct {
	ID       string        `bson:"_id"`
	MuID     bson.ObjectID `bson:"muId"`
	DayStart time.Time     `bson:"dayStart"`

	InEquipment  float64 `bson:"inEquipment"`
	OutEquipment float64 `bson:"outEquipment"`
	InItems      float64 `bson:"inItems"`
	OutItems     float64 `bson:"outItems"`
	InWages      float64 `bson:"inWages"`
	OutWages     float64 `bson:"outWages"`

	InEquipmentInsideMuAlliance  float64 `bson:"inEquipmentInsideMuAlliance"`
	OutEquipmentInsideMuAlliance float64 `bson:"outEquipmentInsideMuAlliance"`
	InItemsInsideMuAlliance      float64 `bson:"inItemsInsideMuAlliance"`
	OutItemsInsideMuAlliance     float64 `bson:"outItemsInsideMuAlliance"`
	InWagesInsideMuAlliance      float64 `bson:"inWagesInsideMuAlliance"`
	OutWagesInsideMuAlliance     float64 `bson:"outWagesInsideMuAlliance"`

	InEquipmentOutsideMuAlliance  float64 `bson:"inEquipmentOutsideMuAlliance"`
	OutEquipmentOutsideMuAlliance float64 `bson:"outEquipmentOutsideMuAlliance"`
	InItemsOutsideMuAlliance      float64 `bson:"inItemsOutsideMuAlliance"`
	OutItemsOutsideMuAlliance     float64 `bson:"outItemsOutsideMuAlliance"`
	InWagesOutsideMuAlliance      float64 `bson:"inWagesOutsideMuAlliance"`
	OutWagesOutsideMuAlliance     float64 `bson:"outWagesOutsideMuAlliance"`

	Counterparts []MuAllianceMoneyFlowCounterpart `bson:"counterparts"`
}

// MuAllianceMoneyFlowReportID is the deterministic per-mu-per-day key.
func MuAllianceMoneyFlowReportID(muID bson.ObjectID, dayStart time.Time) string {
	return muID.Hex() + "@" + dayStart.UTC().Format("2006-01-02")
}

// MuAllianceMoneyFlowReportStore manages the mu_alliance_money_flow_reports collection.
type MuAllianceMoneyFlowReportStore struct {
	coll *mongo.Collection
}

// NewMuAllianceMoneyFlowReportStore creates the store and ensures indexes.
func NewMuAllianceMoneyFlowReportStore(ctx context.Context, db *mongo.Database) *MuAllianceMoneyFlowReportStore {
	store := &MuAllianceMoneyFlowReportStore{coll: db.Collection("mu_alliance_money_flow_reports")}
	store.ensureIndex(ctx)
	return store
}

func (s *MuAllianceMoneyFlowReportStore) ensureIndex(ctx context.Context) {
	_, err := s.coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "muId", Value: 1}, {Key: "dayStart", Value: -1}},
	})
	if err != nil {
		slog.Error("Failed creating compound index on mu_alliance_money_flow_reports.{muId,dayStart}", "error", err)
	}
}

// Upsert replaces a batch of per-MU daily alliance money-flow reports keyed on _id.
func (s *MuAllianceMoneyFlowReportStore) Upsert(ctx context.Context, rows []MuAllianceMoneyFlowReport) error {
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
